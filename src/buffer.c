/*-------------------------------------------------------------------------
 * buffer.c
 *
 * This file contains the functions related to the ring buffer allocated on
 * the dynamic shared memory of the bgworker.
 *
 * Whenever the query processing is completed, the leader process writes
 * the query strings and executed plan into a slot of the ring buffer by
 * using the store_plan() function.
 * (Parallel workers do not store the executed plans.)
 *
 * The ring buffer is periodically read by bgworker using the sweepBuffer()
 * function, the stored query strings and executed plan in the ring buffer
 * are inserted into the query_plan.log table.
 * Also, the ring buffer will be also read when it is full.
 *
 * Note:
 * Unlike the query data stored in shared memory, nested queries stored in the
 * ring buffer are not packed.
 * For example, a user executes a query nested in two levels, this query uses
 * two slots of the ring buffer. This is because each query in a nested query
 * finishes at a different time.
 *
 * Copyright (c) 2021-2023, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/explain.h"
#include "executor/spi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

#include "common.h"
#include "bgworker.h"
#include "buffer.h"
#include "qpam.h"

#define MAX_REFCOUNT 10			/* The threshold of refcount */

/*
 * Global variable declarations
 */
extern pgqpSharedState * pgqp;

/* Links to QueryDesc and ExplainState */
extern QueryDesc *qp_qd[MAX_NESTED_LEVEL];
extern ExplainState *qp_es[MAX_NESTED_LEVEL];

/*
 * Function declarations
 */
bool		is_alive(pid_t pid);
void		store_plan(const queryInfo qi, const int nested_level,
					   const uint64 queryId);
void		create_ring_buffer(void);
void		sweepBuffer(const bool recoveryInProgress);

static int	getNextWriteBuffer(void);
static void clear_bufferSlot(bufferSlot * bslot, const int i);
static void insertLog(bufferSlot * bslot, const bool recoveryInProgress);

/*
 * Check whether the process, whose id is pid, is alive or not.
 */
bool
is_alive(pid_t pid)
{
	if (kill(pid, 0) == 0)
		return true;
	if (EPERM == errno)
		return true;
	return false;
}

/*
 * Create a ring buffer on the dynamic shared memory of bgworker.
 */
void
create_ring_buffer(void)
{
	int			i;
	bufferSlot *bslot;

	/* Create a dsm segment for ring buffer */
	pgqp->dm.seg = dsm_create(sizeof(struct bufferSlot) * BUFFER_SIZE, 0);
	pgqp->dm.dh = dsm_segment_handle(pgqp->dm.seg);

	/* Initialize the write pointer: pgqp->nextWriteBuffer */
	SpinLockAcquire(&pgqp->nwblock);
	pgqp->nextWriteBuffer = 0;
	SpinLockRelease(&pgqp->nwblock);

	/* Initialize all buffer slots of ring buffer */
	bslot = (bufferSlot *) dsm_segment_address(pgqp->dm.seg);
	for (i = 0; i < BUFFER_SIZE; i++)
		clear_bufferSlot(bslot, i);
}


/*
 * Get nextWriteBuffer that points to a vacant buffer slot.
 */
static int
getNextWriteBuffer(void)
{
	int			count = 0;
	int			nextWriteBuffer;

	do
	{
		/* Set the (local) nextWriteBuffer */
		SpinLockAcquire(&pgqp->nwblock);
		nextWriteBuffer = pgqp->nextWriteBuffer;
		pgqp->nextWriteBuffer += 1;
		pgqp->nextWriteBuffer %= BUFFER_SIZE;
		SpinLockRelease(&pgqp->nwblock);

		/*
		 * Check the buffer state pointed by nextWriteBuffer, and change the
		 * state if it is vacant.
		 */
		count++;
		SpinLockAcquire(&(pgqp->bd[nextWriteBuffer].bslock));

		if (pgqp->bd[nextWriteBuffer].bs == BUFFER_VACANT)
		{
			pgqp->bd[nextWriteBuffer].bs = BUFFER_WRITING;
			SpinLockRelease(&(pgqp->bd[nextWriteBuffer].bslock));
			break;
		}
		SpinLockRelease(&(pgqp->bd[nextWriteBuffer].bslock));

		if (count > BUFFER_SIZE)
		{
			/*
			 * Set the bgworker's latch to invoke sweepBuffer() since the ring
			 * buffer is full.
			 */
			SetLatch(pgqp->bgLatch);
			count = 0;			/* Reset count. */
		}
	} while (true);

	return nextWriteBuffer;
}


/*
 * Store the query strings and executed plans into the ring buffer.
 * This function is called by backend processes whenever
 * their query processing ends.
 */

void
store_plan(const queryInfo qi, const int nested_level, const uint64 queryId)
{
	int			nextWriteBuffer;
	dsm_segment *seg = NULL;
	bufferSlot *bslot;
	MemoryContext oldcxt;
	QueryDesc  *queryDesc;


	/* If the bgworker is down, wait for restarting. */
	while (!is_alive(pgqp->bgworkerPid))
		pg_usleep((BGWORKER_RESTART_TIME + 1) * 1000);

	/* Get the write pointer. */
	nextWriteBuffer = getNextWriteBuffer();

	/* Attach the dsm segment. */
	seg = dsm_attach(pgqp->dm.dh);
	bslot = (bufferSlot *) dsm_segment_address(seg);

	/*
	 * Set the query and executed plans into the buffer slot whose index is
	 * nextWriteBuffer.
	 */
	init_qpd(&(bslot[nextWriteBuffer].qpd));

	/*
	 * Write the query string to pgqp->queryPlan_query[]
	 */
	set_query(&(bslot[nextWriteBuffer].qpd), nested_level, queryId, false);

	/*
	 * Get two kinds of executed plans info and set them to
	 * pgqp->queryPlan_plan[] and pgqp->queryPlan_json[]
	 */
	queryDesc = qp_qd[nested_level];
	oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
	qp_es[nested_level] = NewExplainState();

	set_plan(&(bslot[nextWriteBuffer].qpd), nested_level, false, PRINT_PLAN);
	set_plan(&(bslot[nextWriteBuffer].qpd), nested_level, false, PRINT_PLAN_JSON);

	MemoryContextSwitchTo(oldcxt);

	bslot[nextWriteBuffer].qpd.pid = MyProcPid;
	bslot[nextWriteBuffer].qpd.encoding = GetDatabaseEncoding();

	bslot[nextWriteBuffer].nested_level = nested_level;

	/*
	 * Set the query info
	 */
	memcpy((void *) bslot[nextWriteBuffer].qi.database_name,
		   qi.database_name, strlen(qi.database_name));
	memcpy((void *) bslot[nextWriteBuffer].qi.user_name,
		   qi.user_name, strlen(qi.user_name));
	bslot[nextWriteBuffer].qi.starttime = qi.starttime;
	bslot[nextWriteBuffer].qi.endtime = qi.endtime;

	/*
	 * Change the bufferState to BUFFER_OCCUPIED.
	 */
	SpinLockAcquire(&(pgqp->bd[nextWriteBuffer].bslock));
	pgqp->bd[nextWriteBuffer].bs = BUFFER_OCCUPIED;
	SpinLockRelease(&(pgqp->bd[nextWriteBuffer].bslock));

	/* Detach the dsm segment. */
	dsm_detach(seg);
}


/*
 * Clear the specified buffer slot whose index is idx.
 */
static void
clear_bufferSlot(bufferSlot * bslot, const int idx)
{
	bslot[idx].nested_level = 0;

	init_qpd(&bslot[idx].qpd);
	init_qi(&bslot[idx].qi);

	SpinLockAcquire(&(pgqp->bd[idx].bslock));
	pgqp->bd[idx].bs = BUFFER_VACANT;
	pgqp->bd[idx].refcount = 0;
	SpinLockRelease(&(pgqp->bd[idx].bslock));
}

/*
 * Sweep the ring buffer and insert data into the query_plan.log table
 * if data found. This function is periodically invoked by the bgworker.
 */
void
sweepBuffer(const bool recoveryInProgress)
{
	int			i,
				j;
	BufferState bs;
	int			refcount;
	bufferSlot *bslot;
	bool		is_active = false;

	bslot = (bufferSlot *) dsm_segment_address(pgqp->dm.seg);

	/*
	 * Sweep the ring buffer twice.
	 */
	for (j = 0; j < 2; j++)
	{
		for (i = 0; i < BUFFER_SIZE; i++)
		{
			SpinLockAcquire(&(pgqp->bd[i].bslock));
			bs = pgqp->bd[i].bs;
			refcount = pgqp->bd[i].refcount;
			SpinLockRelease(&(pgqp->bd[i].bslock));

			switch (bs)
			{
				case BUFFER_OCCUPIED:
					/* Start a transaction if there is at least one data */
					if (!is_active)
					{
						if (!recoveryInProgress)
						{
							start_tx();
							is_active = true;
						}
					}

					/* Insert the query info into query_plan.log  */
					insertLog(&bslot[i], recoveryInProgress);

					/* Clear the data of this buffer slot */
					clear_bufferSlot(bslot, i);
					break;

				case BUFFER_WRITING:
					if (refcount > MAX_REFCOUNT)
					{
						/*
						 * We consider that this slot could not be written
						 * because of the backend process failure. We
						 * therefore clean it up.
						 */
						clear_bufferSlot(bslot, i);
					}
					else
					{
						/* Increment 1 to the refcount of this buffer slot. */
						SpinLockAcquire(&(pgqp->bd[i].bslock));
						pgqp->bd[i].refcount += 1;
						SpinLockRelease(&(pgqp->bd[i].bslock));
					}
					break;
				default:
					break;
			};
		}
	}

	if (is_active)
		commit_tx();
}


/*
 * Insert data into query_plan.log table
 */
static void
insertLog(bufferSlot * bslot, const bool recoveryInProgress)
{
	StringInfoData buf;
	queryInfo	qi = bslot->qi;
	int			ret;
	bool		is_null;

	if (!recoveryInProgress)
	{
		initStringInfo(&buf);

		/*
		 * Split this INSERT statement into two parts so that each
		 * appendStringInfo() calls the timestamptz_to_str() function only
		 * once. The reason is that
		 * timestamptz_to_str()@src/backend/utils/adt/timestamp.c uses a
		 * static buffer, not pstrdup'd. See the function's comment.
		 */
		appendStringInfo(&buf, "INSERT INTO %s.%s"
						 " (starttime, endtime, database, pid,"
						 "  nested_level, queryid, query, planid, plan, plan_json)"
						 "  VALUES ("
						 "\'%s\',",
						 SCHEMA, LOG_TABLE,
						 timestamptz_to_str(qi.starttime)
			);
		appendStringInfo(&buf, "\'%s\', %s, %d, %d, \'%lu\', %s, \'%lu\', %s, %s);",
						 timestamptz_to_str(qi.endtime),
						 quote_literal_cstr(qi.database_name),
						 bslot->qpd.pid,
						 bslot->nested_level,
						 bslot->qpd.queryId[0],
						 quote_literal_cstr(get_query_plan(&bslot->qpd, PRINT_QUERY, &is_null, 0)),
						 bslot->qpd.planId[0],
						 quote_literal_cstr(get_query_plan(&bslot->qpd, PRINT_PLAN, &is_null, 0)),
						 quote_literal_cstr(get_query_plan(&bslot->qpd, PRINT_PLAN_JSON, &is_null, 0))
			);
		ret = SPI_execute(buf.data, false, 0);

		if (ret != SPI_OK_INSERT)
			ereport(ERROR, (errmsg("failed insertLog()")));
	}
}
