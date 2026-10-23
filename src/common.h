/*-------------------------------------------------------------------------
 * common.h
 *
 * Copyright (c) 2021-2025, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __COMMON_H__
#define __COMMON_H__

#include "postgres.h"

#include "datatype/timestamp.h"
#include "port/atomics.h"
#include "storage/dsm.h"
#include "storage/procnumber.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/s_lock.h"
#include "pg_config_manual.h"

/*-----------------------------------------------------
 * Define constants
 *-----------------------------------------------------*/
#define MAX_QUERY_LEN 16*1024	/* 16[KB] for query strings */
#define MAX_QUERY_PLAN_LEN 128*1024 /* 128[KB] for plan strings */
#define MAX_QUERY_PLAN_JSON_LEN 1024*1024	/* 1[MB] for plan strings
											 * formatted with JSON */
#define MAX_NESTED_LEVEL 16		/* Large enough */

#define BUFFER_SIZE 32			/* The size of the ring buffer allocated on
								 * bgworker's dynamic shared memory */


#define SCHEMA "query_plan"
#define LOG_TABLE "log"

/*-----------------------------------------------------
 * Define enumerate types
 *-----------------------------------------------------*/
/*
 * State of buffer slot
 */
typedef enum BufferState
{
	BUFFER_VACANT = 0,			/* buffer is vacant */
	BUFFER_WRITING,				/* client is writing to buffer */
	BUFFER_OCCUPIED				/* buffer is occupied */
}			BufferState;

/*
 * format
 */
typedef enum PrintFormat
{
	PRINT_QUERY = 0,			/* query */
	PRINT_PLAN,					/* [executed|query] plan formatted with text */
	PRINT_PLAN_JSON				/* [executed|query] plan formatted with json */
}			PrintFormat;

/*
 * State of the result by signal handler processing.
 */
typedef enum qpResultState
{
	RESULT_OK = 0,				/* query and query plan have been stored in
								 * the dsm segment */
	RESULT_NO_QUERY,			/* query is not running */
	RESULT_DISABLE,				/* showing query plan feature is disabled. */
	RESULT_TOO_SMALL,			/* the dsm segment is too small */
	RESULT_ERROR				/* an error occurred in the target */
}			qpResultState;

/*-----------------------------------------------------
 * Define data types
 *-----------------------------------------------------*/
/*
 * Hash Table
 */
typedef struct pgqpHashKey
{
	int			pid;
}			pgqpHashKey;

typedef struct pgqpEntry
{
	pgqpHashKey key;			/* hash key of entry - MUST BE FIRST */
	bool		is_explain;		/* Is the leader process executing EXPLAIN
								 * statement? */
	bool		_dummy;			/* This is a dummy element to make sure I
								 * don't forget how to extend this hash table. */
}			pgqpEntry;

/*
 * Buffer descriptor
 */
typedef struct bufferDesc
{
	BufferState bs;				/* State of buffer slot */
	int			refcount;		/* Counts how many times swept by the
								 * bgworker. */
	slock_t		bslock;			/* Protects this buffer slot */
}			bufferDesc;

/*
 * Query info
 */
typedef struct queryInfo
{
	char		database_name[NAMEDATALEN]; /* What database this query has
											 * been executed */
	char		user_name[NAMEDATALEN]; /* Who executes this query */
	TimestampTz starttime;		/* When this query starts */
	TimestampTz endtime;		/* When this query ends */
}			queryInfo;

/*
 * Query and Plans (text and json format)
 */
typedef struct queryPlanData
{
	int			encoding;

	/*----
	 * Data section:
	 *   queryPlan_query[] := query statement
	 *   queryPlan_text[] := [executed|query] plan info
	 *   queryPlan_json[] := [executed|query] plan info in JSON format
	 *
	 * The internal format of queryPlan_xxx[] is described in the comment of
	 * set_message()@qpam.c. It is also explained in the comment that how
	 * nested_level[], terminalByte[] and totalLen[] are used.
	 */
	char		queryPlan_query[MAX_QUERY_LEN]; /* query string */
	char		queryPlan_text[MAX_QUERY_PLAN_LEN]; /* [executed|query] plan
													 * string formatted with
													 * text */
	char		queryPlan_json[MAX_QUERY_PLAN_JSON_LEN];	/* [executed|query] plan
															 * string formatted wth
															 * json */

	/*
	 * Meta data section
	 */
#define qs_num 3				/* 0 = query; 1 = plan text; 2 = plan json */

	int			nested_level[qs_num];	/* The max nested_level of the stored
										 * info */
	int			terminalByte[qs_num][MAX_NESTED_LEVEL]; /* The positions of the
														 * message's terminators */
	int			totalLen[qs_num];	/* The total length of queryPlan[] */
#undef qs_num

	/*
	 * Identifires section
	 */
	uint64		queryId[MAX_NESTED_LEVEL];
	uint64		planId[MAX_NESTED_LEVEL];

	/*
	 * These are used to link the relationship between the leader and the
	 * parallel workers.
	 */
	bool		is_leader;		/* Is the process that runs this query a
								 * leader or a parallel worker? */
	pid_t		pid;			/* The pid of leader */
}			queryPlanData;


/*
 * dsm (dynamic shared memory) management unit to access to the ring buffer
 * on bgworker
 */
typedef struct dsmMgr
{
	dsm_segment *seg;
	dsm_handle	dh;
}			dsmMgr;


/*-----------------------------------------------------
 * pg_query_plan() request protocol
 *
 * A caller of pg_query_plan() and the target process exchange data through
 * a per-request dsm segment created by the caller.  The rendezvous point is
 * a small request slot per target process (indexed by ProcNumber) in the
 * shared memory.  See the comment of pg_query_plan()@pg_query_plan.c for
 * the whole protocol.
 *-----------------------------------------------------*/

/* Hard-coded protocol parameters */
#define PGQP_REQUEST_TIMEOUT_MS		5000	/* wait at most 5 [sec] for a
											 * response */
#define PGQP_REQUEST_MAX_RETRY		5	/* retry count when the slot is busy
										 * or the segment is too small */
#define PGQP_BUSY_RETRY_INTERVAL_MS 1000	/* wait between busy retries */
#define PGQP_WAIT_INTERVAL_MS		100 /* polling interval while waiting */
#define PGQP_INITIAL_SEGMENT_SIZE	(256 * 1024)	/* initial data area size */

#define PGQP_SEGMENT_MAGIC			0x50475150	/* "PGQP" */

/*
 * State of a request slot.
 *
 * The state word of a slot packs the following three fields so that every
 * state transition can be done with one compare-and-swap (CAS) operation,
 * which is also safe to be executed in a signal handler:
 *
 *   bits  0- 3: slot state (pgqpSlotState)
 *   bits  4- 7: result (qpResultState); meaningful only in PGQP_SLOT_DONE
 *   bits  8-31: generation number of the request
 *
 * The generation number prevents a stale response from being regarded as
 * the response to a newer request.
 */
typedef enum pgqpSlotState
{
	PGQP_SLOT_IDLE = 0,			/* no request */
	PGQP_SLOT_CLAIMED,			/* a caller is filling in the request */
	PGQP_SLOT_PENDING,			/* waiting for the target's response */
	PGQP_SLOT_DONE				/* the target has responded */
}			pgqpSlotState;

#define PGQP_SLOT_MAKE(gen, st, res) \
	((((uint32) (gen) & 0xFFFFFF) << 8) | (((uint32) (res) & 0xF) << 4) | ((uint32) (st) & 0xF))
#define PGQP_SLOT_GEN(v)	(((uint32) (v)) >> 8)
#define PGQP_SLOT_RESULT(v) ((((uint32) (v)) >> 4) & 0xF)
#define PGQP_SLOT_STATE(v)	(((uint32) (v)) & 0xF)
#define PGQP_NEXT_GEN(gen)	((((uint32) (gen)) + 1) & 0xFFFFFF)

/*
 * Request slot, one per target process.
 *
 * `caller`, `caller_pid` and `handle` are written by the caller while the
 * slot is in PGQP_SLOT_CLAIMED, and are read by the target only after it
 * observes PGQP_SLOT_PENDING.
 */
typedef struct pgqpRequestSlot
{
	pg_atomic_uint32 state;		/* see above */
	ProcNumber	caller;			/* ProcNumber of the caller, used to set
								 * the caller's latch */
	pid_t		caller_pid;		/* pid of the caller, used to detect a
								 * stale slot left by a dead caller */
	dsm_handle	handle;			/* the dsm segment created by the caller */
}			pgqpRequestSlot;

/*
 * Layout of the per-request dsm segment.
 *
 *   [pgqpSegHeader][data area: capacity bytes]
 *
 * The data area contains `nlevels` entries.  Each entry is a pgqpSegEntry
 * followed by three NUL-terminated strings (query, plan text, plan json),
 * and the next entry begins at the MAXALIGN'ed offset.  All references are
 * offsets because the segment is mapped at different addresses.
 */
typedef struct pgqpSegHeader
{
	uint32		magic;			/* PGQP_SEGMENT_MAGIC */
	uint32		generation;		/* generation of the request */
	Size		capacity;		/* size of the data area */
	Size		required;		/* set by the target if the data area is too
								 * small */
	Size		used;			/* bytes used in the data area */
	int			result;			/* qpResultState */
	int			encoding;		/* encoding of the strings */
	int			nlevels;		/* number of entries */
	char		database_name[NAMEDATALEN];
	char		user_name[NAMEDATALEN];
}			pgqpSegHeader;

typedef struct pgqpSegEntry
{
	uint64		queryId;
	uint64		planId;
	uint32		len[3];			/* lengths of query, plan text and plan json
								 * without the terminators */
}			pgqpSegEntry;

#define PGQP_SEG_DATA(hdr) \
	((char *) (hdr) + MAXALIGN(sizeof(pgqpSegHeader)))


/*
 * Global shared data
 */
typedef struct pgqpSharedState
{
	LWLock	   *lock;			/* Protects this entity */

	/*
	 * Request slots for pg_query_plan(). This points to the array of
	 * MaxBackends slots, which is allocated just after this structure.
	 */
	pgqpRequestSlot *slots;

	/*
	 * Background worker
	 */
	Latch	   *bgLatch;		/* A bgworker's latch. It is set by the client
								 * to invoke the sweepBuffer() function when
								 * the buffer is full. */
	pid_t		bgworkerPid;	/* The pid of the pg_query_plan bgworker */

	int			nextWriteBuffer;	/* A pointer, which is always pointing to
									 * one of the buffer slots of the ring
									 * buffer. Backend processes write the
									 * query and executed plans to the buffer
									 * slot that this pointer points to. */
	slock_t		nwblock;		/* Protects the variable `nextWriteBuffer` */

	struct dsmMgr dm;			/* This contains a dsm segment and a dsm
								 * handler to access the ring buffer on the
								 * bgworker. */
	struct bufferDesc bd[BUFFER_SIZE];	/* The buffer descriptors of the ring
										 * buffer. */

	/*
	 * The hash table on the shared memory
	 */
	LWLock	   *htlock;			/* Protects the hash table */

}			pgqpSharedState;

#endif							/* __COMMON_H__ */
