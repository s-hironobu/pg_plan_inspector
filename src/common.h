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
#include "storage/dsm.h"
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
								 * shared memory */
	RESULT_NO_QUERY,			/* query is not running */
	RESULT_DISABLE				/* showing query plan feature is disabled. */
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


/*
 * Global shared data
 */
typedef struct pgqpSharedState
{
	LWLock	   *lock;			/* Protects this entity */

	pid_t		callerPID;		/* The pid of the process that invokes the
								 * pg_query_plan function */
	pid_t		targetPID;		/* The pid of the process that is invoked by
								 * callerPID's process */
	Latch	   *latch;			/* A caller's latch. It is set by the target
								 * process to wake up the caller. */
	bool		finished;		/* A flag that is turned true when the getting
								 * query plan process is finished */
	slock_t		elock;			/* Protects the variable `finished` */

	/*
	 * Query and Plans
	 */
	struct queryPlanData qpd;
	struct queryInfo qi;
	qpResultState resultState;

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
