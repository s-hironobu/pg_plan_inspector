/*-------------------------------------------------------------------------
 * pg_query_plan.c
 *
 * Show the query plan of the running query. And, similar to auto_explain,
 * store the executed plan into the query_plan.log table in postgres database.
 *
 * Copyright (c) 2021-2025, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#define __ADJUST_ROWS__
#define __ADAPT_WORK_MEM__

#include "funcapi.h"
#include "libpq/pqsignal.h"
#include "libpq/auth.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/parallel.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "common/hashfn.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "nodes/queryjumble.h"
#include "nodes/nodeFuncs.h"
#include "utils/memutils.h"

#ifdef __ADJUST_ROWS__
#include "nodes/pathnodes.h"
#include "optimizer/geqo.h"
#include "optimizer/planner.h"
#include "nodes/print.h"
#include "nodes/bitmapset.h"
#include "optimizer/pgqp_allpaths.h"
#include "adjust_rows.h"
#include "optimizer/pgqp_planner.h"
#include "optimizer/pgqp_planmain.h"
#endif

#include "optimizer/paths.h"
#include "pg_query_plan.h"
#include "common.h"
#include "buffer.h"
#include "bgworker.h"
#include "pgqp_explain.h"
#include "hash.h"
#include "qpam.h"
#include "planid.h"
#include "param.h"

#ifdef __ADAPT_WORK_MEM__
#include "commands/explain.h"
#include "access/skey.h"
#include "access/relscan.h"
#include "access/genam.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "pgqp_sortnode.h"
#endif

PG_MODULE_MAGIC;

/* Link to shared memory state */
pgqpSharedState *pgqp = NULL;
HTAB	   *pgqp_hash = NULL;

/*
 * Static variables
 */

/* Current nesting depth of ExecutorRun call */
static int	nested_level = 0;

/* See _PG_init() */
static bool pgqp_global_show_plan;
static bool pgqp_show_plan;
static bool pgqp_global_store_plan;
static bool pgqp_store_plan;
static int	pgqp_log_min_duration;
#ifdef __ADJUST_ROWS__
bool		pgqp_enable_adjust_joinrel_rows;
bool		pgqp_enable_adjust_rel_rows;
#endif
#ifdef __ADDITIONAL_OPTIONS__
bool pgqp_log_buffers;
bool pgqp_log_wal;
#endif
#ifdef __ADAPT_WORK_MEM__
bool pgqp_work_mem;
int	tmp_work_mem;
#endif

bool		pgqp_received_signal;

/*
 * Set by the signal handler when a pg_query_plan() request arrives, and
 * checked at the safe point, i.e. at the entry of every plan node.
 */
static volatile sig_atomic_t pgqp_request_pending = false;
static bool pgqp_serving_request = false;	/* guard against reentrance */
static dsm_segment *pgqp_serving_seg = NULL;	/* attached while serving */

/*
 * Map from a plan node to its original ExecProcNodeReal function.
 *
 * pgqp_ExecutorStart() replaces ExecProcNodeReal of every plan node with
 * pgqp_ExecProcNode(), which checks pgqp_request_pending and then calls the
 * original function.  The original functions are saved per EState, indexed
 * by plan_node_id.  The map is allocated in es_query_cxt and unlinked by
 * the reset callback of the context.
 */
typedef struct pgqpNodeMap
{
	EState	   *estate;
	int			nnodes;
	ExecProcNodeMtd *orig;		/* indexed by plan_node_id */
	MemoryContextCallback cb;
	struct pgqpNodeMap *next;
}			pgqpNodeMap;

static pgqpNodeMap *pgqp_node_maps = NULL;

/*
 * The request slot held by this process as a caller of pg_query_plan().
 * It is released at the end of the request, on error, and at process exit.
 */
static int	pgqp_held_slot = -1;
static uint32 pgqp_held_gen = 0;
static bool pgqp_exit_callback_registered = false;

/* Special return values of pgqp_request_plans() */
#define PGQP_REQ_BUSY		(-1)	/* the slot was busy even after retries */
#define PGQP_REQ_TIMEOUT	(-2)	/* the target did not respond in time */
#define PGQP_REQ_GONE		(-3)	/* the target has exited */

/* Query info */
static queryInfo qi;

/* Links to QueryDesc and ExplainState */
QueryDesc  *qp_qd[MAX_NESTED_LEVEL];

/*
 * QueryDesc actually being executed at each nesting level.
 *
 * qp_qd[] is set in ExecutorStart, so it does not represent the running
 * query when several portals are executed alternately (e.g. FETCH from
 * cursors).  qp_run_qd[] is set when ExecutorRun/ExecutorFinish is entered,
 * so qp_run_qd[0 .. nested_level-1] are always the running queries.
 * This is used by pg_query_plan().
 */
static QueryDesc *qp_run_qd[MAX_NESTED_LEVEL];
ExplainState *qp_es[MAX_NESTED_LEVEL];

static bool enable_show_plan;	/* Whether showing query plan is enabled */
static bool enable_store_plan;	/* Whether storing query plan is enabled */
static bool is_explain;			/* Whether this query is EXPLAIN or not */
static bool setSignalHandler = false;	/* Whether the signal handler has been
										 * set */

static bool is_leader;			/* This process is a leader, not a parallel
								 * worker */
static TimestampTz starttime[MAX_NESTED_LEVEL]; /* Start time of query in each
												 * nested_level */
static uint64 queryId[MAX_NESTED_LEVEL];	/* QueryId of query in each
											 * nested_level */

static int	pgqp_hash_max;

/* Saved hook values in case of unload */
shmem_request_hook_type prev_shmem_request_hook = NULL;
shmem_startup_hook_type prev_shmem_startup_hook = NULL;
ExecutorStart_hook_type prev_ExecutorStart = NULL;
ExecutorRun_hook_type prev_ExecutorRun = NULL;
ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
ProcessUtility_hook_type prev_ProcessUtility = NULL;
ClientAuthentication_hook_type prev_ClientAuthentication = NULL;
#ifdef __ADJUST_ROWS__
planner_hook_type prev_planner = NULL;
join_search_hook_type prev_join_search = NULL;
set_rel_pathlist_hook_type prev_set_rel_pathlist = NULL;
set_join_pathlist_hook_type prev_set_join_pathlist = NULL;
#endif


/*
 * Function declarations
 */
void		_PG_init(void);
void		_PG_fini(void);

static void pgqp_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgqp_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
							 uint64 count);
static void pgqp_ExecutorFinish(QueryDesc *queryDesc);
static void pgqp_ExecutorEnd(QueryDesc *queryDesc);
static void pgqp_ClientAuthentication(Port *port, int status);
static void pgqp_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
								bool readOnlyTree,
								ProcessUtilityContext context,
								ParamListInfo params, QueryEnvironment *queryEnv,
								DestReceiver *dest, QueryCompletion *qc);
#ifdef __ADJUST_ROWS__
static PlannedStmt *pgqp_planner(Query *parse, const char *query_string, int cursorOptions,
								 ParamListInfo boundParams);

static RelOptInfo *pgqp_join_search(PlannerInfo *root, int levels_needed,
									List *initial_rels);
#endif

static void pgqp_shmem_request(void);
static void pgqp_shmem_startup(void);
static void pgqp_shmem_shutdown(int code, Datum arg);
static Size pgqp_memsize(void);
static bool is_parallel_worker(const int pid, PgBackendStatus *beentry);
static int	get_leader_pid(void);
static void pgqp_install_node_wrappers(QueryDesc *queryDesc);
static TupleTableSlot *pgqp_ExecProcNode(PlanState *node);
static void pgqp_serve_request(void);
static bool pgqp_respond(pgqpRequestSlot *slot, uint32 expected,
						 ProcNumber caller, int result);
static int	pgqp_request_plans(ProcNumber procno, pid_t pid,
							   dsm_segment **segp);
static void pgqp_release_held_slot(void);
static bool checkConditionOfLogInsertion(const int pid,
										 const TimestampTz currentTimestamp);
static void sig_get_query_plan(SIGNAL_ARGS);
#ifdef __ADAPT_WORK_MEM__
static int	get_sort_space_used(const uint64 queryId);
static void set_work_mem(const int wm);
#endif

Datum		pg_query_plan(PG_FUNCTION_ARGS);
Datum		get_planid(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_query_plan);
PG_FUNCTION_INFO_V1(get_planid);

#include <unistd.h>
/*
 * Module callback
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomBoolVariable("pg_query_plan.global_show_plan",
							 "In the entire server, show the executing query and query plan.",
							 NULL,
							 &pgqp_global_show_plan,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_query_plan.show_plan",
							 "Show the executing query and query plan.",
							 "show_plan is force set to false if global_show_plan is false.",
							 &pgqp_show_plan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_query_plan.global_store_plan",
							 "In the entire server, store the executed query and executed plan into the query_plan.log table.",
							 "global_store_plan is disabled, i.e. practically false, if either global_show_plan or show_plan is false.",
							 &pgqp_global_store_plan,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_query_plan.store_plan",
							 "Store the executed query and executed plan into the query_plan.log table.",
							 "store_plan is forced set to false if either global_store_plan, global_show_plan or show_plan is false.",
							 &pgqp_store_plan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_query_plan.log_min_duration",
							"Set the minimum execution time in seconds above which executed plans will be logged.",
							"Zero stores all plans.",
							&pgqp_log_min_duration,
							10,
							0,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

#ifdef __ADJUST_ROWS__
	DefineCustomBoolVariable("pg_query_plan.enable_adjust_joinrel_rows",
							 "Whether adjust the join rows.",
							 NULL,
							 &pgqp_enable_adjust_joinrel_rows,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_query_plan.enable_adjust_rel_rows",
							 "Whether adjust the index scan rows.",
							 NULL,
							 &pgqp_enable_adjust_rel_rows,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
#endif

#ifdef __ADDITIONAL_OPTIONS__
	DefineCustomBoolVariable("pg_query_plan.log_buffers",
							 "Show and store buffers usage.",
							 NULL,
							 &pgqp_log_buffers,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_query_plan.log_wal",
							 "Show and store WAL usage.",
							 NULL,
							 &pgqp_log_wal,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);
#endif

	EmitWarningsOnPlaceholders("pg_query_plan");

	/* Install hooks. */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pgqp_shmem_request;

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgqp_shmem_startup;

	if (!IsParallelWorker())
	{
		prev_ClientAuthentication = ClientAuthentication_hook;
		ClientAuthentication_hook = pgqp_ClientAuthentication;

		prev_ProcessUtility = ProcessUtility_hook;
		ProcessUtility_hook = pgqp_ProcessUtility;
	}

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgqp_ExecutorStart;

	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pgqp_ExecutorRun;

	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pgqp_ExecutorFinish;

	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgqp_ExecutorEnd;

#ifdef __ADJUST_ROWS__
	prev_planner = planner_hook;
	planner_hook = pgqp_planner;

	prev_join_search = join_search_hook;
	join_search_hook = pgqp_join_search;

	prev_set_rel_pathlist = set_rel_pathlist_hook;
	set_rel_pathlist_hook = pgqp_set_rel_pathlist;

	prev_set_join_pathlist = set_join_pathlist_hook;
	set_join_pathlist_hook = pgqp_set_join_pathlist;
#endif							/* __ADJUST_ROWS__ */

	/* initialize variable */
	is_explain = false;
	pgqp_received_signal = false;
#ifdef __ADJUST_ROWS__
	pgqp_adjust_rows = false;

	init_param_parse_env();
#endif

	/* Enables query identifier computation. */
	EnableQueryId();

	/* Initialize bgworker. */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;

#if defined(__TEST__) || defined(__BGW_TEST__)
	worker.bgw_restart_time = BGW_NEVER_RESTART;
#else
	worker.bgw_restart_time = BGWORKER_RESTART_TIME;
#endif
	sprintf(worker.bgw_library_name, "pg_query_plan");
	sprintf(worker.bgw_function_name, "pg_query_plan_main");
	worker.bgw_notify_pid = 0;
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_query_plan worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_query_plan");
	worker.bgw_main_arg = Int32GetDatum(2);
	RegisterBackgroundWorker(&worker);

}

static void
pgqp_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(pgqp_memsize());
	RequestNamedLWLockTranche("pg_query_plan", 1);
}

void
_PG_fini(void)
{
	/* Uninstall hooks. */
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;

#ifdef __ADJUST_ROWS__
	planner_hook = prev_planner;
	join_search_hook = prev_join_search;
	set_rel_pathlist_hook = prev_set_rel_pathlist;
	set_join_pathlist_hook = prev_set_join_pathlist;
#endif							/* __ADJUST_ROWS__ */

	if (!IsParallelWorker())
	{
		ProcessUtility_hook = prev_ProcessUtility;
		ClientAuthentication_hook = prev_ClientAuthentication;
	}
}


/*
 * shmem_startup hook: allocate or attach to shared memory.
 */
static void
pgqp_shmem_startup(void)
{
	bool		found;
	int			i;
	HASHCTL		info;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	pgqp = NULL;
	pgqp_hash_max = MaxConnections;

	/* Create or attach to the shared memory state, including hash table */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	pgqp = ShmemInitStruct("pg_query_plan",
						   add_size(MAXALIGN(sizeof(pgqpSharedState)),
									mul_size(MaxBackends, sizeof(pgqpRequestSlot))),
						   &found);

	if (!found)
	{
		/* First time through ... */
		pgqp->lock = &(GetNamedLWLockTranche("pg_query_plan"))->lock;
		pgqp->htlock = &(GetNamedLWLockTranche("pg_query_plan"))->lock;

		SpinLockInit(&pgqp->nwblock);
		for (i = 0; i < BUFFER_SIZE; i++)
			SpinLockInit(&pgqp->bd[i].bslock);

		/* Initialize the request slots of pg_query_plan(). */
		pgqp->slots = (pgqpRequestSlot *)
			((char *) pgqp + MAXALIGN(sizeof(pgqpSharedState)));
		for (i = 0; i < MaxBackends; i++)
		{
			pg_atomic_init_u32(&pgqp->slots[i].state,
							   PGQP_SLOT_MAKE(0, PGQP_SLOT_IDLE, RESULT_OK));
			pgqp->slots[i].caller = INVALID_PROC_NUMBER;
			pgqp->slots[i].caller_pid = InvalidPid;
			pgqp->slots[i].handle = DSM_HANDLE_INVALID;
		}
	}

	/* Initialize hash table */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(pgqpHashKey);
	info.entrysize = sizeof(pgqpEntry);
	pgqp_hash = ShmemInitHash("pg_query_plan hash",
							  pgqp_hash_max, pgqp_hash_max,
							  &info,
							  HASH_ELEM | HASH_BLOBS);

	/*
	 * pgqp->wptr and pgqp->bd[] are initialized in
	 * create_ring_buffer()@bgworker.c
	 */
	pgqp->bgworkerPid = InvalidPid;

	LWLockRelease(AddinShmemInitLock);

	if (!IsUnderPostmaster)
		on_shmem_exit(pgqp_shmem_shutdown, (Datum) 0);
}


static void
pgqp_shmem_shutdown(int code, Datum arg)
{
	/* Do nothing */
	return;
}


/*
 * Estimate shared memory space needed.
 */
static Size
pgqp_memsize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(pgqpSharedState));
	size = add_size(size, mul_size(MaxBackends, sizeof(pgqpRequestSlot)));
	size = add_size(size, hash_estimate_size(pgqp_hash_max, sizeof(pgqpEntry)));

	return size;
}

/*
 * Set the result to the request slot and wake the caller up.
 *
 * `expected` is the state word observed in PGQP_SLOT_PENDING.  If the state
 * has been changed since then (e.g. the caller gave up the request), this
 * does nothing and returns false.
 *
 * This function is async-signal-safe: it uses only an atomic CAS and
 * SetLatch().
 */
static bool
pgqp_respond(pgqpRequestSlot *slot, uint32 expected, ProcNumber caller,
			 int result)
{
	uint32		newval = PGQP_SLOT_MAKE(PGQP_SLOT_GEN(expected),
										PGQP_SLOT_DONE, result);

	Assert(PGQP_SLOT_STATE(expected) == PGQP_SLOT_PENDING);

	/* The CAS is a full memory barrier; the writes to the segment precede. */
	if (!pg_atomic_compare_exchange_u32(&slot->state, &expected, newval))
		return false;

	if (caller != INVALID_PROC_NUMBER && caller < MaxBackends)
		SetLatch(&GetPGProcByNumber(caller)->procLatch);

	return true;
}


/*
 * Return the request slot of this process, or NULL if not available.
 */
static inline pgqpRequestSlot *
my_request_slot(void)
{
	if (pgqp == NULL || MyProcNumber == INVALID_PROC_NUMBER
		|| MyProcNumber >= MaxBackends)
		return NULL;
	return &pgqp->slots[MyProcNumber];
}


/*
 * Signal handler function
 *
 * When a signal (SIGUSR2) is received, this handler does only async-signal
 * safe operations:
 *
 *  - If the showing query plan feature is disabled or no query is running,
 *    it responds to the caller immediately via the request slot.
 *  - Otherwise, it sets pgqp_request_pending and returns.  The request is
 *    served at the next safe point, i.e. when any plan node is invoked
 *    (see pgqp_ExecProcNode()).
 *
 * Note: The former implementation generated the query plans inside this
 *       handler. That is unsafe because the handler can interrupt palloc(),
 *       a critical section, a dsm operation and so on.
 */
static void
sig_get_query_plan(SIGNAL_ARGS)
{
	int			save_errno = errno;
	pgqpRequestSlot *slot = my_request_slot();

	if (slot != NULL)
	{
		uint32		cur = pg_atomic_read_u32(&slot->state);

		if (PGQP_SLOT_STATE(cur) == PGQP_SLOT_PENDING)
		{
			if (!(enable_show_plan && pgqp_global_show_plan && pgqp_show_plan))
			{
				pg_read_barrier();
				pgqp_respond(slot, cur, slot->caller, RESULT_DISABLE);
			}
			else if (nested_level <= 0)
			{
				pg_read_barrier();
				pgqp_respond(slot, cur, slot->caller, RESULT_NO_QUERY);
			}
			else
				pgqp_request_pending = true;
		}
	}

	/* Wake up this process if it is waiting on its latch, e.g. in Gather. */
	SetLatch(MyLatch);

	errno = save_errno;
}


/*
 * Build the query strings and the query plans of all nested levels, and
 * write them into the dsm segment specified by handle.
 *
 * This is called from pgqp_serve_request() inside PG_TRY().
 */
static int
pgqp_write_plans(dsm_handle handle, uint32 gen)
{
	int			nlevels = Min(nested_level, MAX_NESTED_LEVEL);
	int			i;
	const char *query[MAX_NESTED_LEVEL];
	char	   *text[MAX_NESTED_LEVEL];
	char	   *json[MAX_NESTED_LEVEL];
	uint32		len[MAX_NESTED_LEVEL][3];
	uint64		planid[MAX_NESTED_LEVEL];
	Size		required = 0;
	pgqpSegHeader *hdr;
	int			result = RESULT_OK;

	/*
	 * Check the running queryDesc and its planstate of each nested level.
	 */
	if (nlevels <= 0)
		return RESULT_NO_QUERY;
	for (i = 0; i < nlevels; i++)
		if (qp_run_qd[i] == NULL || qp_run_qd[i]->planstate == NULL)
			return RESULT_NO_QUERY;

	/*
	 * make_plan_string() explains qp_qd[], so point it to the running
	 * queries.  pgqp_serve_request() restores qp_qd[] afterwards.
	 */
	for (i = 0; i < nlevels; i++)
		qp_qd[i] = qp_run_qd[i];

	/*
	 * Build the strings in the current memory context (a temporary context
	 * created by the caller of this function).
	 */
	pgqp_received_signal = true;
	for (i = 0; i < nlevels; i++)
	{
		query[i] = qp_qd[i]->sourceText ? qp_qd[i]->sourceText : "";
		len[i][PRINT_QUERY] = strlen(query[i]);

		qp_es[i] = NewExplainState();

		make_plan_string(i, PRINT_PLAN, NULL);
		text[i] = pnstrdup(qp_es[i]->str->data, qp_es[i]->str->len);
		len[i][PRINT_PLAN] = qp_es[i]->str->len;

		planid[i] = 0;
		make_plan_string(i, PRINT_PLAN_JSON, &planid[i]);
		json[i] = pnstrdup(qp_es[i]->str->data, qp_es[i]->str->len);
		len[i][PRINT_PLAN_JSON] = qp_es[i]->str->len;

		required += MAXALIGN(sizeof(pgqpSegEntry)
							 + len[i][PRINT_QUERY] + 1
							 + len[i][PRINT_PLAN] + 1
							 + len[i][PRINT_PLAN_JSON] + 1);
	}
	pgqp_received_signal = false;

	/*
	 * Attach the caller's segment. If it does not exist anymore, the caller
	 * has already given up this request.
	 */
	pgqp_serving_seg = dsm_attach(handle);
	if (pgqp_serving_seg == NULL)
		return RESULT_ERROR;

	hdr = (pgqpSegHeader *) dsm_segment_address(pgqp_serving_seg);
	if (hdr->magic != PGQP_SEGMENT_MAGIC || hdr->generation != gen)
	{
		dsm_detach(pgqp_serving_seg);
		pgqp_serving_seg = NULL;
		return RESULT_ERROR;
	}

	hdr->encoding = GetDatabaseEncoding();
	strlcpy(hdr->database_name, qi.database_name, NAMEDATALEN);
	strlcpy(hdr->user_name, qi.user_name, NAMEDATALEN);

	if (required > hdr->capacity)
	{
		/* Tell the caller the required size; it will retry. */
		hdr->required = required;
		hdr->nlevels = 0;
		hdr->used = 0;
		result = RESULT_TOO_SMALL;
	}
	else
	{
		char	   *data = PGQP_SEG_DATA(hdr);
		Size		off = 0;

		for (i = 0; i < nlevels; i++)
		{
			pgqpSegEntry *e = (pgqpSegEntry *) (data + off);
			char	   *p = (char *) e + sizeof(pgqpSegEntry);

			e->queryId = qp_run_qd[i]->plannedstmt->queryId;
			e->planId = planid[i];
			e->len[PRINT_QUERY] = len[i][PRINT_QUERY];
			e->len[PRINT_PLAN] = len[i][PRINT_PLAN];
			e->len[PRINT_PLAN_JSON] = len[i][PRINT_PLAN_JSON];

			memcpy(p, query[i], len[i][PRINT_QUERY]);
			p += len[i][PRINT_QUERY];
			*p++ = '\0';
			memcpy(p, text[i], len[i][PRINT_PLAN]);
			p += len[i][PRINT_PLAN];
			*p++ = '\0';
			memcpy(p, json[i], len[i][PRINT_PLAN_JSON]);
			p += len[i][PRINT_PLAN_JSON];
			*p++ = '\0';

			off += MAXALIGN(sizeof(pgqpSegEntry)
							+ len[i][PRINT_QUERY] + 1
							+ len[i][PRINT_PLAN] + 1
							+ len[i][PRINT_PLAN_JSON] + 1);
		}
		hdr->nlevels = nlevels;
		hdr->used = off;
	}
	hdr->result = result;

	dsm_detach(pgqp_serving_seg);
	pgqp_serving_seg = NULL;

	return result;
}


/*
 * Serve the pending pg_query_plan() request.
 *
 * This is called at a safe point, i.e. at the entry of a plan node, so any
 * ordinary backend operation (palloc, catalog access, dsm_attach, ...) can
 * be used here.
 */
static void
pgqp_serve_request(void)
{
	pgqpRequestSlot *slot;
	uint32		cur;
	ProcNumber	caller;
	dsm_handle	handle;
	int			result = RESULT_ERROR;
	MemoryContext tmpcxt;
	MemoryContext oldcxt;
	ExplainState *saved_es[MAX_NESTED_LEVEL];
	QueryDesc  *saved_qd[MAX_NESTED_LEVEL];

	pgqp_request_pending = false;

	if (pgqp_serving_request || (slot = my_request_slot()) == NULL)
		return;

	cur = pg_atomic_read_u32(&slot->state);
	if (PGQP_SLOT_STATE(cur) != PGQP_SLOT_PENDING)
		return;
	pg_read_barrier();
	caller = slot->caller;
	handle = slot->handle;

	/* The showing query plan feature may have been disabled. */
	if (!(enable_show_plan && pgqp_global_show_plan && pgqp_show_plan))
	{
		pgqp_respond(slot, cur, caller, RESULT_DISABLE);
		return;
	}

	pgqp_serving_request = true;
	memcpy(saved_es, qp_es, sizeof(saved_es));
	memcpy(saved_qd, qp_qd, sizeof(saved_qd));

	tmpcxt = AllocSetContextCreate(TopMemoryContext,
								   "pg_query_plan request",
								   ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(tmpcxt);

	PG_TRY();
	{
		result = pgqp_write_plans(handle, PGQP_SLOT_GEN(cur));
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcxt);
		pgqp_received_signal = false;
		if (pgqp_serving_seg != NULL)
		{
			dsm_detach(pgqp_serving_seg);
			pgqp_serving_seg = NULL;
		}
		memcpy(qp_es, saved_es, sizeof(saved_es));
		memcpy(qp_qd, saved_qd, sizeof(saved_qd));
		pgqp_serving_request = false;

		/* Tell the caller that this request failed. */
		pgqp_respond(slot, cur, caller, RESULT_ERROR);

		/*
		 * A query cancel (including statement_timeout) must not be swallowed,
		 * so re-throw it.  Other errors are only reported as a warning in
		 * order not to abort the running query because of a monitoring
		 * request.
		 */
		edata = CopyErrorData();
		if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED)
		{
			FreeErrorData(edata);
			MemoryContextDelete(tmpcxt);
			PG_RE_THROW();
		}
		FlushErrorState();
		MemoryContextDelete(tmpcxt);
		ereport(WARNING,
				(errmsg("pg_query_plan: could not build the query plan: %s",
						edata->message)));
		FreeErrorData(edata);
		return;
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(tmpcxt);
	memcpy(qp_es, saved_es, sizeof(saved_es));
	memcpy(qp_qd, saved_qd, sizeof(saved_qd));
	pgqp_serving_request = false;

	pgqp_respond(slot, cur, caller, result);
}


/*
 * ExecProcNodeReal wrapper: the safe point to serve pg_query_plan() requests.
 */
static TupleTableSlot *
pgqp_ExecProcNode(PlanState *node)
{
	pgqpNodeMap *map;

	if (unlikely(pgqp_request_pending) && CritSectionCount == 0)
		pgqp_serve_request();

	for (map = pgqp_node_maps; map != NULL; map = map->next)
	{
		if (likely(map->estate == node->state))
			return map->orig[node->plan->plan_node_id] (node);
	}

	elog(ERROR, "pg_query_plan: could not find the original ExecProcNode function");
	return NULL;				/* keep compiler quiet */
}


/*
 * Reset callback of es_query_cxt: unlink the map.
 */
static void
pgqp_node_map_reset(void *arg)
{
	pgqpNodeMap *map = (pgqpNodeMap *) arg;
	pgqpNodeMap **p;

	for (p = &pgqp_node_maps; *p != NULL; p = &(*p)->next)
	{
		if (*p == map)
		{
			*p = map->next;
			break;
		}
	}
}


static bool
pgqp_max_node_id_walker(PlanState *ps, int *maxid)
{
	if (ps->plan != NULL && ps->plan->plan_node_id > *maxid)
		*maxid = ps->plan->plan_node_id;
	return planstate_tree_walker(ps, pgqp_max_node_id_walker, maxid);
}


static bool
pgqp_wrap_walker(PlanState *ps, pgqpNodeMap *map)
{
	if (ps->plan != NULL && ps->ExecProcNodeReal != NULL
		&& ps->ExecProcNodeReal != pgqp_ExecProcNode)
	{
		int			id = ps->plan->plan_node_id;

		/*
		 * Wrap the node only if its plan_node_id is valid and not used by
		 * another node with a different function.
		 */
		if (id >= 0 && id < map->nnodes
			&& (map->orig[id] == NULL || map->orig[id] == ps->ExecProcNodeReal))
		{
			map->orig[id] = ps->ExecProcNodeReal;
			ps->ExecProcNodeReal = pgqp_ExecProcNode;
		}
	}
	return planstate_tree_walker(ps, pgqp_wrap_walker, map);
}


/*
 * Replace ExecProcNodeReal of every plan node with pgqp_ExecProcNode().
 *
 * ExecProcNodeFirst() and ExecProcNodeInstr() call node->ExecProcNodeReal,
 * so replacing it after ExecInitNode() is enough to hook every invocation.
 */
static void
pgqp_install_node_wrappers(QueryDesc *queryDesc)
{
	EState	   *estate = queryDesc->estate;
	pgqpNodeMap *map;
	MemoryContext oldcxt;
	ListCell   *lc;
	int			maxid = -1;

	if (estate == NULL || queryDesc->planstate == NULL)
		return;

	pgqp_max_node_id_walker(queryDesc->planstate, &maxid);
	foreach(lc, estate->es_subplanstates)
	{
		PlanState  *ps = (PlanState *) lfirst(lc);

		if (ps != NULL)
			pgqp_max_node_id_walker(ps, &maxid);
	}
	if (maxid < 0)
		return;

	oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);
	map = (pgqpNodeMap *) palloc0(sizeof(pgqpNodeMap));
	map->estate = estate;
	map->nnodes = maxid + 1;
	map->orig = (ExecProcNodeMtd *) palloc0(sizeof(ExecProcNodeMtd) * map->nnodes);
	map->cb.func = pgqp_node_map_reset;
	map->cb.arg = map;
	MemoryContextRegisterResetCallback(estate->es_query_cxt, &map->cb);
	MemoryContextSwitchTo(oldcxt);

	/* Link the map before wrapping so that wrapped nodes can always find it */
	map->next = pgqp_node_maps;
	pgqp_node_maps = map;

	pgqp_wrap_walker(queryDesc->planstate, map);
	foreach(lc, estate->es_subplanstates)
	{
		PlanState  *ps = (PlanState *) lfirst(lc);

		if (ps != NULL)
			pgqp_wrap_walker(ps, map);
	}
}


/*
 * Return the leader's pid if this process is a parallel bgworker;
 * otherwise, i.e. this process is a leader process, return 0.
 *
 * This function cannot be invoked from pg_query_plan() and the
 * functions invoked by pg_query_plan() because this always returns 0.
 */
static int
get_leader_pid(void)
{
	PGPROC	   *leader;
	LWLock	   *leader_lwlock;
	int			pid = 0;

	leader = MyProc->lockGroupLeader;
	if (leader != NULL)
	{
		leader_lwlock = LockHashPartitionLockByProc(leader);
		LWLockAcquire(leader_lwlock, LW_SHARED);
		pid = leader->pid;
		LWLockRelease(leader_lwlock);
	}

	/*
	 * Note: When MyProc->lockGroupLeader is called twice or more during a
	 * transaction running, leader->pid returns its own pid even if leader.
	 * Thus, it should be compared pid and MyProcPid.
	 */
	return (pid != MyProcPid) ? pid : 0;
}


#ifdef __ADAPT_WORK_MEM__
static void
set_work_mem(const int wm)
{
	char		buf[32];

	sprintf(buf, "%i", wm);
	SetConfigOption("work_mem", buf, PGC_USERSET, PGC_S_SESSION);
}

static int
get_sort_space_used(const uint64 queryId)
{
	char		queryid[32];
	Relation	rel = NULL;
	Oid			relationId;
	Oid			relationPkeyId;
	ScanKeyData scanKey;
	SysScanDesc scanDescriptor = NULL;
	int			scanKeyCount = 1;
	bool		indexOK = true;
	HeapTuple	heapTuple = NULL;
	int			sort_space_used = 0;

	sprintf(queryid, "%llu", queryId);

	relationId = get_relname_relid("reg", LookupExplicitNamespace("query_plan", true));
	relationPkeyId = get_relname_relid("reg_pkey", LookupExplicitNamespace("query_plan", true));

	if (relationId == InvalidOid)
		return -1;

	rel = table_open(relationId, AccessShareLock);
	if (rel == NULL)
		return -1;

	ScanKeyInit(&scanKey, Anum_reg_queryid,
				BTEqualStrategyNumber, F_TEXTEQ, CStringGetTextDatum(queryid));
	scanDescriptor = systable_beginscan(rel, relationPkeyId, indexOK, NULL, scanKeyCount, &scanKey);
	heapTuple = systable_getnext(scanDescriptor);

	while (HeapTupleIsValid(heapTuple))
	{
		TupleDesc	tupleDescriptor = RelationGetDescr(rel);
		bool		isNullArray[Natts_reg];
		Datum		datumArray[Natts_reg];

		heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);
		if (!isNullArray[Anum_reg_sort_space_used - 1])
		{
			sort_space_used = DatumGetInt32(datumArray[Anum_reg_sort_space_used - 1]);
			break;
		}
		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(rel, NoLock);

	return sort_space_used;
}

#endif

/*
 * ExecutorStart hook
 */
static void
pgqp_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	int			instrument_options = 0;
	int			leader_pid;
#ifdef __ADAPT_WORK_MEM__
	int			sort_space_used = 0;
#endif

#ifdef __DEBUG__
	/* _test_count_hashtable(); */
#endif

#ifdef __ADAPT_WORK_MEM__
	pgqp_work_mem = false;
#endif

	/*
	 * Free all elements of reg_params that stores the regression parameters.
	 * This is used for query planners, so it must be cleaned up after
	 * planning.
	 */
	free_reg_params();

	if (nested_level == 0)
	{
		leader_pid = get_leader_pid();

		is_leader = (leader_pid == 0) ? true : false;
		if (leader_pid != 0)
		{
			/* This is a parallel worker. */
			if (find_hash_entry(leader_pid, IS_EXPLAIN))
			{
				/*
				 * Set is_explain to true, if this process is a parallel
				 * worker and the leader executes EXPLAIN statement.
				 *
				 * Parallel workers cannot recognize whether the original
				 * query is an EXPLAIN statement or not, and they just process
				 * a SELECT statement. However, in EXPLAIN ANALYZE, parallel
				 * workers will be crushed if the instrument_options does not
				 * set INSTRUMENT_TIMER option.
				 *
				 * Therefore, INSTRUMENT_TIMER is set to the
				 * instrument_options later if is_explain is true.
				 */
				is_explain = true;
			}
		}
	}

#ifdef __ADAPT_WORK_MEM__
	/*
	 * Experimental implementation: It temporarily expands the work_mem area
	 * during this query execution if the query has ever used temporary files.
	 */
	tmp_work_mem = atoi(GetConfigOption("work_mem", true, true));

	if ((sort_space_used = get_sort_space_used(queryDesc->plannedstmt->queryId)) > 0)
	{
		int			wm;

		pgqp_work_mem = true;
		wm = sort_space_used + tmp_work_mem;
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX_WORK_MEM (1024 * 500)	/* 500MB */
		set_work_mem(MIN(wm * 8, MAX_WORK_MEM));	/* wm*8 is heuristics. */
	}
#endif

	queryId[nested_level] = queryDesc->plannedstmt->queryId;
	starttime[nested_level] = GetCurrentTimestamp();

	/*
	 * The bgworker does not need to set the instrument options, QueryDesc and
	 * ExplainState.
	 */
	if (MyBackendType == B_BACKEND || IsParallelWorker())
	{
		/*
		 * The signal handler is set when the first query is executed in each
		 * session.
		 */
		if (!setSignalHandler)
		{
			pqsignal(SIGUSR2, sig_get_query_plan);
			setSignalHandler = true;
		}

		enable_show_plan = (pgqp_global_show_plan && pgqp_show_plan) ? true : false;
		enable_store_plan = (pgqp_global_store_plan && pgqp_store_plan) ? true : false;
		enable_store_plan &= enable_show_plan;

		if (is_explain)
			instrument_options |= INSTRUMENT_TIMER;

		instrument_options |= INSTRUMENT_ROWS;

#ifdef __ADDITIONAL_OPTIONS__
		if (pgqp_log_buffers)
			instrument_options |= INSTRUMENT_BUFFERS;
		if (pgqp_log_wal)
			instrument_options |= INSTRUMENT_WAL;
#endif
		queryDesc->instrument_options = instrument_options;

		if (nested_level < MAX_NESTED_LEVEL)
			qp_qd[nested_level] = queryDesc;
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	/*
	 * Install the safe point for pg_query_plan() requests into every plan
	 * node.
	 */
	if ((MyBackendType == B_BACKEND || IsParallelWorker())
		&& !(eflags & EXEC_FLAG_EXPLAIN_ONLY))
		pgqp_install_node_wrappers(queryDesc);
}


/*
 * ExecutorRun hook
 */
static void
pgqp_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
				 uint64 count)
{
	if (nested_level >= 0 && nested_level < MAX_NESTED_LEVEL)
		qp_run_qd[nested_level] = queryDesc;
	nested_level++;

#ifdef __ADAPT_WORK_MEM__
	/*
	 * This is a purely experimental implementation.
	 *
	 * Before executing a query, the system searches for the Sort node
	 * and estimates the memory required for the sorting operation.
	 * (Note: This estimate is strictly for testing and may not be accurate.)
	 *
	 * If the configured work_mem is less than the estimated memory,
	 * the work_mem value is temporarily increased to the estimated value.
	 *
	 * After the query finishes executing, the work_mem value is reset to
	 * its original configured value.
	 */
	if (MyBackendType == B_BACKEND || IsParallelWorker())
	{
		ExplainState *es;
		int			wm = 0;
		MemoryContext oldcxt;
		MemoryContext tmpcxt = AllocSetContextCreate(CurrentMemoryContext,
													 "adjust_work_mem_ctx",
													 ALLOCSET_DEFAULT_MINSIZE,
													 ALLOCSET_DEFAULT_INITSIZE,
													 ALLOCSET_DEFAULT_MAXSIZE);

		oldcxt = MemoryContextSwitchTo(tmpcxt);

		es = NewExplainState();

		if (queryDesc != NULL && queryDesc->planstate != NULL)
		{
		    wm = pgqp_ExplainQueryState(es, queryDesc);
			//elog(INFO, "%s  calc wm=%d", __func__, wm);
			if (wm > 0)
				pgqp_work_mem = true;
		}

		MemoryContextSwitchTo(oldcxt);
		MemoryContextDelete(tmpcxt);

		if (pgqp_work_mem && wm > tmp_work_mem)
		{
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX_WORK_MEM (1024 * 500)	/* 500MB */
			//elog(INFO, "%s  set work_mem=%d", __func__, wm);
			set_work_mem(MIN(wm, MAX_WORK_MEM));
		}
	}
#endif


	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
		nested_level--;
	}
	PG_CATCH();
	{
		nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * ExecutorFinish hook
 */
static void
pgqp_ExecutorFinish(QueryDesc *queryDesc)
{
	if (nested_level >= 0 && nested_level < MAX_NESTED_LEVEL)
		qp_run_qd[nested_level] = queryDesc;
	nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		nested_level--;
	}
	PG_CATCH();
	{
		nested_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorEnd hook
 */
static void
pgqp_ExecutorEnd(QueryDesc *queryDesc)
{
	TimestampTz currentTimestamp = GetCurrentTimestamp();

#ifdef __ADAPT_WORK_MEM__
	if (pgqp_work_mem == true)
		set_work_mem(tmp_work_mem);
#endif

	if (checkConditionOfLogInsertion(MyProcPid, currentTimestamp))
	{
		/* Store the query strings and executed plan to the ring buffer. */
		qi.starttime = starttime[nested_level];
		qi.endtime = currentTimestamp;

		/* Only SELECT commands are stored. */
		if (queryDesc->operation == CMD_SELECT)
			store_plan(qi, nested_level, queryId[nested_level]);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	if (nested_level == 0)
	{
		/* Reset valiables */
		is_explain = false;
		delete_hash_entry(MyProcPid);
	}
}


/*
 * ProcessUtility hook
 */
static void
pgqp_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
					bool readOnlyTree,
					ProcessUtilityContext context, ParamListInfo params,
					QueryEnvironment *queryEnv, DestReceiver *dest,
					QueryCompletion *qc)
{
	if (nodeTag(pstmt->utilityStmt) == T_ExplainStmt)
	{
		store_hash_entry(MyProcPid, IS_EXPLAIN, true);
		is_explain = true;
	}
	else
	{
		delete_hash_entry(MyProcPid);
		is_explain = false;
	}

	if (prev_ProcessUtility)
		prev_ProcessUtility(pstmt, queryString,
							readOnlyTree,
							context,
							params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString,
								readOnlyTree,
								context,
								params, queryEnv, dest, qc);
}


/*
 * hook function: this function is invoked by ClientAuthentication()@auth.c
 */
static void
pgqp_ClientAuthentication(Port *port, int status)
{
	sprintf(qi.database_name, "%s", port->database_name);
	sprintf(qi.user_name, "%s", port->user_name);
}


/*
 * Check whether the backend, whose status is beentry, is
 * a parallel worker of the leader whose process-id is pid.
 * Note:
 *      This function should be invoked from pg_query_plan().
 */
static bool
is_parallel_worker(const int pid, PgBackendStatus *beentry)
{
	PGPROC	   *proc = BackendPidGetProc(beentry->st_procpid);

	if (proc == NULL && (beentry->st_backendType != B_BACKEND))
		proc = AuxiliaryPidGetProc(beentry->st_procpid);

	if (proc != NULL)
	{
		PGPROC	   *leader;

		leader = proc->lockGroupLeader;
		if (leader != NULL)
			if (leader->pid != beentry->st_procpid && leader->pid == pid)
				return true;
	}
	return false;
}


/*
 * Check the condition whether the executed plan should be inserted into query_plan.log table.
 */
static bool
checkConditionOfLogInsertion(const int pid, const TimestampTz currentTimestamp)
{
	if (RecoveryInProgress())
		return false;			/* Currently, standby cannot insert logs */

	if (!is_leader)
		return false;			/* No need to insert logs since this is not a
								 * leader */

	if (is_explain)
		return false;			/* No need to insert logs since this command
								 * is EXPLAIN */

	if (!(enable_store_plan && pgqp_global_store_plan && pgqp_store_plan))
		return false;			/* No need to insert logs */

	if (!(enable_show_plan && pgqp_global_show_plan && pgqp_show_plan))
		return false;			/* It is unable to store plans if the showing
								 * query plan feature is disabled, because the
								 * executor does not count the actual
								 * statistics values. */

	if ((MyBackendType == B_BACKEND)
		&& (currentTimestamp - starttime[nested_level] > pgqp_log_min_duration * 1000000))
		return true;

	return false;
}

#ifdef __ADJUST_ROWS__

/*
 * join_search hook
 */
static RelOptInfo *
pgqp_join_search(PlannerInfo *root, int levels_needed,
				 List *initial_rels)
{
	RelOptInfo *rel = NULL;
	if (enable_geqo && levels_needed >= geqo_threshold)
		return geqo(root, levels_needed, initial_rels);

	if (!pgqp_adjust_rows)
	{
		if (prev_join_search)
			return (*prev_join_search) (root, levels_needed, initial_rels);
		else
			return standard_join_search(root, levels_needed, initial_rels);
	}

	rel = pgqp_standard_join_search(root, levels_needed, initial_rels);

	return rel;
}


/*
 * planner hook
 */
static PlannedStmt *
pgqp_planner(Query *parse, const char *query_string, int cursorOptions,
			 ParamListInfo boundParams)
{
	PlannedStmt *result;
	char		queryid[32];
	char	   *params = NULL;

	pgqp_adjust_rows = false;
	sprintf(queryid, "%llu", parse->queryId);

	/*
	 * Check query_plan.reg
	 */
	if ((pgqp_enable_adjust_joinrel_rows || pgqp_enable_adjust_rel_rows)
		&& (params = selectParams(queryid)) != NULL)
	{
		if (set_reg_params(parse, params))
		{
			pgqp_adjust_rows = true;
			set_current_state();
		}
	}

	if (prev_planner)
		result = prev_planner(parse, query_string, cursorOptions, boundParams);
	else
		result = pgqp_standard_planner(parse, query_string, cursorOptions, boundParams);

	if (pgqp_adjust_rows)
		set_join_config_options(current_state.init_join_mask, false,
								current_state.context);

	return result;
}

#endif							/* __ADJUST_ROWS__ */

/*
 * Get planId of the specified json plan. This is a helper function.
 */
Datum
get_planid(PG_FUNCTION_ARGS)
{
	char		planid[32];
	text	   *json_plan_text = PG_GETARG_TEXT_P(0);
	char	   *json_plan = text_to_cstring(json_plan_text);

	pgqp_json_plan = json_plan;
	pre_plan_parse(strlen(json_plan));
	if (plan_parse() != 0)
		elog(WARNING, "Warning: Parse error in the json plan.");
	sprintf(planid, "%llu", get_planId());

	PG_RETURN_TEXT_P(cstring_to_text_with_len(planid, strlen(planid)));
}


/*
 * Release the request slot held by this process, if any.
 *
 * The slot is reset to PGQP_SLOT_IDLE only if it still belongs to our
 * request (i.e. the generation matches).  If the target responds at the
 * same moment, the CAS fails and we simply retry.
 */
static void
pgqp_release_held_slot(void)
{
	pgqpRequestSlot *slot;

	if (pgqp_held_slot < 0 || pgqp == NULL)
		return;

	slot = &pgqp->slots[pgqp_held_slot];
	for (;;)
	{
		uint32		cur = pg_atomic_read_u32(&slot->state);

		if (PGQP_SLOT_GEN(cur) != pgqp_held_gen
			|| PGQP_SLOT_STATE(cur) == PGQP_SLOT_IDLE)
			break;
		if (pg_atomic_compare_exchange_u32(&slot->state, &cur,
										   PGQP_SLOT_MAKE(pgqp_held_gen,
														  PGQP_SLOT_IDLE,
														  RESULT_OK)))
			break;
	}
	pgqp_held_slot = -1;
}


static void
pgqp_caller_exit(int code, Datum arg)
{
	pgqp_release_held_slot();
}


/*
 * Try to claim the request slot.  Returns true and sets *gen on success.
 *
 * A slot that is not idle is normally in use by another caller.  However,
 * if the caller that owns the slot has exited without releasing it, the
 * slot is taken over.
 */
static bool
pgqp_claim_slot(pgqpRequestSlot *slot, uint32 *gen)
{
	uint32		cur = pg_atomic_read_u32(&slot->state);

	for (;;)
	{
		uint32		newval;

		if (PGQP_SLOT_STATE(cur) != PGQP_SLOT_IDLE)
		{
			pid_t		owner;

			/*
			 * caller_pid is fixed before the slot becomes PENDING, so check
			 * it only in PENDING or DONE.
			 */
			if (PGQP_SLOT_STATE(cur) == PGQP_SLOT_CLAIMED)
				return false;
			pg_read_barrier();
			owner = slot->caller_pid;
			if (owner == InvalidPid || is_alive(owner))
				return false;
		}

		newval = PGQP_SLOT_MAKE(PGQP_NEXT_GEN(PGQP_SLOT_GEN(cur)),
								PGQP_SLOT_CLAIMED, RESULT_OK);
		if (pg_atomic_compare_exchange_u32(&slot->state, &cur, newval))
		{
			*gen = PGQP_SLOT_GEN(newval);
			return true;
		}
		/* cur has been updated by the CAS; check again */
	}
}


/*
 * Wait for the target's response, at most PGQP_REQUEST_TIMEOUT_MS.
 *
 * Returns the result (qpResultState) set by the target, PGQP_REQ_TIMEOUT
 * or PGQP_REQ_GONE.
 */
static int
pgqp_wait_response(pgqpRequestSlot *slot, uint32 gen, PGPROC *proc, pid_t pid)
{
	TimestampTz start = GetCurrentTimestamp();

	for (;;)
	{
		uint32		cur = pg_atomic_read_u32(&slot->state);

		if (PGQP_SLOT_GEN(cur) != gen)
			return RESULT_ERROR;	/* should not happen */
		if (PGQP_SLOT_STATE(cur) == PGQP_SLOT_DONE)
		{
			pg_read_barrier();
			return PGQP_SLOT_RESULT(cur);
		}

		if (proc->pid != pid)
			return PGQP_REQ_GONE;

		if (TimestampDifferenceExceeds(start, GetCurrentTimestamp(),
									   PGQP_REQUEST_TIMEOUT_MS))
			return PGQP_REQ_TIMEOUT;

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 PGQP_WAIT_INTERVAL_MS, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
}


/*
 * Sleep for the busy retry interval.
 */
static void
pgqp_sleep_for_retry(void)
{
	(void) WaitLatch(MyLatch,
					 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					 PGQP_BUSY_RETRY_INTERVAL_MS, PG_WAIT_EXTENSION);
	ResetLatch(MyLatch);
	CHECK_FOR_INTERRUPTS();
}


/*
 * Request the query plans of the process (procno, pid).
 *
 * On RESULT_OK, *segp is the attached dsm segment that contains the plans;
 * the caller must detach it.  Otherwise, returns another qpResultState or
 * one of PGQP_REQ_XXX.
 *
 * Retries at most PGQP_REQUEST_MAX_RETRY times in total when the slot is
 * busy or the segment is too small.
 */
static int
pgqp_request_plans(ProcNumber procno, pid_t pid, dsm_segment **segp)
{
	pgqpRequestSlot *slot = &pgqp->slots[procno];
	PGPROC	   *proc = GetPGProcByNumber(procno);
	Size		capacity = PGQP_INITIAL_SEGMENT_SIZE;
	int			retry = 0;

	*segp = NULL;

	if (!pgqp_exit_callback_registered)
	{
		before_shmem_exit(pgqp_caller_exit, (Datum) 0);
		pgqp_exit_callback_registered = true;
	}

	for (;;)
	{
		dsm_segment *seg;
		pgqpSegHeader *hdr;
		uint32		gen;
		int			result;

		CHECK_FOR_INTERRUPTS();

		if (proc->pid != pid)
			return PGQP_REQ_GONE;

		/*
		 * Create the segment for this request.  It is owned by this process
		 * (via CurrentResourceOwner), so it is destroyed automatically on
		 * error, and when both this process and the target have detached.
		 */
		seg = dsm_create(add_size(MAXALIGN(sizeof(pgqpSegHeader)), capacity), 0);
		hdr = (pgqpSegHeader *) dsm_segment_address(seg);
		memset(hdr, 0, sizeof(pgqpSegHeader));
		hdr->magic = PGQP_SEGMENT_MAGIC;
		hdr->capacity = capacity;
		hdr->result = RESULT_ERROR;

		if (!pgqp_claim_slot(slot, &gen))
		{
			dsm_detach(seg);
			if (++retry > PGQP_REQUEST_MAX_RETRY)
				return PGQP_REQ_BUSY;
			pgqp_sleep_for_retry();
			continue;
		}
		pgqp_held_slot = procno;
		pgqp_held_gen = gen;

		PG_TRY();
		{
			hdr->generation = gen;
			slot->caller = MyProcNumber;
			slot->caller_pid = MyProcPid;
			slot->handle = dsm_segment_handle(seg);

			/* Publish the request; the exchange is a full memory barrier. */
			(void) pg_atomic_exchange_u32(&slot->state,
										  PGQP_SLOT_MAKE(gen, PGQP_SLOT_PENDING,
														 RESULT_OK));

			if (proc->pid != pid || kill(pid, SIGUSR2) < 0)
				result = PGQP_REQ_GONE;
			else
				result = pgqp_wait_response(slot, gen, proc, pid);
		}
		PG_CATCH();
		{
			pgqp_release_held_slot();
			PG_RE_THROW();
		}
		PG_END_TRY();

		pgqp_release_held_slot();

		if (result == RESULT_TOO_SMALL)
		{
			/* Retry with the size that the target requires. */
			capacity = Max(hdr->required, capacity);
			dsm_detach(seg);
			if (++retry > PGQP_REQUEST_MAX_RETRY)
				return RESULT_TOO_SMALL;
			continue;
		}

		if (result == RESULT_OK && hdr->result == RESULT_OK
			&& hdr->generation == gen)
		{
			*segp = seg;
			return RESULT_OK;
		}

		dsm_detach(seg);
		return (result == RESULT_OK) ? RESULT_ERROR : result;
	}
}


/*
 * Convert a string in the segment to a text datum in the database encoding.
 */
static Datum
pgqp_text_datum(const char *str, uint32 len, int encoding, bool *isnull)
{
	char	   *converted;

	if (len == 0)
	{
		*isnull = true;
		return (Datum) 0;
	}

	*isnull = false;
	converted = (char *) pg_do_encoding_conversion((unsigned char *) str, len,
												   encoding,
												   GetDatabaseEncoding());
	if (converted == str)
		return PointerGetDatum(cstring_to_text_with_len(str, len));
	return PointerGetDatum(cstring_to_text(converted));
}


/*
 * pg_query_plan function returns the specified process's query plan.
 *
 * Processing outline:
 *
 * Caller (the process that invokes this function):
 *  1. Creates a dsm segment for the request.
 *  2. Claims the request slot of the target, which is indexed by the
 *     target's ProcNumber, writes the segment handle into it, makes it
 *     PENDING and sends SIGUSR2 to the target.
 *  3. Waits on its latch until the slot becomes DONE, at most
 *     PGQP_REQUEST_TIMEOUT_MS.
 *  4. Reads the query plans from the segment and returns them.
 *
 * Target:
 *  1. The signal handler only sets a flag (or responds immediately if no
 *     query is running or the feature is disabled).
 *  2. At the next invocation of any plan node, pgqp_ExecProcNode() builds
 *     the query plans, attaches the segment, writes them, detaches it, makes
 *     the slot DONE and sets the caller's latch.
 *
 * Unlike the former implementation, no global lock is held while waiting,
 * so requests to different processes can run concurrently.  When the slot
 * of the target is busy (another caller is requesting it), the request is
 * retried at most PGQP_REQUEST_MAX_RETRY times.
 */
Datum
pg_query_plan(PG_FUNCTION_ARGS)
{
	int			pid = PG_GETARG_INT32(0);
	int			num_backends;
	int			curr_backend;

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	bool		_exist = false;
	bool		has_data = false;
	bool		is_enabled = true;
	bool		warned = false;		/* a warning about a failed request has
									 * been emitted */
	bool		leader_gone = false;	/* the target exited while waiting */

	if (pid == MyProcPid)
		elog(ERROR, "This function cannot be run against itself.");

	if (!is_alive(pid))
		elog(ERROR, "The process (pid=%d) does not exist.", pid);

	/* Check library */
	if (!pgqp)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_query_plan must be loaded via shared_preload_libraries")));

	/* Check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/*
	 * The function must be defined with the 10 output columns.  An old
	 * definition without planid would silently shift the columns (e.g. the
	 * plan column would show the planid), so reject it.
	 */
#define PG_QUERY_PLAN_NUM_COLS 10
	if (tupdesc->natts != PG_QUERY_PLAN_NUM_COLS)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
				 errmsg("the definition of pg_query_plan() is outdated: it returns %d columns, but %d are expected",
						tupdesc->natts, PG_QUERY_PLAN_NUM_COLS),
				 errhint("Recreate the function with pg_query_plan--1.0.sql, which has the planid column.")));
#undef PG_QUERY_PLAN_NUM_COLS

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Main loop
	 *
	 * Note: The local backend status array is indexed from 1 to
	 * num_backends; its entries are not indexed by ProcNumber.
	 */
	num_backends = pgstat_fetch_stat_numbackends();

	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
#define PG_QUERY_PLAN_COLS 10	/* pid, database, worker_type, nested_level,
								 * queryid, query_start, query, planid, plan,
								 * plan_json */
		Datum		values[PG_QUERY_PLAN_COLS];
		bool		nulls[PG_QUERY_PLAN_COLS];
		LocalPgBackendStatus *local_beentry;
		PgBackendStatus *beentry;
		ProcNumber	procno;
		bool		parallel_worker;
		dsm_segment *seg;
		int			result;
#undef PG_QUERY_PLAN_COLS
		local_beentry = pgstat_get_local_beentry_by_index(curr_backend);
		if (local_beentry == NULL)
			continue;
		beentry = &local_beentry->backendStatus;
		procno = local_beentry->proc_number;

		parallel_worker = is_parallel_worker(pid, beentry);

		/* Only the target process and its parallel workers are processed. */
		if (pid != beentry->st_procpid && !parallel_worker)
			continue;

		if (pid == beentry->st_procpid)
			_exist = true;

		if (!is_member_of_role(GetUserId(), beentry->st_userid))
			elog(ERROR, "You do not have privileges to show the %d's query plan.", pid);

		if (!(beentry->st_state == STATE_RUNNING
			  && (beentry->st_backendType == B_BACKEND || beentry->st_backendType == B_BG_WORKER)
			  && (GetCurrentTimestamp() - beentry->st_activity_start_timestamp > 1 * 1000000 /* 1[sec] */ )))
			continue;

		if (procno == INVALID_PROC_NUMBER || procno >= MaxBackends)
			continue;

		/*
		 * Request the query plans of this process.
		 */
		result = pgqp_request_plans(procno, beentry->st_procpid, &seg);

		switch (result)
		{
			case RESULT_OK:
				break;
			case RESULT_DISABLE:
				/* The showing query plan feature is disabled */
				is_enabled = false;
				continue;
			case RESULT_NO_QUERY:
				continue;
			case PGQP_REQ_GONE:
				/* A parallel worker that has finished is simply skipped. */
				if (!parallel_worker)
					leader_gone = true;
				continue;
			case PGQP_REQ_BUSY:
				elog(ERROR, "The process (pid=%d) is busy with another pg_query_plan request; gave up after %d retries.",
					 beentry->st_procpid, PGQP_REQUEST_MAX_RETRY);
				break;
			case PGQP_REQ_TIMEOUT:
				elog(WARNING, "The process (pid=%d) did not respond within %d [msec].",
					 beentry->st_procpid, PGQP_REQUEST_TIMEOUT_MS);
				warned = true;
				continue;
			case RESULT_TOO_SMALL:
				elog(WARNING, "Could not get the query plan of the process (pid=%d): the plan kept growing after %d retries.",
					 beentry->st_procpid, PGQP_REQUEST_MAX_RETRY);
				warned = true;
				continue;
			default:
				elog(WARNING, "Could not get the query plan of the process (pid=%d).",
					 beentry->st_procpid);
				warned = true;
				continue;
		}

		/*
		 * Read the query plan data from the segment
		 */
		{
			pgqpSegHeader *hdr = (pgqpSegHeader *) dsm_segment_address(seg);
			char	   *data = PGQP_SEG_DATA(hdr);
			Size		off = 0;
			int			j;

			for (j = 0; j < hdr->nlevels; j++)
			{
				pgqpSegEntry *e = (pgqpSegEntry *) (data + off);
				char	   *q;
				char	   *t;
				char	   *js;
				Size		entry_size;
				int			k = 0;
				char		buf[32];

				entry_size = MAXALIGN(sizeof(pgqpSegEntry)
									  + (Size) e->len[PRINT_QUERY] + 1
									  + (Size) e->len[PRINT_PLAN] + 1
									  + (Size) e->len[PRINT_PLAN_JSON] + 1);
				if (off + entry_size > hdr->used || hdr->used > hdr->capacity)
				{
					elog(WARNING, "Broken query plan data from the process (pid=%d).",
						 beentry->st_procpid);
					break;
				}
				q = (char *) e + sizeof(pgqpSegEntry);
				t = q + e->len[PRINT_QUERY] + 1;
				js = t + e->len[PRINT_PLAN] + 1;
				off += entry_size;

				memset(values, 0, sizeof(values));
				memset(nulls, 0, sizeof(nulls));

				/* pid */
				values[k++] = Int32GetDatum(beentry->st_procpid);

				/* database */
				values[k++] = CStringGetTextDatum(hdr->database_name);

				/* worker_type */
				if (parallel_worker)
					values[k++] = CStringGetTextDatum("parallel worker");
				else
					values[k++] = CStringGetTextDatum("leader");

				/* nested_level */
				values[k++] = Int32GetDatum(j);

				/* queryid */
				snprintf(buf, sizeof(buf), UINT64_FORMAT, e->queryId);
				values[k++] = CStringGetTextDatum(buf);

				/* start time */
				if (beentry->st_activity_start_timestamp != 0)
					values[k++] = TimestampTzGetDatum(beentry->st_activity_start_timestamp);
				else
					nulls[k++] = true;

				/* query */
				values[k] = pgqp_text_datum(q, e->len[PRINT_QUERY], hdr->encoding, &nulls[k]);
				k++;

				/* planid */
				snprintf(buf, sizeof(buf), UINT64_FORMAT, e->planId);
				values[k++] = CStringGetTextDatum(buf);

				/* query plan in text */
				values[k] = pgqp_text_datum(t, e->len[PRINT_PLAN], hdr->encoding, &nulls[k]);
				k++;

				/* query plan in json */
				values[k] = pgqp_text_datum(js, e->len[PRINT_PLAN_JSON], hdr->encoding, &nulls[k]);
				k++;

				tuplestore_putvalues(tupstore, tupdesc, values, nulls);
				has_data = true;
			}
		}

		dsm_detach(seg);
	}							/* end for loop */

	if (!_exist)
		elog(ERROR, "The process (pid=%d) does not exist.", pid);

	if (!is_enabled)
		elog(ERROR, "The showing query plan feature is disabled in the process (pid=%d).", pid);

	/*
	 * Report why no data is returned.  If a warning has already explained
	 * the failure (e.g. timeout), the process is not idle, so say nothing.
	 */
	if (!has_data && !warned)
	{
		if (leader_gone)
			elog(INFO, "The process (pid=%d) exited before responding.", pid);
		else
			elog(INFO, "The process (pid=%d) is in idle state.", pid);
	}

	return (Datum) 0;
}
