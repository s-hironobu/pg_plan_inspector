/*-------------------------------------------------------------------------
 * pg_query_plan.c
 *
 * Show the query plan of the running query. And, similar to auto_explain,
 * store the executed plan into the query_plan.log table in postgres database.
 *
 * Copyright (c) 2021-2022, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#define __ADJUST_ROWS__

#include "funcapi.h"
#include "libpq/pqsignal.h"
#include "libpq/auth.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/parallel.h"
#include "access/xact.h"
#include "access/xlog.h"
#if PG_VERSION_NUM < 140000
#include "parser/analyze.h"
#include "postmaster/autovacuum.h"
#endif
#include "common/hashfn.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#if PG_VERSION_NUM >= 140000
#include "storage/proc.h"
#endif
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#if PG_VERSION_NUM >= 140000
#include "utils/queryjumble.h"
#endif

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
#include "common.h"
#include "buffer.h"
#include "bgworker.h"
#include "pgqp_explain.h"
#include "hash.h"
#include "qpam.h"
#if PG_VERSION_NUM < 140000
#include "pg_stat_statements/pg_stat_statements.h"
#endif
#include "planid.h"
#include "param.h"

PG_MODULE_MAGIC;

#ifdef __ADJUST_ROWS__
extern CurrentState current_state;
extern regParams reg_params;
extern bool pgqp_adjust_rows;
#endif

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
static bool pgqp_log_buffers;
static bool pgqp_log_wal;
#endif
bool		pgqp_received_signal;

/* Query info */
static queryInfo qi;

/* Links to QueryDesc and ExplainState */
QueryDesc  *qp_qd[MAX_NESTED_LEVEL];
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

/*
 * Flag set by signal handlers
 */
static volatile sig_atomic_t got_siguser2 = false;

/* Saved hook values in case of unload */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
#if PG_VERSION_NUM < 140000
static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
#endif
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static ClientAuthentication_hook_type prev_ClientAuthentication = NULL;
#ifdef __ADJUST_ROWS__
static planner_hook_type prev_planner = NULL;
static join_search_hook_type prev_join_search = NULL;
set_rel_pathlist_hook_type prev_set_rel_pathlist = NULL;
set_join_pathlist_hook_type prev_set_join_pathlist = NULL;
#endif

/*
 * Function declarations
 */
void		_PG_init(void);
void		_PG_fini(void);

#if PG_VERSION_NUM < 140000
static void pgqp_post_parse_analyze(ParseState *pstate, Query *query);
#endif
static void pgqp_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgqp_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
							 uint64 count, bool execute_once);
static void pgqp_ExecutorFinish(QueryDesc *queryDesc);
static void pgqp_ExecutorEnd(QueryDesc *queryDesc);
static void pgqp_ClientAuthentication(Port *port, int status);
static void pgqp_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
#if PG_VERSION_NUM >= 140000
								bool readOnlyTree,
#endif
								ProcessUtilityContext context,
								ParamListInfo params, QueryEnvironment *queryEnv,
								DestReceiver *dest, QueryCompletion *qc);
#ifdef __ADJUST_ROWS__
static PlannedStmt *pgqp_planner(Query *parse, const char *query_string, int cursorOptions,
								 ParamListInfo boundParams);

static RelOptInfo *pgqp_join_search(PlannerInfo *root, int levels_needed,
									List *initial_rels);
#endif

static void pgqp_shmem_startup(void);
static void pgqp_shmem_shutdown(int code, Datum arg);
static Size pgqp_memsize(void);
static bool is_parallel_worker(const int pid, PgBackendStatus *beentry);
static int	get_leader_pid(void);
static void init_pgqp(struct queryPlanData *qpd, struct queryInfo *qi);
static bool checkConditionOfLogInsertion(const int pid,
										 const TimestampTz currentTimestamp);
static void sig_get_query_plan(SIGNAL_ARGS);

Datum		pg_query_plan(PG_FUNCTION_ARGS);
Datum		get_planid(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_query_plan);
PG_FUNCTION_INFO_V1(get_planid);


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

	RequestAddinShmemSpace(pgqp_memsize());
	RequestNamedLWLockTranche("pg_query_plan", 1);

	/* Install hooks. */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgqp_shmem_startup;

#if PG_VERSION_NUM < 140000
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pgqp_post_parse_analyze;
#endif

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

	if (!IsParallelWorker())
	{
		prev_ClientAuthentication = ClientAuthentication_hook;
		ClientAuthentication_hook = pgqp_ClientAuthentication;

		prev_ProcessUtility = ProcessUtility_hook;
		ProcessUtility_hook = pgqp_ProcessUtility;
	}

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

	/* initialize variable */
	is_explain = false;
	pgqp_received_signal = false;
#ifdef __ADJUST_ROWS__
	pgqp_adjust_rows = false;

	init_param_parse_env();
#endif

#if PG_VERSION_NUM >= 140000
	/* Enables query identifier computation. */
	EnableQueryId();
#endif
}


void
_PG_fini(void)
{
	/* Uninstall hooks. */
#if PG_VERSION_NUM < 140000
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
#endif
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

	pgqp = ShmemInitStruct("pg_query_plan", sizeof(pgqpSharedState), &found);

	if (!found)
	{
		/* First time through ... */
		pgqp->lock = &(GetNamedLWLockTranche("pg_query_plan"))->lock;
		pgqp->htlock = &(GetNamedLWLockTranche("pg_query_plan"))->lock;

		SpinLockInit(&pgqp->elock);
		SpinLockInit(&pgqp->nwblock);
		for (i = 0; i < BUFFER_SIZE; i++)
			SpinLockInit(&pgqp->bd[i].bslock);
	}

	/* Initialize hash table */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(pgqpHashKey);
	info.entrysize = sizeof(pgqpEntry);
	pgqp_hash = ShmemInitHash("pg_query_plan hash",
							  pgqp_hash_max, pgqp_hash_max,
							  &info,
							  HASH_ELEM | HASH_BLOBS);

	/* Set initial values */
	init_pgqp(&pgqp->qpd, &pgqp->qi);

	/*
	 * pgqp->wptr and pgqp->bd[] are initialized in
	 * create_ring_buffer()@bgworker.c
	 */
	pgqp->bgworkerPid = InvalidPid;
	pgqp->resultState = RESULT_OK;

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
	size = add_size(size, hash_estimate_size(pgqp_hash_max, sizeof(pgqpEntry)));

	return size;
}

#define PROCESS_FINISHED(void) do \
	{											\
		SpinLockAcquire(&pgqp->elock);			\
		pgqp->finished = true;					\
		SpinLockRelease(&pgqp->elock);			\
	} while(0);


/*
 * Initialize pgqp
 */
static void
init_pgqp(struct queryPlanData *qpd, struct queryInfo *qi)
{
	pgqp->latch = NULL;
	pgqp->finished = false;
	pgqp->qpd.encoding = -1;
	pgqp->callerPID = InvalidPid;
	pgqp->targetPID = InvalidPid;
	pgqp->resultState = RESULT_OK;

	init_qpd(qpd);
	init_qi(qi);
}


/*
 * Signal handler function
 *
 * When a signal (the default is SIGUSR2) is received, this function invokes
 * pgqpExplainPrintQueryState() to get the query plan, and then writes
 * the getting data to pgqp->queryPlan.
 * After that, this function sets the latch to wake the caller process up.
 *
 * Note: During the processing of this function, the caller process
 *       holds the pgqp->lock. Thus, it does not need to acquire
 *       any lock because no process writes pgqp->qpd except this process.
 */
static void
sig_get_query_plan(SIGNAL_ARGS)
{
	int			i;
	int			save_errno = errno;
	MemoryContext oldcxt;
	QueryDesc  *queryDesc;

	PG_SETMASK(&BlockSig);

	/*
	 * Check the showing query plan feature is enabled or not
	 */
	if (!(enable_show_plan && pgqp_global_show_plan && pgqp_show_plan))
	{
		pgqp->resultState = RESULT_DISABLE;
		PROCESS_FINISHED();
		goto final;
	}

	/*
	 * Check queryDesc and ExplainState
	 */
	for (i = 0; i < nested_level; i++)
	{
		if (qp_qd[i] == NULL)
		{
			pgqp->resultState = RESULT_NO_QUERY;
			PROCESS_FINISHED();
			goto final;
		}
		else if (qp_qd[i]->planstate == NULL)
		{
			pgqp->resultState = RESULT_NO_QUERY;
			PROCESS_FINISHED();
			goto final;
		}
	}

	got_siguser2 = true;

	/*
	 * Get the query plans and Write them into pgqp->qpd on the shared memory.
	 */
	pgqp_received_signal = true;

	for (i = 0; i < nested_level; i++)
	{
		if (i < MAX_NESTED_LEVEL)
		{
			/*
			 * Write the query string to pgqp->queryPlan_query[]
			 */
			set_query(&pgqp->qpd, i, queryId[i], true);

			/*
			 * Get two kinds of query plans and Set them to
			 * pgqp->queryPlan_plan[] and pgqp->queryPlan_json[]
			 */
			queryDesc = qp_qd[i];
			oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
			qp_es[i] = NewExplainState();

			set_plan(&pgqp->qpd, i, true, PRINT_PLAN);
			set_plan(&pgqp->qpd, i, true, PRINT_PLAN_JSON);

			MemoryContextSwitchTo(oldcxt);
		}
	}


	pgqp->qpd.encoding = GetDatabaseEncoding();

	/*
	 * Set the database and user names
	 */
	memset((void *) pgqp->qi.database_name, '\0', sizeof(qi.database_name));
	memset((void *) pgqp->qi.user_name, '\0', sizeof(qi.user_name));
	memcpy((void *) pgqp->qi.database_name, qi.database_name, strlen(qi.database_name));
	memcpy((void *) pgqp->qi.user_name, qi.user_name, strlen(qi.user_name));

	PROCESS_FINISHED();

final:

	pgqp_received_signal = false;

	/*
	 * Set the caller's latch to wake up the caller process.
	 */
	if (pgqp->latch != NULL)
		SetLatch(pgqp->latch);

	PG_SETMASK(&UnBlockSig);

	errno = save_errno;
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


#if PG_VERSION_NUM < 140000
/*
 * Post-parse-analysis hook: mark query with a queryId
 */
static void
pgqp_post_parse_analyze(ParseState *pstate, Query *query)
{
	pgssJumbleState jstate;

	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query);

	/* Assert we didn't do this already */
	Assert(query->queryId == UINT64CONST(0));

	/* Set up workspace for query jumbling */
	jstate.jumble = (unsigned char *) palloc(JUMBLE_SIZE);
	jstate.jumble_len = 0;
	jstate.clocations_buf_size = 32;
	jstate.clocations = (pgssLocationLen *)
		palloc(jstate.clocations_buf_size * sizeof(pgssLocationLen));
	jstate.clocations_count = 0;
	jstate.highest_extern_param_id = 0;

	/* Compute query ID and mark the Query node with it */
	JumbleQuery(&jstate, query);
	query->queryId =
		DatumGetUInt64(hash_any_extended(jstate.jumble, jstate.jumble_len, 0));
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


#ifdef __DEBUG__
	/* _test_count_hashtable(); */
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
}


/*
 * ExecutorRun hook
 */
static void
pgqp_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
				 uint64 count, bool execute_once)
{
	nested_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
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

	if (checkConditionOfLogInsertion(MyProcPid, currentTimestamp))
	{
		/* Store the query strings and executed plan to the ring buffer. */
		qi.starttime = starttime[nested_level];
		qi.endtime = currentTimestamp;

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
#if PG_VERSION_NUM >= 140000
					bool readOnlyTree,
#endif
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
#if PG_VERSION_NUM >= 140000
							readOnlyTree,
#endif
							context,
							params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString,
#if PG_VERSION_NUM >= 140000
								readOnlyTree,
#endif
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
	sprintf(queryid, "%lu", parse->queryId);

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
	sprintf(planid, "%lu", get_planId());

	PG_RETURN_TEXT_P(cstring_to_text_with_len(planid, strlen(planid)));
}


/*
 * pg_query_plan function returns the specified process's query plan.
 *
 * Processing outline:
 * When this function is invoked on the backend process by a caller, the
 * process sets MyLatch to the pgqpSharedState on shared-memory and sends a
 * SIGUSR2 signal to the specified process (target process).
 * After sending the signal, the caller's backend waits till the end of
 * the target process' signal handler job.
 *
 * When receiving SIGUSR2, the target process switches to run the signal
 * handler function sig_get_query_plan(). The handler function gathers
 * all instruments data and writes them to the pgqpSharedState on shared-memory.
 * After writing all, the target process executes the SetLatch() function to
 * wake the caller's backend up and resumes the process that was going
 * on before the signal was received.
 *
 * When the caller's backend wakes up by receiving the latch, the backend process
 * reads the target process's query plan on the pgqpSharedState and displays it.
 *
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

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Main loop
	 */
	num_backends = pgstat_fetch_stat_numbackends();

	for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
	{
#define PG_QUERY_PLAN_COLS 10	/* pid, database, worker_type, nested_level,
								 * queryid, query_start, query, planid, plan,
								 * plan_json */
		Datum		values[PG_QUERY_PLAN_COLS];
		bool		nulls[PG_QUERY_PLAN_COLS];
		PgBackendStatus *beentry = NULL;
		bool		parallel_worker;
#undef PG_QUERY_PLAN_COLS
		beentry = pgstat_fetch_stat_beentry(curr_backend);

		if (beentry == NULL)
			continue;

		parallel_worker = is_parallel_worker(pid, beentry);

		if (pid == beentry->st_procpid)
			_exist = true;

		if (!is_member_of_role(GetUserId(), beentry->st_userid))
			elog(ERROR, "You do not have privileges to show the %d's query plan.", pid);

		if ((pid == beentry->st_procpid || parallel_worker)
			&& beentry->st_state == STATE_RUNNING
			&& (beentry->st_backendType == B_BACKEND || beentry->st_backendType == B_BG_WORKER)
			&& (GetCurrentTimestamp() - beentry->st_activity_start_timestamp > 1 * 1000000 /* 1[sec] */ ))
		{
			int			j;

			LWLockAcquire(pgqp->lock, LW_EXCLUSIVE);

			/* Initialize pgqp. */
			init_pgqp(&pgqp->qpd, &pgqp->qi);

			/* Set MyLatch. */
			pgqp->latch = MyLatch;

			/* Set values to pgqp. */
			pgqp->callerPID = MyProcPid;
			pgqp->targetPID = pid;

			/* Send a signal to invoke the target's signal handler function. */
			kill(beentry->st_procpid, SIGUSR2);

			/* Wait for finishing of the target' signal handler processing. */
			while (true)
			{
				int			rc;

				/* Check the process is still alive. */
				if (!is_alive(beentry->st_procpid))
					goto next;

				rc = WaitLatch(pgqp->latch,
							   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
							   100L, PG_WAIT_EXTENSION);
				ResetLatch(pgqp->latch);

				/* Emergency bailout if postmaster has died. */
				if (rc & WL_POSTMASTER_DEATH)
					proc_exit(1);

				/* Break this loop if the target's signal handler is finished. */
				SpinLockAcquire(&pgqp->elock);
				if (pgqp->finished == true)
				{
					SpinLockRelease(&pgqp->elock);
					break;
				}
				SpinLockRelease(&pgqp->elock);
			}					/* end while loop */

			/* Check the result state */
			if (pgqp->resultState == RESULT_DISABLE)
			{
				/* The showing query plan feature is disabled */
				is_enabled = false;
				goto next;
			}
			else if (pgqp->resultState == RESULT_NO_QUERY)
			{
				has_data = false;
				goto next;
			}

			/*
			 * Read the query plan data from pgqp->qpd
			 */
			Assert(pgqp->finished == true);
			if (pid != pgqp->targetPID || MyProcPid != pgqp->callerPID)
			{
				elog(ERROR, "Could not get the requested data.");
				return (Datum) 0;
			}

			if (pgqp->qpd.totalLen[PRINT_PLAN] > 0 || pgqp->qpd.totalLen[PRINT_PLAN_JSON] > 0)
				has_data |= true;

			for (j = 0; j <= pgqp->qpd.nested_level[PRINT_PLAN]; j++)
				/* The values of all nested_level[] are equal. */
			{
				int			k = 0;
				bool		is_null;
				char		queryid[32];
				char		planid[32];

				/* Set values */
				memset(values, 0, sizeof(values));
				memset(nulls, 0, sizeof(nulls));

				/* pid */
				values[k++] = ObjectIdGetDatum(beentry->st_procpid);

				/* database */
				values[k++] = CStringGetTextDatum(pgqp->qi.database_name);

				/* worker_type */
				if (parallel_worker)
					values[k++] = CStringGetTextDatum("parallel worker");
				else
					values[k++] = CStringGetTextDatum("leader");

				/* nested_level */
				values[k++] = ObjectIdGetDatum(j);

				/* queryid */
				sprintf(queryid, "%lu", pgqp->qpd.queryId[j]);
				values[k++] = CStringGetTextDatum(queryid);

				/* start time */
				if (beentry->st_activity_start_timestamp != 0)
					values[k++] = TimestampTzGetDatum(beentry->st_activity_start_timestamp);
				else
					nulls[k++] = true;

				/* query */
				values[k] = CStringGetTextDatum(get_query_plan(&pgqp->qpd, PRINT_QUERY, &is_null, j));
				nulls[k++] = is_null;

				/* planid */
				sprintf(planid, "%lu", pgqp->qpd.planId[j]);
				values[k++] = CStringGetTextDatum(planid);

				/* query plan in text */
				values[k] = CStringGetTextDatum(get_query_plan(&pgqp->qpd, PRINT_PLAN, &is_null, j));
				nulls[k++] = is_null;

				/* query plan in json */
				values[k] = CStringGetTextDatum(get_query_plan(&pgqp->qpd, PRINT_PLAN_JSON, &is_null, j));
				nulls[k++] = is_null;

				tuplestore_putvalues(tupstore, tupdesc, values, nulls);

			}					/* end for loop */

	next:
			/* Release the exclusive lock */
			LWLockRelease(pgqp->lock);

		}						/* end if */
	}							/* end for loop */

	if (!_exist)
		elog(ERROR, "The process (pid=%d) does not exist.", pid);

	if (!is_enabled)
		elog(ERROR, "The showing query plan feature is disabled in the process (pid=%d).", pid);

	if (!has_data)
		elog(INFO, "The process (pid=%d) is in idle state.", pid);
	tuplestore_donestoring(tupstore);

	return (Datum) 0;
}
