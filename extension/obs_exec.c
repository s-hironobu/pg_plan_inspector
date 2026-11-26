/*-------------------------------------------------------------------------
 * obs_exec.c
 *	  Observation, executor side.
 *
 * For every observed query:
 *   - Per-node row counting is switched on (INSTRUMENT_ROWS, no timing).
 *   - The ExecProcNodeReal of each node is wrapped. This tells us in how many
 *     of its loops the node ran to exhaustion (returned an empty slot). A node
 *     that did not is "censored": its actual row count is only a lower bound
 *     (LIMIT, inner sides of semi-joins, merge joins, cursors, cancels, ...).
 *     Parallel workers stash their counts in an instrumentation field. This
 *     field is unused without timing, and it travels back to the leader.
 *   - At ExecutorEnd, the plan is logged. The log has the JSON of
 *     EXPLAIN (ANALYZE, TIMING OFF) from the core EXPLAIN code. It also has
 *     the per-node JSON, which joins the planning-time descriptors
 *     (obs_plan.c) with the run-time counters.
 *   - A query canceled by statement_timeout or a cancel request is logged too
 *     (status "canceled"), with the counters reached so far. This path runs
 *     inside PG_CATCH and does no catalog access.
 *
 * (The live view of a running query, pg_query_plan(), is a separate
 * extension: https://github.com/s-hironobu/pg_query_plan)
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h"
#include "access/xact.h"
#include "utils/lsyscache.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/params.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "pgpi.h"

extern const char *pgpi_node_type_name(Plan *plan);

/* marker for done-counts that parallel workers stash in instr->startup */
#define STASH_BASE	INT64CONST(0x7047500000000000)
#define STASH_LIMIT INT64CONST(0x0000010000000000)

typedef struct ObsQuery
{
	QueryDesc  *qd;
	EState	   *estate;
	int			level;
	SubTransactionId subid;
	TimestampTz start;
	int			maxid;
	ExecProcNodeMtd *orig;
	double	   *done;
	double	   *last_done_loop;
	bool	   *unknown;
	bool	   *check_after;	/* Gather: wrap again the lost wrappers after the first call */
	int		   *active;			/* calls in progress (seen by the log of a canceled query) */
	bool	   *visited;		/* scratch for tree walks */
	List	   *frags;			/* static per-node fragments, or NIL */
	char	   *meta;
	int64		planid;
	double		planning_ms;
	bool		log;
	bool		worker;
	bool		stash_ok;		/* worker: may stash counts (no timer) */
	char	   *dbname;
	char	   *username;
	char	   *label;
	Oid			userid;
} ObsQuery;

static List *obs_stack = NIL;	/* ObsQuery *, innermost last */
static int	exec_nesting = 0;
static MemoryContext cancel_cxt = NULL;

static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static char *cached_dbname = NULL;
static Oid	cached_userid = InvalidOid;
static char *cached_username = NULL;

static TupleTableSlot *pgpi_exec_wrapper(PlanState *node);

/* ---------- query stack ---------- */

static inline ObsQuery *
find_query(EState *estate)
{
	/* the innermost query is the usual answer */
	for (int i = list_length(obs_stack) - 1; i >= 0; i--)
	{
		ObsQuery   *q = (ObsQuery *) list_nth(obs_stack, i);

		if (q->estate == estate)
			return q;
	}
	return NULL;
}

static void
remove_query(ObsQuery *q)
{
	obs_stack = list_delete_ptr(obs_stack, q);
}

static void
obs_xact_callback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
	{
		list_free(obs_stack);
		obs_stack = NIL;
		exec_nesting = 0;
	}
}

static void
obs_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
					 SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		List	   *keep = NIL;
		ListCell   *lc;
		MemoryContext old;

		if (obs_stack == NIL)
			return;
		/* the list must outlive the memory contexts of the (sub)transaction */
		old = MemoryContextSwitchTo(TopMemoryContext);
		foreach(lc, obs_stack)
		{
			ObsQuery   *q = (ObsQuery *) lfirst(lc);

			if (q->subid < mySubid)
				keep = lappend(keep, q);
		}
		MemoryContextSwitchTo(old);
		list_free(obs_stack);
		obs_stack = keep;
	}
}

/* ---------- wrapping ---------- */

static bool
max_id_walker(PlanState *ps, int *maxid)
{
	if (ps->plan->plan_node_id > *maxid)
		*maxid = ps->plan->plan_node_id;
	return planstate_tree_walker(ps, max_id_walker, maxid);
}

static bool
wrap_walker(PlanState *ps, ObsQuery *q)
{
	int			id = ps->plan->plan_node_id;

	if (id >= 0 && id <= q->maxid && ps->ExecProcNodeReal != NULL &&
		ps->ExecProcNodeReal != pgpi_exec_wrapper)
	{
		q->orig[id] = ps->ExecProcNodeReal;
		ps->ExecProcNodeReal = pgpi_exec_wrapper;
		if (!q->worker && (IsA(ps, GatherState) || IsA(ps, GatherMergeState)))
			q->check_after[id] = true;
	}
	return planstate_tree_walker(ps, wrap_walker, q);
}

/*
 * Parallel hash join resets its node function when the parallel context is
 * initialized. This happens after ExecutorStart. Wrap such nodes again.
 */
static bool
rewrap_walker(PlanState *ps, ObsQuery *q)
{
	int			id = ps->plan->plan_node_id;

	if (id >= 0 && id <= q->maxid && ps->ExecProcNodeReal != NULL &&
		ps->ExecProcNodeReal != pgpi_exec_wrapper)
	{
		if (ps->instrument && (ps->instrument->running || ps->instrument->nloops > 0))
			q->unknown[id] = true;	/* already ran unobserved */
		q->orig[id] = ps->ExecProcNodeReal;
		ps->ExecProcNodeReal = pgpi_exec_wrapper;
	}
	return planstate_tree_walker(ps, rewrap_walker, q);
}

static TupleTableSlot *
pgpi_exec_wrapper(PlanState *node)
{
	ObsQuery   *q = find_query(node->state);
	int			id = node->plan->plan_node_id;
	TupleTableSlot *slot;

	if (unlikely(q == NULL || id < 0 || id > q->maxid || q->orig[id] == NULL))
		elog(ERROR, "pgpi: lost track of plan node %d", id);

	q->active[id]++;			/* not undone on error, so that a cancel sees it */
	slot = q->orig[id] (node);
	q->active[id]--;

	if (TupIsNull(slot))
	{
		if (node->instrument)
		{
			double		loop = node->instrument->nloops;

			if (q->last_done_loop[id] != loop)
			{
				q->last_done_loop[id] = loop;
				q->done[id] += 1;
			}
		}
		else
			q->done[id] += 1;
	}

	if (unlikely(q->check_after[id]))
	{
		q->check_after[id] = false;
		(void) planstate_tree_walker(node, rewrap_walker, q);
	}
	return slot;
}

/* ---------- static info from the planner ---------- */

static void
load_static_info(ObsQuery *q, PlannedStmt *pstmt)
{
	ListCell   *lc;

	foreach(lc, pstmt->extension_state)
	{
		DefElem    *de = (DefElem *) lfirst(lc);
		List	   *l;

		if (strcmp(de->defname, PGPI_PSTMT_KEY) != 0 || !IsA(de->arg, List))
			continue;
		l = (List *) de->arg;
		if (list_length(l) < 4)
			continue;
		q->meta = strVal(linitial(l));
		q->frags = (List *) lsecond(l);
		q->planid = strtoi64(strVal(lthird(l)), NULL, 10);
		q->planning_ms = strtod(strVal(lfourth(l)), NULL);
		return;
	}
}

/* ---------- per-node JSON ---------- */

typedef enum NodesMode
{
	NODES_FINAL,				/* ExecutorEnd: loops ended, workers merged */
	NODES_CANCEL				/* canceled: counters so far, all censored */
} NodesMode;

typedef struct NodesCtx
{
	ObsQuery   *q;
	NodesMode	mode;
	StringInfo	buf;
	bool		first;
} NodesCtx;

static bool
is_multiexec(PlanState *ps)
{
	return IsA(ps, HashState) || IsA(ps, BitmapIndexScanState) ||
		IsA(ps, BitmapAndState) || IsA(ps, BitmapOrState);
}

/* May the child stop early although its parent ran to completion? */
static bool
structural(PlanState *parent, const char *role)
{
	if (parent == NULL)
		return false;
	switch (nodeTag(parent->plan))
	{
		case T_NestLoop:
			{
				Join	   *j = (Join *) parent->plan;

				return strcmp(role, "inner") == 0 &&
					(j->jointype == JOIN_SEMI || j->jointype == JOIN_ANTI ||
					 j->inner_unique);
			}
		case T_MergeJoin:
		case T_Limit:
			return true;
		case T_HashJoin:
			return strcmp(role, "outer") == 0;
		default:
			return false;
	}
}

static void emit_nodes(NodesCtx *nc, PlanState *ps, PlanState *parent,
					   const char *role, bool parent_censored);

static bool
inner_unique_child(PlanState *parent, const char *role)
{
	return parent != NULL && strcmp(role, "inner") == 0 &&
		(IsA(parent->plan, NestLoop) || IsA(parent->plan, MergeJoin) ||
		 IsA(parent->plan, HashJoin)) &&
		((Join *) parent->plan)->inner_unique;
}

static void
emit_children(NodesCtx *nc, PlanState *ps, bool censored)
{
	ListCell   *lc;

	foreach(lc, ps->initPlan)
		emit_nodes(nc, ((SubPlanState *) lfirst(lc))->planstate, ps, "initplan", false);
	if (outerPlanState(ps))
		emit_nodes(nc, outerPlanState(ps), ps, "outer", censored);
	if (innerPlanState(ps))
		emit_nodes(nc, innerPlanState(ps), ps, "inner", censored);
	switch (nodeTag(ps))
	{
		case T_AppendState:
			for (int i = 0; i < ((AppendState *) ps)->as_nplans; i++)
				emit_nodes(nc, ((AppendState *) ps)->appendplans[i], ps, "member", censored);
			break;
		case T_MergeAppendState:
			for (int i = 0; i < ((MergeAppendState *) ps)->ms_nplans; i++)
				emit_nodes(nc, ((MergeAppendState *) ps)->mergeplans[i], ps, "member", censored);
			break;
		case T_BitmapAndState:
			for (int i = 0; i < ((BitmapAndState *) ps)->nplans; i++)
				emit_nodes(nc, ((BitmapAndState *) ps)->bitmapplans[i], ps, "member", censored);
			break;
		case T_BitmapOrState:
			for (int i = 0; i < ((BitmapOrState *) ps)->nplans; i++)
				emit_nodes(nc, ((BitmapOrState *) ps)->bitmapplans[i], ps, "member", censored);
			break;
		case T_SubqueryScanState:
			emit_nodes(nc, ((SubqueryScanState *) ps)->subplan, ps, "subquery", censored);
			break;
		case T_CustomScanState:
			foreach(lc, ((CustomScanState *) ps)->custom_ps)
				emit_nodes(nc, (PlanState *) lfirst(lc), ps, "member", censored);
			break;
		default:
			break;
	}
	foreach(lc, ps->subPlan)
		emit_nodes(nc, ((SubPlanState *) lfirst(lc))->planstate, ps, "subplan", false);
}

static void
emit_nodes(NodesCtx *nc, PlanState *ps, PlanState *parent, const char *role,
		   bool parent_censored)
{
	ObsQuery   *q = nc->q;
	StringInfo	buf = nc->buf;
	int			id;
	NodeInstrumentation *in;
	double		ntuples = 0,
				nloops = 0,
				wtuples = 0,
				wloops = 0,
				wdone = 0;
	bool		wknown = true;
	bool		known;
	double		done;
	bool		censored = false;
	const char *frag = NULL;

	if (ps == NULL)
		return;
	id = ps->plan->plan_node_id;
	if (id < 0 || id > q->maxid || q->visited[id])
		return;
	q->visited[id] = true;

	if (q->frags != NIL && id < list_length(q->frags))
		frag = strVal(list_nth(q->frags, id));

	appendStringInfoString(buf, nc->first ? "{" : ",{");
	nc->first = false;
	if (frag && frag[0])
		appendStringInfoString(buf, frag);
	else
	{
		appendStringInfo(buf, "\"id\":%d,\"type\":\"%s\",\"plan_rows\":",
						 id, pgpi_node_type_name(ps->plan));
		pgpi_append_double(buf, ps->plan->plan_rows);
	}
	appendStringInfo(buf, ",\"parent\":%d,\"role\":\"%s\"",
					 parent ? parent->plan->plan_node_id : -1, role);

	in = ps->instrument;
	if (in)
	{
		if (nc->mode == NODES_FINAL)
		{
			ntuples = in->ntuples;
			nloops = in->nloops;
		}
		else
		{
			bool		cur = in->running || q->active[id] > 0;

			ntuples = in->ntuples + (in->running ? in->tuplecount : 0);
			nloops = in->nloops + (cur ? 1 : 0);
		}
	}
	if (nc->mode == NODES_FINAL && ps->worker_instrument)
	{
		WorkerNodeInstrumentation *wi = ps->worker_instrument;

		for (int i = 0; i < wi->num_workers; i++)
		{
			NodeInstrumentation *w = &wi->instrument[i];
			int64		t = w->startup.ticks;

			wtuples += w->ntuples;
			wloops += w->nloops;
			if (w->nloops == 0)
				continue;
			if (t >= STASH_BASE && t < STASH_BASE + STASH_LIMIT)
				wdone += (double) (t - STASH_BASE);
			else
				wknown = false;
		}
	}

	if (is_multiexec(ps))
	{
		known = true;
		done = nloops;
	}
	else
	{
		known = q->orig[id] != NULL && !q->unknown[id] &&
			ps->ExecProcNodeReal == pgpi_exec_wrapper &&
			(wloops == 0 || wknown) && !(in && in->async_mode);
		done = q->done[id] + wdone;
	}

	appendStringInfoString(buf, ",\"ntuples\":");
	pgpi_append_double(buf, ntuples);
	appendStringInfoString(buf, ",\"nloops\":");
	pgpi_append_double(buf, nloops);
	appendStringInfoString(buf, ",\"done\":");
	if (known)
		pgpi_append_double(buf, done);
	else
		appendStringInfoString(buf, "null");
	if (wloops > 0)
	{
		appendStringInfoString(buf, ",\"worker_ntuples\":");
		pgpi_append_double(buf, wtuples);
		appendStringInfoString(buf, ",\"worker_loops\":");
		pgpi_append_double(buf, wloops);
	}
	if (in && (in->nfiltered1 > 0 || in->nfiltered2 > 0))
	{
		appendStringInfoString(buf, ",\"nfiltered1\":");
		pgpi_append_double(buf, in->nfiltered1);
		appendStringInfoString(buf, ",\"nfiltered2\":");
		pgpi_append_double(buf, in->nfiltered2);
	}
	if (IsA(ps, GatherState))
		appendStringInfo(buf, ",\"workers_launched\":%d",
						 ((GatherState *) ps)->nworkers_launched);
	else if (IsA(ps, GatherMergeState))
		appendStringInfo(buf, ",\"workers_launched\":%d",
						 ((GatherMergeState *) ps)->nworkers_launched);

	if (nc->mode == NODES_FINAL)
	{
		if (nloops == 0)
			censored = false;
		else if (known)
			censored = done < nloops;
		else
			censored = parent_censored || structural(parent, role);
		appendStringInfo(buf, ",\"censored\":%s,\"censor_basis\":\"%s\"",
						 censored ? "true" : "false",
						 known ? "exact" : "inferred");
	}
	else
	{
		bool		act = q->active[id] > 0;

		/* worker counters never came back: anything parallel is a guess */
		bool		exact = known && !ps->plan->parallel_aware &&
			!IsA(ps, GatherState) && !IsA(ps, GatherMergeState);

		appendStringInfo(buf, ",\"running\":%s,\"active\":%s",
						 (in && in->running) ? "true" : "false",
						 act ? "true" : "false");
		censored = nloops > 0 && (act || !exact || done < nloops);
		appendStringInfo(buf, ",\"censored\":%s,\"censor_basis\":\"%s\"",
						 censored ? "true" : "false",
						 exact ? "exact" : "inferred");
	}
	if (inner_unique_child(parent, role))
		appendStringInfoString(buf, ",\"inner_unique_child\":true");
	appendStringInfoChar(buf, '}');

	emit_children(nc, ps, censored);
}

static char *
build_nodes_json(ObsQuery *q, NodesMode mode)
{
	StringInfoData buf;
	NodesCtx	nc;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '[');
	memset(q->visited, 0, sizeof(bool) * (q->maxid + 1));
	nc.q = q;
	nc.mode = mode;
	nc.buf = &buf;
	nc.first = true;
	emit_nodes(&nc, q->qd->planstate, NULL, "top", false);
	appendStringInfoChar(&buf, ']');
	return buf.data;
}

static bool
endloop_walker(PlanState *ps, void *ctx)
{
	if (ps->instrument)
		InstrEndLoop(ps->instrument);
	return planstate_tree_walker(ps, endloop_walker, ctx);
}

/* ---------- EXPLAIN ---------- */

static char *
build_explain(QueryDesc *qd, bool analyze)
{
	ExplainState *es = NewExplainState();

	es->analyze = analyze && qd->instrument_options != 0;
	es->timing = es->analyze && (qd->instrument_options & INSTRUMENT_TIMER) != 0;
	es->buffers = es->analyze && (qd->instrument_options & INSTRUMENT_BUFFERS) != 0;
	es->wal = es->analyze && (qd->instrument_options & INSTRUMENT_WAL) != 0;
	es->summary = false;
	es->settings = true;
	es->format = EXPLAIN_FORMAT_JSON;

	ExplainBeginOutput(es);
	ExplainPrintPlan(es, qd);
	if (es->analyze)
		ExplainPrintTriggers(es, qd);
	ExplainEndOutput(es);

	if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
		es->str->data[--es->str->len] = '\0';
	if (es->str->len >= 2)
	{
		es->str->data[0] = '{';
		es->str->data[es->str->len - 1] = '}';
	}
	return es->str->data;
}

/* ---------- logging ---------- */

static void
emit_record(ObsQuery *q, int status, double exec_ms, const char *explain,
			const char *nodes, const char *params)
{
	PgpiPlanRecHdr hdr;
	const char *fields[PGPI_PF_NFIELDS];

	memset(&hdr, 0, sizeof(hdr));
	hdr.start_time = q->start;
	hdr.end_time = GetCurrentTimestamp();
	hdr.planning_ms = q->planning_ms;
	hdr.execution_ms = exec_ms;
	hdr.queryid = q->qd->plannedstmt->queryId;
	hdr.planid = q->planid;
	hdr.pid = MyProcPid;
	hdr.nested_level = q->level;
	hdr.status = status;
	hdr.encoding = GetDatabaseEncoding();
	hdr.dbid = MyDatabaseId;
	hdr.userid = q->userid;

	fields[PGPI_PF_DATABASE] = q->dbname;
	fields[PGPI_PF_USER] = q->username;
	fields[PGPI_PF_LABEL] = q->label;
	fields[PGPI_PF_QUERY] = q->qd->sourceText;
	fields[PGPI_PF_PARAMS] = params;
	fields[PGPI_PF_EXPLAIN] = explain;
	fields[PGPI_PF_NODES] = nodes;
	fields[PGPI_PF_META] = q->meta;
	(void) pgpi_ring_put(PGPI_REC_PLAN, &hdr, sizeof(hdr), fields, PGPI_PF_NFIELDS);
}

static void
log_finished_query(ObsQuery *q)
{
	QueryDesc  *qd = q->qd;
	double		exec_ms = 0;
	int			status = PGPI_ST_COMPLETE;
	PlanState  *top = qd->planstate;
	char	   *explain = NULL;
	char	   *nodes;
	char	   *params = NULL;
	int			topid = top->plan->plan_node_id;

	if (qd->query_instr)
		exec_ms = INSTR_TIME_GET_MILLISEC(qd->query_instr->total);
	if (exec_ms < pgpi_log_min_duration)
		return;

	/* did the top node run to completion? */
	if (top->instrument)
	{
		double		loops = top->instrument->nloops +
			(top->instrument->running ? 1 : 0);

		if (q->orig[topid] != NULL && q->done[topid] < loops)
			status = PGPI_ST_PARTIAL;
	}
	/* collect the worker counters of a query that was not run to the end */
	if (status == PGPI_ST_PARTIAL)
		ExecShutdownNode(top);

	if (pgpi_log_explain)
		explain = build_explain(qd, true);
	(void) endloop_walker(top, NULL);
	nodes = build_nodes_json(q, NODES_FINAL);
	if (pgpi_log_parameters && qd->params && qd->params->numParams > 0)
		params = BuildParamLogString(qd->params, NULL, -1);

	emit_record(q, status, exec_ms, explain, nodes, params);
}

/*
 * A query canceled by statement_timeout or a cancel request: record what was
 * reached. We are inside PG_CATCH, before the transaction abort. Do no catalog
 * access, only string work on data that we already have.
 */
static void
log_canceled_query(ObsQuery *q)
{
	MemoryContext old;
	ErrorData  *edata;

	if (cancel_cxt == NULL)
		cancel_cxt = AllocSetContextCreate(TopMemoryContext, "pgpi cancel",
										   ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(cancel_cxt);
	edata = CopyErrorData();
	if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED)
	{
		double		exec_ms = (double) (GetCurrentTimestamp() - q->start) / 1000.0;
		char	   *nodes = build_nodes_json(q, NODES_CANCEL);

		emit_record(q, PGPI_ST_CANCELED, exec_ms, NULL, nodes, NULL);
	}
	MemoryContextSwitchTo(old);
	MemoryContextReset(cancel_cxt);
}

/* ---------- executor hooks ---------- */

static bool
observe_this(QueryDesc *qd, int eflags)
{
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return false;
	if (!pgpi_obs_enabled())
		return false;
	if (exec_nesting > 0 && !pgpi_log_nested)
		return false;
	return true;
}

static ObsQuery *
new_query(QueryDesc *qd, bool worker)
{
	MemoryContext old = MemoryContextSwitchTo(qd->estate->es_query_cxt);
	ObsQuery   *q = palloc0(sizeof(ObsQuery));
	int			maxid = -1;

	(void) max_id_walker(qd->planstate, &maxid);
	foreach_node(PlanState, sp, qd->estate->es_subplanstates)
		if (sp)
			(void) max_id_walker(sp, &maxid);
	q->qd = qd;
	q->estate = qd->estate;
	q->level = exec_nesting;
	q->subid = GetCurrentSubTransactionId();
	q->start = GetCurrentTimestamp();
	q->maxid = maxid;
	q->orig = palloc0(sizeof(ExecProcNodeMtd) * (maxid + 1));
	q->done = palloc0(sizeof(double) * (maxid + 1));
	q->last_done_loop = palloc(sizeof(double) * (maxid + 1));
	for (int i = 0; i <= maxid; i++)
		q->last_done_loop[i] = -1;
	q->unknown = palloc0(sizeof(bool) * (maxid + 1));
	q->check_after = palloc0(sizeof(bool) * (maxid + 1));
	q->active = palloc0(sizeof(int) * (maxid + 1));
	q->visited = palloc0(sizeof(bool) * (maxid + 1));
	q->worker = worker;
	MemoryContextSwitchTo(old);
	return q;
}

static void
pgpi_ExecutorStart(QueryDesc *qd, int eflags)
{
	bool		observe = observe_this(qd, eflags);
	bool		worker = IsParallelWorker() &&
		(qd->instrument_options & INSTRUMENT_ROWS) != 0;

	if (observe)
	{
		qd->instrument_options |= INSTRUMENT_ROWS;
		qd->query_instr_options |= INSTRUMENT_TIMER;
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(qd, eflags);
	else
		standard_ExecutorStart(qd, eflags);

	if ((!observe && !worker) || qd->planstate == NULL)
		return;

	{
		ObsQuery   *q = new_query(qd, !observe);
		MemoryContext old = MemoryContextSwitchTo(qd->estate->es_query_cxt);

		if (observe)
		{
			Oid			userid = GetUserId();

			load_static_info(q, qd->plannedstmt);
			if (cached_dbname == NULL)
				cached_dbname = MemoryContextStrdup(TopMemoryContext,
													get_database_name(MyDatabaseId));
			if (cached_userid != userid || cached_username == NULL)
			{
				char	   *u = GetUserNameFromId(userid, true);

				cached_username = MemoryContextStrdup(TopMemoryContext, u ? u : "?");
				cached_userid = userid;
			}
			q->dbname = cached_dbname;
			q->username = cached_username;
			q->userid = userid;
			q->label = (pgpi_label && pgpi_label[0]) ? pstrdup(pgpi_label) : NULL;
			q->log = pgpi_log_plans && pgpi_obs_sampled();
		}
		else
			q->stash_ok = (qd->instrument_options & INSTRUMENT_TIMER) == 0;

		(void) wrap_walker(qd->planstate, q);
		foreach_node(PlanState, sp, qd->estate->es_subplanstates)
			if (sp)
				(void) wrap_walker(sp, q);
		MemoryContextSwitchTo(old);

		old = MemoryContextSwitchTo(TopMemoryContext);
		obs_stack = lappend(obs_stack, q);
		MemoryContextSwitchTo(old);
	}
}

static void
pgpi_ExecutorRun(QueryDesc *qd, ScanDirection direction, uint64 count)
{
	ObsQuery   *q = find_query(qd->estate);

	/* parallel workers: the initialization after ExecutorStart may reset nodes */
	if (q && q->worker)
		(void) rewrap_walker(qd->planstate, q);

	exec_nesting++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(qd, direction, count);
		else
			standard_ExecutorRun(qd, direction, count);
	}
	PG_CATCH();
	{
		exec_nesting--;
		if (q && q->log && pgpi_log_canceled)
			log_canceled_query(q);
		PG_RE_THROW();
	}
	PG_END_TRY();
	exec_nesting--;
}

static bool
stash_walker(PlanState *ps, ObsQuery *q)
{
	int			id = ps->plan->plan_node_id;

	if (ps->instrument && !ps->instrument->instr.need_timer &&
		id >= 0 && id <= q->maxid && q->orig[id] != NULL && !q->unknown[id] &&
		ps->ExecProcNodeReal == pgpi_exec_wrapper)
		ps->instrument->startup.ticks = STASH_BASE + (int64) q->done[id];
	return planstate_tree_walker(ps, stash_walker, q);
}

static void
pgpi_ExecutorFinish(QueryDesc *qd)
{
	ObsQuery   *q;

	exec_nesting++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(qd);
		else
			standard_ExecutorFinish(qd);
	}
	PG_FINALLY();
	{
		exec_nesting--;
	}
	PG_END_TRY();

	/* worker: hand the done-counts to the leader before they are reported */
	q = find_query(qd->estate);
	if (q && q->worker && q->stash_ok)
		(void) stash_walker(qd->planstate, q);
}

static void
pgpi_ExecutorEnd(QueryDesc *qd)
{
	ObsQuery   *q = find_query(qd->estate);

	if (q)
	{
		if (q->log && pgpi_log_plans && qd->planstate)
		{
			MemoryContext old = MemoryContextSwitchTo(qd->estate->es_query_cxt);

			PG_TRY();
			{
				log_finished_query(q);
			}
			PG_CATCH();
			{
				ErrorData  *edata;

				MemoryContextSwitchTo(old);
				edata = CopyErrorData();
				if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED)
				{
					remove_query(q);
					PG_RE_THROW();
				}
				FlushErrorState();
				ereport(WARNING,
						(errmsg("pgpi: could not log the plan: %s", edata->message)));
				FreeErrorData(edata);
			}
			PG_END_TRY();
			MemoryContextSwitchTo(old);
		}
		remove_query(q);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(qd);
	else
		standard_ExecutorEnd(qd);
}

/* ---------- init ---------- */

void
pgpi_obs_exec_init(void)
{
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgpi_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pgpi_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pgpi_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgpi_ExecutorEnd;
	RegisterXactCallback(obs_xact_callback, NULL);
	RegisterSubXactCallback(obs_subxact_callback, NULL);
}
