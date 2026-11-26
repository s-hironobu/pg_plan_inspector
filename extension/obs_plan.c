/*-------------------------------------------------------------------------
 * obs_plan.c
 *	  Observation, planning side.
 *
 * When planning finishes, walk the final plan tree and describe every node:
 *   - which relations it covers (final range-table indexes and relation
 *     identifiers of pgpi)
 *   - whether a nested loop parameterizes it, and by which relations
 *   - for nodes whose row count is a cardinality estimate (scans, joins,
 *     appends): the descriptor of the question, in exactly the format of an
 *     intervention request. If a request was made during this planning cycle,
 *     the matching request and answer are attached too.
 *
 * The result is attached to the PlannedStmt (extension_state). So it lives as
 * long as the plan, including in the plan cache. It is combined with the
 * run-time counters when the query ends (obs_exec.c).
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "utils/lsyscache.h"
#include "common/hashfn.h"
#include "common/pg_prng.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/extendplan.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "pgpi.h"

/* final range-table entry -> planning-time view */
typedef struct RtInfo
{
	RangeTblEntry *rte;
	PlannerInfo *root;			/* NULL if unknown */
	Index		local;			/* RTI within root */
	const char *plan_name;
	const char *ident;			/* NULL for RTE_JOIN / NULL entries */
} RtInfo;

typedef struct NLParam
{
	int			paramno;
	Relids		relids;			/* final RTIs */
} NLParam;

typedef struct ObsCtx
{
	PlannedStmt *pstmt;
	PlannerGlobal *glob;
	PgpiPlanState *ps;
	int			nrt;
	RtInfo	   *rt;				/* indexed by final RTI (1-based) */
	int			maxid;
	char	  **frag;			/* static JSON fragment per plan_node_id */
	List	   *matched;		/* PgpiPlanReq * matched to a node */
	int		   *matched_node;	/* parallel to ps->reqs: node id or -1 */
	StringInfoData shape;
} ObsCtx;

/* ---------- setup / teardown ---------- */

/*
 * Is the current client statement sampled for logging? This is decided once per
 * statement (identified by its start timestamp), so that its planning, its
 * execution and its nested statements agree.
 */
bool
pgpi_obs_sampled(void)
{
	static TimestampTz decided_for = 0;
	static bool sampled = true;
	TimestampTz ts;

	if (pgpi_log_sample_rate >= 1.0)
		return true;
	if (pgpi_log_sample_rate <= 0.0)
		return false;
	ts = GetCurrentStatementStartTimestamp();
	if (ts != decided_for)
	{
		decided_for = ts;
		sampled = pg_prng_double(&pg_global_prng_state) < pgpi_log_sample_rate;
	}
	return sampled;
}

/* Executor side: instrument and track this statement? */
bool
pgpi_obs_enabled(void)
{
	if (!pgpi_shm_available() || MyBackendType != B_BACKEND || IsParallelWorker())
		return false;
	return pgpi_log_plans && pgpi_obs_sampled();
}

/* Planner side: describe the plan? Only for the statements that will be logged. */
static bool
obs_static_enabled(void)
{
	return pgpi_obs_enabled();
}

void
pgpi_obs_planner_setup(PlannerGlobal *glob)
{
	PgpiPlanState *ps;

	if (!obs_static_enabled() || pgpi_ext_id < 0)
		return;
	ps = palloc0(sizeof(PgpiPlanState));
	ps->cxt = CurrentMemoryContext;
	ps->start = GetCurrentTimestamp();
	SetPlannerGlobalExtensionState(glob, pgpi_ext_id, ps);
}

/* ---------- small helpers ---------- */

static const char *
rtekind_name(RTEKind k)
{
	switch (k)
	{
		case RTE_RELATION:
			return "relation";
		case RTE_SUBQUERY:
			return "subquery";
		case RTE_JOIN:
			return "join";
		case RTE_FUNCTION:
			return "function";
		case RTE_TABLEFUNC:
			return "tablefunc";
		case RTE_VALUES:
			return "values";
		case RTE_CTE:
			return "cte";
		case RTE_NAMEDTUPLESTORE:
			return "namedtuplestore";
		case RTE_RESULT:
			return "result";
		default:
			return "other";
	}
}

const char *
pgpi_join_type_name(JoinType jt)
{
	switch (jt)
	{
		case JOIN_INNER:
			return "Inner";
		case JOIN_LEFT:
			return "Left";
		case JOIN_FULL:
			return "Full";
		case JOIN_RIGHT:
			return "Right";
		case JOIN_SEMI:
			return "Semi";
		case JOIN_ANTI:
			return "Anti";
		case JOIN_RIGHT_SEMI:
			return "Right Semi";
		case JOIN_RIGHT_ANTI:
			return "Right Anti";
		default:
			return "?";
	}
}

/* EXPLAIN-compatible node type names */
const char *
pgpi_node_type_name(Plan *plan)
{
	switch (nodeTag(plan))
	{
		case T_Result:
			return "Result";
		case T_ProjectSet:
			return "ProjectSet";
		case T_ModifyTable:
			return "ModifyTable";
		case T_Append:
			return "Append";
		case T_MergeAppend:
			return "Merge Append";
		case T_RecursiveUnion:
			return "Recursive Union";
		case T_BitmapAnd:
			return "BitmapAnd";
		case T_BitmapOr:
			return "BitmapOr";
		case T_NestLoop:
			return "Nested Loop";
		case T_MergeJoin:
			return "Merge Join";
		case T_HashJoin:
			return "Hash Join";
		case T_SeqScan:
			return "Seq Scan";
		case T_SampleScan:
			return "Sample Scan";
		case T_Gather:
			return "Gather";
		case T_GatherMerge:
			return "Gather Merge";
		case T_IndexScan:
			return "Index Scan";
		case T_IndexOnlyScan:
			return "Index Only Scan";
		case T_BitmapIndexScan:
			return "Bitmap Index Scan";
		case T_BitmapHeapScan:
			return "Bitmap Heap Scan";
		case T_TidScan:
			return "Tid Scan";
		case T_TidRangeScan:
			return "Tid Range Scan";
		case T_SubqueryScan:
			return "Subquery Scan";
		case T_FunctionScan:
			return "Function Scan";
		case T_TableFuncScan:
			return "Table Function Scan";
		case T_ValuesScan:
			return "Values Scan";
		case T_CteScan:
			return "CTE Scan";
		case T_NamedTuplestoreScan:
			return "Named Tuplestore Scan";
		case T_WorkTableScan:
			return "WorkTable Scan";
		case T_ForeignScan:
			return "Foreign Scan";
		case T_CustomScan:
			return "Custom Scan";
		case T_Material:
			return "Materialize";
		case T_Memoize:
			return "Memoize";
		case T_Sort:
			return "Sort";
		case T_IncrementalSort:
			return "Incremental Sort";
		case T_Group:
			return "Group";
		case T_Agg:
			return "Aggregate";
		case T_WindowAgg:
			return "WindowAgg";
		case T_Unique:
			return "Unique";
		case T_SetOp:
			return "SetOp";
		case T_LockRows:
			return "LockRows";
		case T_Limit:
			return "Limit";
		case T_Hash:
			return "Hash";
		default:
			return "???";
	}
}

static bool
is_scan_node(Plan *plan)
{
	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapIndexScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_TidRangeScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_TableFuncScan:
		case T_ValuesScan:
		case T_CteScan:
		case T_NamedTuplestoreScan:
		case T_WorkTableScan:
		case T_ForeignScan:
		case T_CustomScan:
			return true;
		default:
			return false;
	}
}

static Relids
filter_join_relids(ObsCtx *cx, Relids relids)
{
	Relids		result = NULL;
	int			rti = -1;

	while ((rti = bms_next_member(relids, rti)) >= 0)
	{
		if (rti >= 1 && rti <= cx->nrt && cx->rt[rti].rte != NULL &&
			cx->rt[rti].rte->rtekind == RTE_JOIN)
			continue;
		result = bms_add_member(result, rti);
	}
	return result;
}

static void
append_relids_array(StringInfo buf, Relids relids)
{
	int			rti = -1;
	bool		first = true;

	appendStringInfoChar(buf, '[');
	while ((rti = bms_next_member(relids, rti)) >= 0)
	{
		appendStringInfo(buf, "%s%d", first ? "" : ",", rti);
		first = false;
	}
	appendStringInfoChar(buf, ']');
}

static void
append_ident_array(ObsCtx *cx, StringInfo buf, Relids relids)
{
	int			rti = -1;
	bool		first = true;

	appendStringInfoChar(buf, '[');
	while ((rti = bms_next_member(relids, rti)) >= 0)
	{
		const char *id = (rti >= 1 && rti <= cx->nrt) ? cx->rt[rti].ident : NULL;

		if (!first)
			appendStringInfoChar(buf, ',');
		first = false;
		pgpi_append_json_string(buf, id);
	}
	appendStringInfoChar(buf, ']');
}

/* ---------- range table: identifiers and planning-time roots ---------- */

/*
 * Identifiers in the form used by pg_plan_advice:
 *   alias#occurrence/partition_schema.partition@subquery
 * They are computed in the same way as pgpa_create_identifiers_for_planned_stmt().
 */
static void
build_rtinfo(ObsCtx *cx)
{
	PlannedStmt *pstmt = cx->pstmt;
	Index	   *top = palloc0(sizeof(Index) * (cx->nrt + 1));
	SubPlanRTInfo *rtinfo = NULL;
	SubPlanRTInfo *next = NULL;
	int			idx = 0;
	ListCell   *lc;

	for (int rti = 1; rti <= cx->nrt; rti++)
		top[rti] = rti;
	foreach(lc, pstmt->appendRelations)
	{
		AppendRelInfo *ai = lfirst_node(AppendRelInfo, lc);
		RangeTblEntry *prte = rt_fetch(ai->parent_relid, pstmt->rtable);

		if (prte->rtekind == RTE_RELATION)
			top[ai->child_relid] = top[ai->parent_relid];
	}

	if (pstmt->subrtinfos != NIL)
		next = linitial(pstmt->subrtinfos);

	for (int rti = 1; rti <= cx->nrt; rti++)
	{
		RtInfo	   *ri = &cx->rt[rti];
		RangeTblEntry *rte;
		Index		offset;
		int			occurrence = 1;
		const char *id;

		while (next != NULL && (Index) rti > next->rtoffset)
		{
			rtinfo = next;
			if (++idx >= list_length(pstmt->subrtinfos))
				next = NULL;
			else
				next = list_nth(pstmt->subrtinfos, idx);
		}

		rte = rt_fetch(rti, pstmt->rtable);
		ri->rte = rte;
		ri->plan_name = rtinfo ? rtinfo->plan_name : NULL;
		offset = rtinfo ? rtinfo->rtoffset : 0;
		ri->local = rti - offset;

		/* planning-time root of this part of the range table */
		ri->root = NULL;
		if (rtinfo == NULL || !rtinfo->dummy)
		{
			foreach(lc, cx->ps->roots)
			{
				PlannerInfo *root = (PlannerInfo *) lfirst(lc);

				if ((root->plan_name == NULL) != (ri->plan_name == NULL))
					continue;
				if (root->plan_name && strcmp(root->plan_name, ri->plan_name) != 0)
					continue;
				if (ri->local < 1 || ri->local > list_length(root->parse->rtable) ||
					ri->local >= (Index) root->simple_rel_array_size)
					continue;
				ri->root = root;
				break;
			}
		}

		if (rte == NULL || rte->rtekind == RTE_JOIN)
			continue;

		for (Index prior = offset + 1; prior < top[rti]; prior++)
		{
			RangeTblEntry *prte = rt_fetch(prior, pstmt->rtable);

			if (top[prior] != prior || prte->rtekind == RTE_JOIN)
				continue;
			if (strcmp(prte->eref->aliasname,
					   rt_fetch(top[rti], pstmt->rtable)->eref->aliasname) != 0)
				continue;
			occurrence++;
		}

		id = quote_identifier(rt_fetch(top[rti], pstmt->rtable)->eref->aliasname);
		if (occurrence > 1)
			id = psprintf("%s#%d", id, occurrence);
		if (top[rti] != (Index) rti && OidIsValid(rte->relid))
			id = psprintf("%s/%s.%s", id,
						  quote_identifier(get_namespace_name_or_temp(get_rel_namespace(rte->relid))),
						  quote_identifier(get_rel_name(rte->relid)));
		if (ri->plan_name)
			id = psprintf("%s@%s", id, quote_identifier(ri->plan_name));
		ri->ident = id;
	}
}

/* Map final relids to (root, local relids). The root is NULL if the mapping is not uniform. */
static PlannerInfo *
to_local(ObsCtx *cx, Relids final, Relids *local)
{
	PlannerInfo *root = NULL;
	int			rti = -1;

	*local = NULL;
	while ((rti = bms_next_member(final, rti)) >= 0)
	{
		RtInfo	   *ri;

		if (rti < 1 || rti > cx->nrt)
			return NULL;
		ri = &cx->rt[rti];
		if (ri->root == NULL || (root != NULL && ri->root != root))
			return NULL;
		root = ri->root;
		*local = bms_add_member(*local, ri->local);
	}
	return root;
}

/* ---------- nested-loop parameters ---------- */

static Relids plan_output_relids(ObsCtx *cx, Plan *plan, AttrNumber attno, int depth);

typedef struct VarWalk
{
	ObsCtx	   *cx;
	Plan	   *plan;
	int			depth;
	Relids		result;
} VarWalk;

static Relids
resolve_var(ObsCtx *cx, Plan *plan, Var *var, int depth)
{
	Plan	   *child;

	if (depth > 64)
		return NULL;
	if (var->varno == OUTER_VAR)
		child = outerPlan(plan);
	else if (var->varno == INNER_VAR)
		child = innerPlan(plan);
	else if (IS_SPECIAL_VARNO(var->varno))
		return NULL;
	else
		return filter_join_relids(cx, bms_make_singleton(var->varno));
	if (child == NULL)
		return NULL;
	return plan_output_relids(cx, child, var->varattno, depth + 1);
}

static bool
var_walker(Node *node, VarWalk *vw)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		vw->result = bms_add_members(vw->result,
									 resolve_var(vw->cx, vw->plan, (Var *) node,
												 vw->depth));
		return false;
	}
	return expression_tree_walker(node, var_walker, vw);
}

/* Which base relations does output column attno of plan come from? */
static Relids
plan_output_relids(ObsCtx *cx, Plan *plan, AttrNumber attno, int depth)
{
	TargetEntry *tle;
	VarWalk		vw;
	ListCell   *lc;
	Relids		elided = NULL;
	bool		found = false;

	/* a trivial subquery scan or single-child append was removed above */
	foreach(lc, cx->pstmt->elidedNodes)
	{
		ElidedNode *en = lfirst_node(ElidedNode, lc);

		if (en->plan_node_id == plan->plan_node_id)
		{
			elided = en->relids;
			found = true;
		}
	}
	if (found)
		return filter_join_relids(cx, elided);

	if (IsA(plan, Append))
		return filter_join_relids(cx, ((Append *) plan)->apprelids);
	if (IsA(plan, MergeAppend))
		return filter_join_relids(cx, ((MergeAppend *) plan)->apprelids);
	if (IsA(plan, ForeignScan) && ((Scan *) plan)->scanrelid == 0)
		return filter_join_relids(cx, ((ForeignScan *) plan)->fs_relids);
	if (IsA(plan, CustomScan) && ((Scan *) plan)->scanrelid == 0)
		return filter_join_relids(cx, ((CustomScan *) plan)->custom_relids);
	if (is_scan_node(plan) && ((Scan *) plan)->scanrelid > 0)
		return bms_make_singleton(((Scan *) plan)->scanrelid);

	tle = get_tle_by_resno(plan->targetlist, attno);
	if (tle == NULL)
		return NULL;
	vw.cx = cx;
	vw.plan = plan;
	vw.depth = depth;
	vw.result = NULL;
	(void) var_walker((Node *) tle->expr, &vw);
	return vw.result;
}

/* ---------- descriptors ---------- */

static RelOptInfo *
find_rel(PlannerInfo *root, Relids local)
{
	ListCell   *lc;

	if (bms_membership(local) == BMS_SINGLETON)
	{
		int			rti = bms_singleton_member(local);

		if (rti < root->simple_rel_array_size)
			return root->simple_rel_array[rti];
		return NULL;
	}
	foreach(lc, root->join_rel_list)
	{
		RelOptInfo *rel = (RelOptInfo *) lfirst(lc);

		if (bms_equal(pgpi_nonjoin_relids(root, rel->relids), local))
			return rel;
	}
	return NULL;
}

/* Clauses evaluated inside rel, plus those moved in by parameterization. */
static List *
descriptor_clauses(PlannerInfo *root, RelOptInfo *rel, Relids outer_local)
{
	List	   *clauses;
	ListCell   *lc;
	Relids		joinrelids;

	if (rel->reloptkind == RELOPT_BASEREL ||
		rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
		clauses = list_copy(rel->baserestrictinfo);
	else
		clauses = list_copy(pgpi_all_clauses(root, rel));

	if (bms_is_empty(outer_local))
		return clauses;

	joinrelids = bms_union(rel->relids, outer_local);
	foreach(lc, rel->joininfo)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		if (join_clause_is_movable_into(ri, rel->relids, joinrelids))
			clauses = list_append_unique_ptr(clauses, ri);
	}
	/*
	 * Equivalence-class join clauses. Not for child rels, because translating them
	 * needs the appendrel context that the planner had at that moment.
	 */
	if (!IS_OTHER_REL(rel))
		clauses = list_concat_unique_ptr(clauses,
										 generate_join_implied_equalities(root, joinrelids,
																		  outer_local,
																		  rel, NULL));
	return clauses;
}

static PgpiPlanReq *
match_request(ObsCtx *cx, PlannerInfo *root, PgpiPoint point, Relids local,
			  Relids outer_local, int node_id)
{
	PgpiPlanReq *best = NULL;
	int			best_score = -1;
	int			i = 0;
	ListCell   *lc;

	foreach(lc, cx->ps->reqs)
	{
		PgpiPlanReq *pr = (PgpiPlanReq *) lfirst(lc);
		int			score;

		i++;
		if (pr->root != root || pr->point != point ||
			!bms_equal(pr->relids, local) ||
			!bms_equal(pr->outer_relids, outer_local))
			continue;
		score = (pr->applied ? 4 : 0) + (pr->has_answer ? 2 : 0) +
			(pr->route == 'D' ? 1 : 0);
		if (score >= best_score)
		{
			best = pr;
			best_score = score;
		}
	}
	if (best)
	{
		/* every request for the same question refers to this node */
		i = 0;
		foreach(lc, cx->ps->reqs)
		{
			PgpiPlanReq *pr = (PgpiPlanReq *) lfirst(lc);

			if (pr->root == root && pr->point == point &&
				bms_equal(pr->relids, local) &&
				bms_equal(pr->outer_relids, outer_local))
				cx->matched_node[i] = node_id;
			i++;
		}
	}
	return best;
}

static void
append_relinfo(StringInfo buf, PlannerInfo *root, Plan *plan, Index local)
{
	RelOptInfo *rel;
	Oid			indexid = InvalidOid;
	ListCell   *lc;

	if (root == NULL || local >= (Index) root->simple_rel_array_size ||
		(rel = root->simple_rel_array[local]) == NULL ||
		rel->rtekind != RTE_RELATION)
		return;

	appendStringInfoString(buf, ",\"relinfo\":{\"pages\":");
	pgpi_append_double(buf, (double) rel->pages);
	appendStringInfoString(buf, ",\"tuples\":");
	pgpi_append_double(buf, rel->tuples);
	appendStringInfoString(buf, ",\"allvisfrac\":");
	pgpi_append_double(buf, rel->allvisfrac);
	appendStringInfoChar(buf, '}');

	if (IsA(plan, IndexScan))
		indexid = ((IndexScan *) plan)->indexid;
	else if (IsA(plan, IndexOnlyScan))
		indexid = ((IndexOnlyScan *) plan)->indexid;
	else if (IsA(plan, BitmapIndexScan))
		indexid = ((BitmapIndexScan *) plan)->indexid;
	if (!OidIsValid(indexid))
		return;
	foreach(lc, rel->indexlist)
	{
		IndexOptInfo *ix = (IndexOptInfo *) lfirst(lc);

		if (ix->indexoid != indexid)
			continue;
		appendStringInfo(buf, ",\"index\":{\"oid\":%u,\"name\":", indexid);
		pgpi_append_json_string(buf, get_rel_name(indexid));
		appendStringInfoString(buf, ",\"pages\":");
		pgpi_append_double(buf, (double) ix->pages);
		appendStringInfoString(buf, ",\"tuples\":");
		pgpi_append_double(buf, ix->tuples);
		appendStringInfo(buf, ",\"tree_height\":%d,\"unique\":%s}",
						 ix->tree_height, ix->unique ? "true" : "false");
		break;
	}
}

/* ---------- the plan walk ---------- */

static void
emit_node(ObsCtx *cx, Plan *plan, Relids own, Relids param_outer)
{
	StringInfoData buf;
	int			id = plan->plan_node_id;
	bool		ce = false;
	PgpiPoint	point = PGPI_PT_BASE;
	JoinType	jointype = JOIN_INNER;
	int			nrels = bms_num_members(own);
	ListCell   *lc;

	if (id < 0 || id > cx->maxid)
		return;

	initStringInfo(&buf);
	appendStringInfo(&buf, "\"id\":%d,\"type\":\"%s\",\"relids\":", id,
					 pgpi_node_type_name(plan));
	append_relids_array(&buf, own);
	appendStringInfoString(&buf, ",\"ident\":");
	append_ident_array(cx, &buf, own);
	appendStringInfoString(&buf, ",\"plan_rows\":");
	pgpi_append_double(&buf, plan->plan_rows);
	appendStringInfoString(&buf, ",\"startup_cost\":");
	pgpi_append_double(&buf, plan->startup_cost);
	appendStringInfoString(&buf, ",\"total_cost\":");
	pgpi_append_double(&buf, plan->total_cost);
	appendStringInfo(&buf, ",\"width\":%d,\"parallel_aware\":%s,\"parallel_safe\":%s",
					 plan->plan_width, plan->parallel_aware ? "true" : "false",
					 plan->parallel_safe ? "true" : "false");

	switch (nodeTag(plan))
	{
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			jointype = ((Join *) plan)->jointype;
			appendStringInfo(&buf, ",\"join_type\":\"%s\",\"inner_unique\":%s",
							 pgpi_join_type_name(jointype),
							 ((Join *) plan)->inner_unique ? "true" : "false");
			if (IsA(plan, NestLoop))
				appendStringInfo(&buf, ",\"nest_params\":%d",
								 list_length(((NestLoop *) plan)->nestParams));
			ce = true;
			point = PGPI_PT_JOIN;
			break;
		case T_Gather:
			appendStringInfo(&buf, ",\"workers_planned\":%d",
							 ((Gather *) plan)->num_workers);
			break;
		case T_GatherMerge:
			appendStringInfo(&buf, ",\"workers_planned\":%d",
							 ((GatherMerge *) plan)->num_workers);
			break;
		case T_Append:
		case T_MergeAppend:
			ce = nrels > 0;
			point = nrels > 1 ? PGPI_PT_JOIN : PGPI_PT_BASE;
			break;
		case T_BitmapIndexScan:
			break;				/* rows are index-qual matches, not rel rows */
		case T_ForeignScan:
		case T_CustomScan:
			ce = nrels > 0;
			point = nrels > 1 ? PGPI_PT_JOIN : PGPI_PT_BASE;
			break;
		default:
			if (is_scan_node(plan) && ((Scan *) plan)->scanrelid > 0)
			{
				ce = true;
				point = PGPI_PT_BASE;
			}
			break;
	}

	if (!bms_is_empty(param_outer))
	{
		appendStringInfoString(&buf, ",\"param_outer\":");
		append_relids_array(&buf, param_outer);
		if (point == PGPI_PT_BASE)
			point = PGPI_PT_PARAM_BASE;
		else if (point == PGPI_PT_JOIN)
			point = PGPI_PT_PARAM_JOIN;
	}

	/* size inputs of the planner for base relation scans */
	if (is_scan_node(plan) && ((Scan *) plan)->scanrelid > 0)
	{
		Index		srti = ((Scan *) plan)->scanrelid;

		if (srti <= (Index) cx->nrt)
			append_relinfo(&buf, cx->rt[srti].root, plan, cx->rt[srti].local);
	}

	if (ce)
	{
		Relids		local;
		Relids		outer_local = NULL;
		PlannerInfo *root = to_local(cx, own, &local);
		PlannerInfo *oroot = bms_is_empty(param_outer) ? root :
			to_local(cx, param_outer, &outer_local);
		RelOptInfo *rel = NULL;

		if (root != NULL && oroot == root && !bms_is_empty(local))
			rel = find_rel(root, local);

		/*
		 * An Append over several relids that is not a join relation is a set
		 * operation (UNION ALL of subqueries). There is no cardinality question there.
		 */
		if (rel == NULL && nrels > 1 &&
			(IsA(plan, Append) || IsA(plan, MergeAppend)))
			ce = false;
	}
	if (ce)
	{
		Relids		local;
		Relids		outer_local = NULL;
		PlannerInfo *root = to_local(cx, own, &local);
		PlannerInfo *oroot = bms_is_empty(param_outer) ? root :
			to_local(cx, param_outer, &outer_local);
		RelOptInfo *rel = NULL;

		if (root != NULL && oroot == root && !bms_is_empty(local))
			rel = find_rel(root, local);
		appendStringInfo(&buf, ",\"point\":\"%s\"", pgpi_point_name(point));
		if (rel != NULL)
		{
			PgpiPlanReq *pr;

			appendStringInfoString(&buf, ",\"ce\":{");
			pgpi_append_descriptor(&buf, root, point, local, outer_local,
								   descriptor_clauses(root, rel, outer_local),
								   jointype);
			appendStringInfoString(&buf, ",\"tuples\":");
			pgpi_append_double(&buf, rel->tuples);
			appendStringInfoString(&buf, ",\"rel_rows\":");
			pgpi_append_double(&buf, rel->rows);
			appendStringInfoChar(&buf, '}');

			pr = match_request(cx, root, point, local, outer_local, id);
			if (pr)
			{
				appendStringInfoString(&buf, ",\"est\":{\"pg\":");
				pgpi_append_double(&buf, pr->pg_value);
				appendStringInfoString(&buf, ",\"answer\":");
				if (pr->has_answer)
					pgpi_append_double(&buf, pr->answer);
				else
					appendStringInfoString(&buf, "null");
				appendStringInfoString(&buf, ",\"source\":");
				pgpi_append_json_string(&buf, pr->source);
				appendStringInfo(&buf, ",\"applied\":%s,\"route\":\"%c\",\"req\":" INT64_FORMAT "}",
								 pr->applied ? "true" : "false", pr->route, pr->seq);
			}
		}
	}

	/* nodes removed by setrefs above this one */
	{
		bool		first = true;

		foreach(lc, cx->pstmt->elidedNodes)
		{
			ElidedNode *en = lfirst_node(ElidedNode, lc);

			if (en->plan_node_id != id)
				continue;
			appendStringInfoString(&buf, first ? ",\"elided\":[" : ",");
			first = false;
			appendStringInfo(&buf, "{\"type\":\"%s\",\"relids\":",
							 en->elided_type == T_SubqueryScan ? "Subquery Scan" :
							 en->elided_type == T_Append ? "Append" :
							 en->elided_type == T_MergeAppend ? "Merge Append" : "?");
			append_relids_array(&buf, filter_join_relids(cx, en->relids));
			appendStringInfoChar(&buf, '}');
		}
		if (!first)
			appendStringInfoChar(&buf, ']');
	}

	cx->frag[id] = buf.data;

	/* structural signature for the plan id */
	appendStringInfo(&cx->shape, "%d:%s:", id, pgpi_node_type_name(plan));
	{
		int			rti = -1;

		while ((rti = bms_next_member(own, rti)) >= 0)
			appendStringInfo(&cx->shape, "%s,",
							 cx->rt[rti].ident ? cx->rt[rti].ident : "?");
	}
	appendStringInfoChar(&cx->shape, ';');
}

static Relids walk_plan(ObsCtx *cx, Plan *plan, List *nlparams);

static Relids
walk_list(ObsCtx *cx, List *plans, List *nlparams)
{
	Relids		result = NULL;
	ListCell   *lc;

	foreach(lc, plans)
		result = bms_add_members(result, walk_plan(cx, (Plan *) lfirst(lc), nlparams));
	return result;
}

static Relids
walk_plan(ObsCtx *cx, Plan *plan, List *nlparams)
{
	Relids		own = NULL;
	Relids		view;
	Relids		param_outer = NULL;
	ListCell   *lc;

	if (plan == NULL)
		return NULL;
	check_stack_depth();

	switch (nodeTag(plan))
	{
		case T_NestLoop:
			{
				NestLoop   *nl = (NestLoop *) plan;
				List	   *inner_params = list_copy(nlparams);
				Relids		l;
				Relids		r;

				foreach(lc, nl->nestParams)
				{
					NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
					NLParam    *p = palloc(sizeof(NLParam));

					p->paramno = nlp->paramno;
					p->relids = resolve_var(cx, plan, nlp->paramval, 0);
					inner_params = lappend(inner_params, p);
				}
				l = walk_plan(cx, outerPlan(plan), nlparams);
				r = walk_plan(cx, innerPlan(plan), inner_params);
				own = bms_union(l, r);
			}
			break;
		case T_MergeJoin:
		case T_HashJoin:
			own = bms_union(walk_plan(cx, outerPlan(plan), nlparams),
							walk_plan(cx, innerPlan(plan), nlparams));
			break;
		case T_Append:
			(void) walk_list(cx, ((Append *) plan)->appendplans, nlparams);
			own = filter_join_relids(cx, ((Append *) plan)->apprelids);
			break;
		case T_MergeAppend:
			(void) walk_list(cx, ((MergeAppend *) plan)->mergeplans, nlparams);
			own = filter_join_relids(cx, ((MergeAppend *) plan)->apprelids);
			break;
		case T_BitmapAnd:
			own = walk_list(cx, ((BitmapAnd *) plan)->bitmapplans, nlparams);
			break;
		case T_BitmapOr:
			own = walk_list(cx, ((BitmapOr *) plan)->bitmapplans, nlparams);
			break;
		case T_SubqueryScan:
			(void) walk_plan(cx, ((SubqueryScan *) plan)->subplan, nlparams);
			own = bms_make_singleton(((Scan *) plan)->scanrelid);
			break;
		case T_CustomScan:
			(void) walk_list(cx, ((CustomScan *) plan)->custom_plans, nlparams);
			if (((Scan *) plan)->scanrelid > 0)
				own = bms_make_singleton(((Scan *) plan)->scanrelid);
			else
				own = filter_join_relids(cx, ((CustomScan *) plan)->custom_relids);
			break;
		case T_ForeignScan:
			(void) walk_plan(cx, outerPlan(plan), nlparams);
			if (((Scan *) plan)->scanrelid > 0)
				own = bms_make_singleton(((Scan *) plan)->scanrelid);
			else
				own = filter_join_relids(cx, ((ForeignScan *) plan)->fs_relids);
			break;
		case T_Result:
			{
				Relids		l = walk_plan(cx, outerPlan(plan), nlparams);

				own = ((Result *) plan)->relids ?
					filter_join_relids(cx, ((Result *) plan)->relids) : l;
			}
			break;
		default:
			if (is_scan_node(plan) && ((Scan *) plan)->scanrelid > 0)
			{
				own = bms_make_singleton(((Scan *) plan)->scanrelid);
				(void) walk_plan(cx, outerPlan(plan), nlparams);
			}
			else
			{
				own = walk_plan(cx, outerPlan(plan), nlparams);
				own = bms_add_members(own, walk_plan(cx, innerPlan(plan), nlparams));
			}
			break;
	}

	/* parameterized by an enclosing nested loop? */
	foreach(lc, nlparams)
	{
		NLParam    *p = (NLParam *) lfirst(lc);

		if (bms_is_member(p->paramno, plan->extParam))
			param_outer = bms_add_members(param_outer, p->relids);
	}

	emit_node(cx, plan, own, param_outer);

	/* what the parent sees: the relids of the outermost removed node, if any */
	view = own;
	foreach(lc, cx->pstmt->elidedNodes)
	{
		ElidedNode *en = lfirst_node(ElidedNode, lc);

		if (en->plan_node_id == plan->plan_node_id)
			view = filter_join_relids(cx, en->relids);
	}
	return view;
}

static int
max_plan_node_id(Plan *plan)
{
	int			m;
	ListCell   *lc;
	List	   *kids = NIL;

	if (plan == NULL)
		return -1;
	m = plan->plan_node_id;
	m = Max(m, max_plan_node_id(outerPlan(plan)));
	m = Max(m, max_plan_node_id(innerPlan(plan)));
	switch (nodeTag(plan))
	{
		case T_Append:
			kids = ((Append *) plan)->appendplans;
			break;
		case T_MergeAppend:
			kids = ((MergeAppend *) plan)->mergeplans;
			break;
		case T_BitmapAnd:
			kids = ((BitmapAnd *) plan)->bitmapplans;
			break;
		case T_BitmapOr:
			kids = ((BitmapOr *) plan)->bitmapplans;
			break;
		case T_CustomScan:
			kids = ((CustomScan *) plan)->custom_plans;
			break;
		case T_SubqueryScan:
			m = Max(m, max_plan_node_id(((SubqueryScan *) plan)->subplan));
			break;
		default:
			break;
	}
	foreach(lc, kids)
		m = Max(m, max_plan_node_id((Plan *) lfirst(lc)));
	return m;
}

/* ---------- statistics references ---------- */

static void
append_stats_refs(ObsCtx *cx, StringInfo buf)
{
	List	   *seen = NIL;
	bool		first = true;

	appendStringInfoChar(buf, '[');
	for (int rti = 1; rti <= cx->nrt; rti++)
	{
		RangeTblEntry *rte = cx->rt[rti].rte;
		int64		version;
		char		relkind;

		if (rte == NULL || rte->rtekind != RTE_RELATION ||
			!OidIsValid(rte->relid) || list_member_oid(seen, rte->relid))
			continue;
		relkind = get_rel_relkind(rte->relid);
		if (relkind != RELKIND_RELATION && relkind != RELKIND_MATVIEW &&
			relkind != RELKIND_PARTITIONED_TABLE && relkind != RELKIND_FOREIGN_TABLE)
			continue;
		seen = lappend_oid(seen, rte->relid);

		version = pgpi_log_stats ? pgpi_stats_version(rte->relid) : 0;
		if (!first)
			appendStringInfoChar(buf, ',');
		first = false;
		appendStringInfo(buf, "{\"relid\":%u,\"rel\":", rte->relid);
		pgpi_append_json_string(buf,
								quote_qualified_identifier(get_namespace_name_or_temp(get_rel_namespace(rte->relid)),
														   get_rel_name(rte->relid)));
		appendStringInfoString(buf, ",\"version\":");
		pgpi_append_int64(buf, version);
		appendStringInfoChar(buf, '}');

		/* first sight of this version: save its contents */
		if (pgpi_log_stats && !pgpi_stats_seen(MyDatabaseId, rte->relid, version, false))
		{
			char	   *stats = pgpi_stats_capture(rte->relid);
			PgpiStatsRecHdr hdr;
			const char *fields[PGPI_SF_NFIELDS];

			memset(&hdr, 0, sizeof(hdr));
			hdr.captured_at = GetCurrentTimestamp();
			hdr.version = version;
			hdr.dbid = MyDatabaseId;
			hdr.relid = rte->relid;
			hdr.encoding = GetDatabaseEncoding();
			fields[PGPI_SF_DATABASE] = get_database_name(MyDatabaseId);
			fields[PGPI_SF_REL] = quote_qualified_identifier(get_namespace_name_or_temp(get_rel_namespace(rte->relid)),
															 get_rel_name(rte->relid));
			fields[PGPI_SF_STATS] = stats;
			if (pgpi_ring_put(PGPI_REC_STATS, &hdr, sizeof(hdr), fields,
							  PGPI_SF_NFIELDS))
				(void) pgpi_stats_seen(MyDatabaseId, rte->relid, version, true);
		}
	}
	appendStringInfoChar(buf, ']');
}

/* ---------- entry point ---------- */

static void build_static_info(PlannerGlobal *glob, PgpiPlanState *ps,
							  PlannedStmt *pstmt);

/*
 * Observation must never make a query fail. An error while describing the
 * plan is reported as a WARNING, and the plan simply goes without static info
 * (query cancels are re-thrown).
 */
void
pgpi_obs_planner_shutdown(PlannerGlobal *glob, Query *parse, PlannedStmt *pstmt)
{
	PgpiPlanState *ps = pgpi_plan_state(glob);
	MemoryContext oldcxt = CurrentMemoryContext;

	if (ps == NULL)
		return;
	if (!obs_static_enabled() || pstmt == NULL || pstmt->planTree == NULL)
		return;

	PG_TRY();
	{
		build_static_info(glob, ps, pstmt);
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcxt);
		edata = CopyErrorData();
		if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED)
			PG_RE_THROW();
		FlushErrorState();
		ereport(WARNING,
				(errmsg("pgpi: could not describe the plan: %s", edata->message)));
		FreeErrorData(edata);
	}
	PG_END_TRY();
}

static void
build_static_info(PlannerGlobal *glob, PgpiPlanState *ps, PlannedStmt *pstmt)
{
	ObsCtx		cx;
	MemoryContext old;
	StringInfoData meta;
	List	   *frags = NIL;
	int64		planid;
	double		planning_ms;
	ListCell   *lc;
	ListCell   *lc2;
	int			i;

	old = MemoryContextSwitchTo(GetMemoryChunkContext(pstmt));

	memset(&cx, 0, sizeof(cx));
	cx.pstmt = pstmt;
	cx.glob = glob;
	cx.ps = ps;
	cx.nrt = list_length(pstmt->rtable);
	cx.rt = palloc0(sizeof(RtInfo) * (cx.nrt + 1));
	build_rtinfo(&cx);

	cx.maxid = max_plan_node_id(pstmt->planTree);
	foreach(lc, pstmt->subplans)
		cx.maxid = Max(cx.maxid, max_plan_node_id((Plan *) lfirst(lc)));
	cx.frag = palloc0(sizeof(char *) * (cx.maxid + 1));
	cx.matched_node = palloc(sizeof(int) * (list_length(ps->reqs) + 1));
	for (i = 0; i <= list_length(ps->reqs); i++)
		cx.matched_node[i] = -1;
	initStringInfo(&cx.shape);

	(void) walk_plan(&cx, pstmt->planTree, NIL);
	foreach(lc, pstmt->subplans)
	{
		appendStringInfoString(&cx.shape, "|");
		(void) walk_plan(&cx, (Plan *) lfirst(lc), NIL);
	}

	planning_ms = (double) (GetCurrentTimestamp() - ps->start) / 1000.0;
	/* the plan id identifies the shape of the plan: node types, tree, relations */
	planid = (int64) hash_bytes_extended((const unsigned char *) cx.shape.data,
										 cx.shape.len, 0);

	/* plan-level metadata */
	initStringInfo(&meta);
	appendStringInfoString(&meta, "{\"planning_ms\":");
	pgpi_append_double(&meta, planning_ms);
	appendStringInfo(&meta, ",\"mode\":\"%s\"",
					 pgpi_mode == PGPI_MODE_ENFORCE ? "enforce" :
					 pgpi_mode == PGPI_MODE_SHADOW ? "shadow" : "off");

	/* range table */
	appendStringInfoString(&meta, ",\"rtable\":[");
	for (i = 1; i <= cx.nrt; i++)
	{
		RtInfo	   *ri = &cx.rt[i];

		if (i > 1)
			appendStringInfoChar(&meta, ',');
		appendStringInfo(&meta, "{\"rti\":%d,\"kind\":\"%s\",\"ident\":", i,
						 ri->rte ? rtekind_name(ri->rte->rtekind) : "null");
		pgpi_append_json_string(&meta, ri->ident);
		appendStringInfoString(&meta, ",\"alias\":");
		pgpi_append_json_string(&meta, (ri->rte && ri->rte->eref) ? ri->rte->eref->aliasname : NULL);
		appendStringInfoString(&meta, ",\"rel\":");
		if (ri->rte && ri->rte->rtekind == RTE_RELATION && OidIsValid(ri->rte->relid))
		{
			pgpi_append_json_string(&meta,
									quote_qualified_identifier(get_namespace_name_or_temp(get_rel_namespace(ri->rte->relid)),
															   get_rel_name(ri->rte->relid)));
			appendStringInfo(&meta, ",\"relid\":%u,\"inh\":%s", ri->rte->relid,
							 ri->rte->inh ? "true" : "false");
		}
		else
			appendStringInfoString(&meta, "null");
		appendStringInfoString(&meta, ",\"plan_name\":");
		pgpi_append_json_string(&meta, ri->plan_name);
		appendStringInfo(&meta, ",\"local_rti\":%u}", ri->local);
	}
	appendStringInfoChar(&meta, ']');

	/* subplans */
	appendStringInfoString(&meta, ",\"subplans\":[");
	i = 0;
	forboth(lc, pstmt->subplans, lc2, glob->subroots)
	{
		Plan	   *sp = (Plan *) lfirst(lc);
		PlannerInfo *sr = (PlannerInfo *) lfirst(lc2);

		i++;
		if (i > 1)
			appendStringInfoChar(&meta, ',');
		appendStringInfo(&meta, "{\"plan_id\":%d,\"name\":", i);
		pgpi_append_json_string(&meta, sr ? sr->plan_name : NULL);
		appendStringInfo(&meta, ",\"node\":%d}", sp ? sp->plan_node_id : -1);
	}
	appendStringInfoChar(&meta, ']');

	/* statistics versions (and first-sight capture) */
	appendStringInfoString(&meta, ",\"stats_refs\":");
	append_stats_refs(&cx, &meta);

	/* requests of this planning cycle */
	appendStringInfoString(&meta, ",\"requests\":[");
	if (pgpi_log_requests != PGPI_LOGREQ_NONE)
	{
		bool		first = true;

		i = 0;
		foreach(lc, ps->reqs)
		{
			PgpiPlanReq *pr = (PgpiPlanReq *) lfirst(lc);
			int			node = cx.matched_node[i++];

			if (pgpi_log_requests == PGPI_LOGREQ_MATCHED && node < 0)
				continue;
			if (!first)
				appendStringInfoChar(&meta, ',');
			first = false;
			appendStringInfoString(&meta, "{\"seq\":");
			pgpi_append_int64(&meta, pr->seq);
			appendStringInfo(&meta, ",\"node\":%d,\"answer\":", node);
			if (pr->has_answer)
				pgpi_append_double(&meta, pr->answer);
			else
				appendStringInfoString(&meta, "null");
			appendStringInfoString(&meta, ",\"source\":");
			pgpi_append_json_string(&meta, pr->source);
			appendStringInfo(&meta, ",\"applied\":%s,\"request\":%s}",
							 pr->applied ? "true" : "false", pr->json);
		}
	}
	appendStringInfoString(&meta, "]}");

	for (i = 0; i <= cx.maxid; i++)
		frags = lappend(frags, makeString(cx.frag[i] ? cx.frag[i] : pstrdup("")));

	pstmt->extension_state =
		lappend(pstmt->extension_state,
				makeDefElem(PGPI_PSTMT_KEY,
							(Node *) list_make4(makeString(meta.data),
												frags,
												makeString(psprintf(INT64_FORMAT, planid)),
												makeString(psprintf("%.17g", planning_ms))),
							-1));
	MemoryContextSwitchTo(old);
}
