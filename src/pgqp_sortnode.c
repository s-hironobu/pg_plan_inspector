/*-------------------------------------------------------------------------
 * pgqp_sortnode.c
 *
 * This is based on src/pgqp_explain.c, and it is modified for workmem-size adjusting.
 *
 * Copyright (c) 2021-2025, Hironobu Suzuki @ interdb.jp
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#define __PG_QUERY_PLAN__

#include "postgres.h"
#include "nodes/nodeFuncs.h"
#include "utils/ruleutils.h"

#ifdef __PG_QUERY_PLAN__
#include "common.h"
#include "pg_query_plan.h"
#include "pgqp_sortnode.h"
#endif							/* __PG_QUERY_PLAN__ */

/* Declare static functions */
static bool ExplainPreScanNode(PlanState *planstate, Bitmapset **rels_used);

static int ExplainMemberNodes(PlanState **planstates, int nplans,
							  List *ancestors, ExplainState *es, int mem);
static int ExplainSubPlans(List *plans, List *ancestors,
						   const char *relationship, ExplainState *es, int mem);
static int ExplainCustomChildren(CustomScanState *css,
								 List *ancestors, ExplainState *es, int mem);
static ExplainWorkersState *ExplainCreateWorkersState(int num_workers);
static int SearchSort(PlanState *planstate, List *ancestors,
					  const char *relationship, const char *plan_name,
					  ExplainState *es, int mem);


#ifdef __PG_QUERY_PLAN__
int
pgqp_ExplainQueryState(ExplainState *es, QueryDesc *queryDesc)
{
	Bitmapset  *rels_used = NULL;
	PlanState  *ps;

	/* Set up ExplainState fields associated with this plan tree */
	Assert(queryDesc->plannedstmt != NULL);
	es->pstmt = queryDesc->plannedstmt;
	es->rtable = queryDesc->plannedstmt->rtable;
	ExplainPreScanNode(queryDesc->planstate, &rels_used);
	es->rtable_names = select_rtable_names_for_explain(es->rtable, rels_used);
	es->deparse_cxt = deparse_context_for_plan_tree(queryDesc->plannedstmt,
													es->rtable_names);
	es->printed_subplans = NULL;


	/*
	 * Sometimes we mark a Gather node as "invisible", which means that it's
	 * not to be displayed in EXPLAIN output.  The purpose of this is to allow
	 * running regression tests with force_parallel_mode=regress to get the
	 * same results as running the same tests with force_parallel_mode=off.
	 * Such marking is currently only supported on a Gather at the top of the
	 * plan.  We skip that node, and we must also hide per-worker detail data
	 * further down in the plan tree.
	 */
	ps = queryDesc->planstate;
	if (IsA(ps, GatherState) && ((Gather *) ps->plan)->invisible)
	{
		ps = outerPlanState(ps);
		es->hide_workers = true;
	}
	return SearchSort(ps, NIL, NULL, NULL, es, 0);

}

static int
SearchSort(PlanState *planstate, List *ancestors,
			const char *relationship, const char *plan_name,
		   ExplainState *es, int mem)
{
	Plan	   *plan = planstate->plan;
	ExplainWorkersState *save_workers_state = es->workers_state;
	int			save_indent = es->indent;
	bool		haschildren;
	int work_mem = mem;

	if (planstate->worker_instrument && es->analyze && !es->hide_workers)
		es->workers_state = ExplainCreateWorkersState(planstate->worker_instrument->num_workers);
	else
		es->workers_state = NULL;

	/* quals, sort keys, etc */
	if (nodeTag(plan) == T_Sort)
	{
		int width = plan->plan_width;
		int row = plan->plan_rows;
		
		/*
		 * TODO
		 *
		 * This is a useful heuristic.
		 * Normally, the memory required for Quicksort should be calculate taking into
		 * account the expected data volume, data types, and other relevant factors.
		 */
		work_mem += width * (row / 100);
		return work_mem;
	}

	es->workers_state = save_workers_state;

	/* Get ready to display the child plans */
	haschildren = planstate->initPlan ||
		outerPlanState(planstate) ||
		innerPlanState(planstate) ||
		IsA(plan, Append) ||
		IsA(plan, MergeAppend) ||
		IsA(plan, BitmapAnd) ||
		IsA(plan, BitmapOr) ||
		IsA(plan, SubqueryScan) ||
		(IsA(planstate, CustomScanState) &&
		 ((CustomScanState *) planstate)->custom_ps != NIL) ||
		planstate->subPlan;
	if (haschildren)
	{
		/* Pass current Plan as head of ancestors list for children */
		ancestors = lcons(plan, ancestors);
	}

	/* initPlan-s */
	if (planstate->initPlan)
		work_mem = ExplainSubPlans(planstate->initPlan, ancestors, "InitPlan", es, work_mem);

	/* lefttree */
	if (outerPlanState(planstate))
		work_mem = SearchSort(outerPlanState(planstate), ancestors,
							  "Outer", NULL, es, work_mem);

	/* righttree */
	if (innerPlanState(planstate))
		work_mem = SearchSort(innerPlanState(planstate), ancestors,
							  "Inner", NULL, es, work_mem);

	/* special child plans */
	
	switch (nodeTag(plan))
	{
		case T_Append:
			work_mem += ExplainMemberNodes(((AppendState *) planstate)->appendplans,
										   ((AppendState *) planstate)->as_nplans,
										   ancestors, es, work_mem);
			break;
		case T_MergeAppend:
			work_mem += ExplainMemberNodes(((MergeAppendState *) planstate)->mergeplans,
										   ((MergeAppendState *) planstate)->ms_nplans,
										   ancestors, es, work_mem);
			break;
		case T_BitmapAnd:
			work_mem += ExplainMemberNodes(((BitmapAndState *) planstate)->bitmapplans,
										   ((BitmapAndState *) planstate)->nplans,
										   ancestors, es, work_mem);
			break;
		case T_BitmapOr:
			work_mem += ExplainMemberNodes(((BitmapOrState *) planstate)->bitmapplans,
										   ((BitmapOrState *) planstate)->nplans,
										   ancestors, es, work_mem);
			break;
		case T_SubqueryScan:
			work_mem += SearchSort(((SubqueryScanState *) planstate)->subplan, ancestors,
								   "Subquery", NULL, es, work_mem);
			break;
		case T_CustomScan:
			work_mem += ExplainCustomChildren((CustomScanState *) planstate,
											  ancestors, es, work_mem);
			break;
		default:
			break;
	}

	/* subPlan-s */
	if (planstate->subPlan)
		work_mem += ExplainSubPlans(planstate->subPlan, ancestors, "SubPlan", es, work_mem);

	/* end of child plans */
	if (haschildren)
	{
		ancestors = list_delete_first(ancestors);
		/*
		  ExplainCloseGroup("Plans", "Plans", false, es);
		*/
	}

	/* in text format, undo whatever indentation we added */
	if (es->format == EXPLAIN_FORMAT_TEXT)
		es->indent = save_indent;

	return work_mem;
}

#endif							/* __PG_QUERY_PLAN__ */


/*
 * ExplainPreScanNode -
 *	  Prescan the planstate tree to identify which RTEs are referenced
 *
 * Adds the relid of each referenced RTE to *rels_used.  The result controls
 * which RTEs are assigned aliases by select_rtable_names_for_explain.
 * This ensures that we don't confusingly assign un-suffixed aliases to RTEs
 * that never appear in the EXPLAIN output (such as inheritance parents).
 */
static bool
ExplainPreScanNode(PlanState *planstate, Bitmapset **rels_used)
{
	Plan	   *plan = planstate->plan;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
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
			*rels_used = bms_add_member(*rels_used,
										((Scan *) plan)->scanrelid);
			break;
		case T_ForeignScan:
			*rels_used = bms_add_members(*rels_used,
										 ((ForeignScan *) plan)->fs_relids);
			break;
		case T_CustomScan:
			*rels_used = bms_add_members(*rels_used,
										 ((CustomScan *) plan)->custom_relids);
			break;
		case T_ModifyTable:
			*rels_used = bms_add_member(*rels_used,
										((ModifyTable *) plan)->nominalRelation);
			if (((ModifyTable *) plan)->exclRelRTI)
				*rels_used = bms_add_member(*rels_used,
											((ModifyTable *) plan)->exclRelRTI);
			break;
		case T_Append:
			*rels_used = bms_add_members(*rels_used,
										 ((Append *) plan)->apprelids);
			break;
		case T_MergeAppend:
			*rels_used = bms_add_members(*rels_used,
										 ((MergeAppend *) plan)->apprelids);
			break;
		default:
			break;
	}

	return planstate_tree_walker(planstate, ExplainPreScanNode, rels_used);
}





/*
 * Explain the constituent plans of an Append, MergeAppend,
 * BitmapAnd, or BitmapOr node.
 *
 * The ancestors list should already contain the immediate parent of these
 * plans.
 */
static int
ExplainMemberNodes(PlanState **planstates, int nplans,
				   List *ancestors, ExplainState *es, int mem)
{
	int			j;
	int			work_mem = mem;


	for (j = 0; j < nplans; j++)
		work_mem += SearchSort(planstates[j], ancestors,
							   "Member", NULL, es, work_mem);
	return work_mem;
}


/*
 * Explain a list of SubPlans (or initPlans, which also use SubPlan nodes).
 *
 * The ancestors list should already contain the immediate parent of these
 * SubPlans.
 */
static int
ExplainSubPlans(List *plans, List *ancestors,
				const char *relationship, ExplainState *es, int mem)
{
	ListCell   *lst;
	int work_mem = mem;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);
		SubPlan    *sp = sps->subplan;

		/*
		 * There can be multiple SubPlan nodes referencing the same physical
		 * subplan (same plan_id, which is its index in PlannedStmt.subplans).
		 * We should print a subplan only once, so track which ones we already
		 * printed.  This state must be global across the plan tree, since the
		 * duplicate nodes could be in different plan nodes, eg both a bitmap
		 * indexscan's indexqual and its parent heapscan's recheck qual.  (We
		 * do not worry too much about which plan node we show the subplan as
		 * attached to in such cases.)
		 */
		if (bms_is_member(sp->plan_id, es->printed_subplans))
			continue;
		es->printed_subplans = bms_add_member(es->printed_subplans,
											  sp->plan_id);

		/*
		 * Treat the SubPlan node as an ancestor of the plan node(s) within
		 * it, so that ruleutils.c can find the referents of subplan
		 * parameters.
		 */
		ancestors = lcons(sp, ancestors);

		work_mem = SearchSort(sps->planstate, ancestors,
							  relationship, sp->plan_name, es, mem);

		ancestors = list_delete_first(ancestors);
	}
	return work_mem;
}

/*
 * Explain a list of children of a CustomScan.
 */
static int
ExplainCustomChildren(CustomScanState *css, List *ancestors, ExplainState *es, int mem)
{
	ListCell   *cell;
	int work_mem = mem;
	
	const char *label =
	(list_length(css->custom_ps) != 1 ? "children" : "child");

	foreach(cell, css->custom_ps)
		work_mem += SearchSort((PlanState *) lfirst(cell), ancestors, label, NULL, es, work_mem);

	return work_mem;
}

/*
 * Create a per-plan-node workspace for collecting per-worker data.
 *
 * Output related to each worker will be temporarily "set aside" into a
 * separate buffer, which we'll merge into the main output stream once
 * we've processed all data for the plan node.  This makes it feasible to
 * generate a coherent sub-group of fields for each worker, even though the
 * code that produces the fields is in several different places in this file.
 * Formatting of such a set-aside field group is managed by
 * ExplainOpenSetAsideGroup and ExplainSaveGroup/ExplainRestoreGroup.
 */
static ExplainWorkersState *
ExplainCreateWorkersState(int num_workers)
{
	ExplainWorkersState *wstate;

	wstate = (ExplainWorkersState *) palloc(sizeof(ExplainWorkersState));
	wstate->num_workers = num_workers;
	wstate->worker_inited = (bool *) palloc0(num_workers * sizeof(bool));
	wstate->worker_str = (StringInfoData *)
		palloc0(num_workers * sizeof(StringInfoData));
	wstate->worker_state_save = (int *) palloc(num_workers * sizeof(int));
	return wstate;
}

