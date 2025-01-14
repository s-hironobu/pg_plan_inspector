/*-------------------------------------------------------------------------
 * pgqp_planner.h
 *
 * Copyright (c) 2021-202h, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_PLANNER_H__
#define __PGQP_PLANNER_H__

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/params.h"

extern PlannedStmt *pgqp_standard_planner(Query *parse, const char *query_string, int cursorOptions,
										  ParamListInfo boundParams);
#if PG_VERSION_NUM >= 170000
extern PlannerInfo *pgqp_subquery_planner(PlannerGlobal *glob, Query *parse,
									 PlannerInfo *parent_root,
									 bool hasRecursion, double tuple_fraction,
									 SetOperationStmt *setops);
#else
extern PlannerInfo *pgqp_subquery_planner(PlannerGlobal *glob, Query *parse,
										  PlannerInfo *parent_root,
										  bool hasRecursion, double tuple_fraction);
#endif
extern RowMarkType pgqp_select_rowmark_type(RangeTblEntry *rte, LockClauseStrength strength);
extern void pgqp_mark_partial_aggref(Aggref *agg, AggSplit aggsplit);
extern Path *pgqp_get_cheapest_fractional_path(RelOptInfo *rel, double tuple_fraction);


#endif							/* __PGQP_PLANNER_H__ */
