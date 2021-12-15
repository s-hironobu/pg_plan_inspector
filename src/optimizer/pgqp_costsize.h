/*-------------------------------------------------------------------------
 * pgqp_costsize.h
 *
 * Copyright (c) 2021, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_COSTSIZE_H__
#define __PGQP_COSTSIZE_H__
#include "postgres.h"

#include "optimizer/paths.h"


extern void pgqp_cost_index(IndexPath *path, PlannerInfo *root, double loop_count,
							bool partial_path);
extern void pgqp_cost_bitmap_tree_node(Path *path, Cost *cost, Selectivity *selec);
extern void pgqp_cost_bitmap_and_node(BitmapAndPath *path, PlannerInfo *root);
extern void pgqp_cost_tidrangescan(Path *path, PlannerInfo *root,
								   RelOptInfo *baserel, List *tidrangequals,
								   ParamPathInfo *param_info);

extern void pgqp_cost_group(Path *path, PlannerInfo *root,
							int numGroupCols, double numGroups,
							List *quals,
							Cost input_startup_cost, Cost input_total_cost,
							double input_tuples);
extern void pgqp_initial_cost_nestloop(PlannerInfo *root, JoinCostWorkspace *workspace,
									   JoinType jointype,
									   Path *outer_path, Path *inner_path,
									   JoinPathExtraData *extra);
extern void pgqp_final_cost_nestloop(PlannerInfo *root, NestPath *path,
									 JoinCostWorkspace *workspace,
									 JoinPathExtraData *extra);
extern void pgqp_initial_cost_mergejoin(PlannerInfo *root, JoinCostWorkspace *workspace,
										JoinType jointype,
										List *mergeclauses,
										Path *outer_path, Path *inner_path,
										List *outersortkeys, List *innersortkeys,
										JoinPathExtraData *extra);
extern void pgqp_final_cost_mergejoin(PlannerInfo *root, MergePath *path,
									  JoinCostWorkspace *workspace,
									  JoinPathExtraData *extra);
extern void pgqp_initial_cost_hashjoin(PlannerInfo *root, JoinCostWorkspace *workspace,
									   JoinType jointype,
									   List *hashclauses,
									   Path *outer_path, Path *inner_path,
									   JoinPathExtraData *extra,
									   bool parallel_hash);
extern void pgqp_final_cost_hashjoin(PlannerInfo *root, HashPath *path,
									 JoinCostWorkspace *workspace,
									 JoinPathExtraData *extra);
extern void pgqp_compute_semi_anti_join_factors(PlannerInfo *root,
												RelOptInfo *joinrel,
												RelOptInfo *outerrel,
												RelOptInfo *innerrel,
												JoinType jointype,
												SpecialJoinInfo *sjinfo,
												List *restrictlist,
												SemiAntiJoinFactors *semifactors);
extern void pgqp_set_baserel_size_estimates(PlannerInfo *root, RelOptInfo *rel);
extern double pgqp_get_parameterized_baserel_size(PlannerInfo *root, RelOptInfo *rel,
												  List *param_clauses);
extern void pgqp_set_joinrel_size_estimates(PlannerInfo *root, RelOptInfo *rel,
											RelOptInfo *outer_rel,
											RelOptInfo *inner_rel,
											SpecialJoinInfo *sjinfo,
											List *restrictlist);
extern double pgqp_get_parameterized_joinrel_size(PlannerInfo *root, RelOptInfo *rel,
												  Path *outer_path,
												  Path *inner_path,
												  SpecialJoinInfo *sjinfo,
												  List *restrict_clauses);
extern void pgqp_set_result_size_estimates(PlannerInfo *root, RelOptInfo *rel);
extern double pgqp_compute_bitmap_pages(PlannerInfo *root, RelOptInfo *baserel, Path *bitmapqual,
										int loop_count, Cost *cost, double *tuple);
extern void pgqp_cost_bitmap_heap_scan(Path *path, PlannerInfo *root, RelOptInfo *baserel,
									   ParamPathInfo *param_info,
									   Path *bitmapqual, double loop_count);

#endif							/* __PGQP_COSTSIZE_H__ */
