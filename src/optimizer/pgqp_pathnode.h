/*-------------------------------------------------------------------------
 * pgqp_pathnode.h
 *
 * Copyright (c) 2021-2022, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_PATHNODE_H__
#define __PGQP_PATHNODE_H__

#include "postgres.h"
#include "nodes/pathnodes.h"

extern Path *pgqp_create_seqscan_path(PlannerInfo *root, RelOptInfo *rel,
									  Relids required_outer, int parallel_workers);
extern Path *pgqp_create_samplescan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer);
extern IndexPath *pgqp_create_index_path(PlannerInfo *root,
										 IndexOptInfo *index,
										 List *indexclauses,
										 List *indexorderbys,
										 List *indexorderbycols,
										 List *pathkeys,
										 ScanDirection indexscandir,
										 bool indexonly,
										 Relids required_outer,
										 double loop_count,
										 bool partial_path);
extern BitmapHeapPath *pgqp_create_bitmap_heap_path(PlannerInfo *root,
													RelOptInfo *rel,
													Path *bitmapqual,
													Relids required_outer,
													double loop_count,
													int parallel_degree);
extern BitmapAndPath *pgqp_create_bitmap_and_path(PlannerInfo *root,
												  RelOptInfo *rel,
												  List *bitmapquals);
extern BitmapOrPath *pgqp_create_bitmap_or_path(PlannerInfo *root,
												RelOptInfo *rel,
												List *bitmapquals);
extern AppendPath *pgqp_create_append_path(PlannerInfo *root,
										   RelOptInfo *rel,
										   List *subpaths, List *partial_subpaths,
										   List *pathkeys, Relids required_outer,
										   int parallel_workers, bool parallel_aware,
										   double rows);
extern GatherMergePath *pgqp_create_gather_merge_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
													  PathTarget *target, List *pathkeys,
													  Relids required_outer, double *rows);
extern GatherPath *pgqp_create_gather_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
										   PathTarget *target, Relids required_outer, double *rows);
extern SubqueryScanPath *pgqp_create_subqueryscan_path(PlannerInfo *root, RelOptInfo *rel, Path *subpath,
													   List *pathkeys, Relids required_outer);
extern Path *pgqp_create_ctescan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer);
extern Path *pgqp_create_namedtuplestorescan_path(PlannerInfo *root, RelOptInfo *rel,
												  Relids required_outer);

extern NestPath *pgqp_create_nestloop_path(PlannerInfo *root,
										   RelOptInfo *joinrel,
										   JoinType jointype,
										   JoinCostWorkspace *workspace,
										   JoinPathExtraData *extra,
										   Path *outer_path,
										   Path *inner_path,
										   List *restrict_clauses,
										   List *pathkeys,
										   Relids required_outer);
extern MergePath *pgqp_create_mergejoin_path(PlannerInfo *root,
											 RelOptInfo *joinrel,
											 JoinType jointype,
											 JoinCostWorkspace *workspace,
											 JoinPathExtraData *extra,
											 Path *outer_path,
											 Path *inner_path,
											 List *restrict_clauses,
											 List *pathkeys,
											 Relids required_outer,
											 List *mergeclauses,
											 List *outersortkeys,
											 List *innersortkeys);
extern HashPath *pgqp_create_hashjoin_path(PlannerInfo *root,
										   RelOptInfo *joinrel,
										   JoinType jointype,
										   JoinCostWorkspace *workspace,
										   JoinPathExtraData *extra,
										   Path *outer_path,
										   Path *inner_path,
										   bool parallel_hash,
										   List *restrict_clauses,
										   Relids required_outer,
										   List *hashclauses);

extern Path *pgqp_reparameterize_path(PlannerInfo *root, Path *path,
									  Relids required_outer,
									  double loop_count);



#endif							/* __PGQP_PATHNODE_H__ */
