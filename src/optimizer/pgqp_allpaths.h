/*-------------------------------------------------------------------------
 * pgqp_allpaths.h
 *
 * Copyright (c) 2021-2024, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_ALLPATHS_H__
#define __PGQP_ALLPATHS_H__

#include "postgres.h"
#include "nodes/pathnodes.h"

extern RelOptInfo *pgqp_make_one_rel(PlannerInfo *root, List *joinlist);
extern void pgqp_add_paths_to_append_rel(PlannerInfo *root, RelOptInfo *rel, List *live_childrels);
extern void pgqp_generate_gather_paths(PlannerInfo *root, RelOptInfo *rel, bool override_rows);
extern void pgqp_generate_useful_gather_paths(PlannerInfo *root, RelOptInfo *rel, bool override_rows);
extern RelOptInfo *pgqp_standard_join_search(PlannerInfo *root, int levels_needed, List *initial_rels);
extern void pgqp_create_partial_bitmap_paths(PlannerInfo *root, RelOptInfo *rel, Path *bitmapqual);
extern void pgqp_generate_partitionwise_join_paths(PlannerInfo *root, RelOptInfo *rel);

#endif							/* __PGQP_ALLPATHS_H__ */
