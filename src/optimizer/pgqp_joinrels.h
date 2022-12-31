/*-------------------------------------------------------------------------
 * pgqp_relnode.h
 *
 * Copyright (c) 2021-2023, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_JOINRELS_H__
#define __PGQP_JOINRELS_H__

#include "postgres.h"
#include "nodes/pathnodes.h"

extern void pgqp_join_search_one_level(PlannerInfo *root, int level);
extern RelOptInfo *pgqp_make_join_rel(PlannerInfo *root, RelOptInfo *rel1, RelOptInfo *rel2);

#endif							/* __PGQP_JOINRELS_H__ */
