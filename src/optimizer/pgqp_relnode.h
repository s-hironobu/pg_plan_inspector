/*-------------------------------------------------------------------------
 * pgqp_relnode.h
 *
 * Copyright (c) 2021-2022, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_RELNODE_H__
#define __PGQP_RELNODE_H__
#include "postgres.h"

#include <limits.h>

#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/inherit.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/placeholder.h"
#include "optimizer/plancat.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"

extern RelOptInfo *pgqp_build_join_rel(PlannerInfo *root,
									   Relids joinrelids,
									   RelOptInfo *outer_rel,
									   RelOptInfo *inner_rel,
									   SpecialJoinInfo *sjinfo,
									   List **restrictlist_ptr);
extern RelOptInfo *pgqp_build_child_join_rel(PlannerInfo *root, RelOptInfo *outer_rel,
											 RelOptInfo *inner_rel, RelOptInfo *parent_joinrel,
											 List *restrictlist, SpecialJoinInfo *sjinfo,
											 JoinType jointype);
extern ParamPathInfo *pgqp_get_baserel_parampathinfo(PlannerInfo *root, RelOptInfo *baserel,
													 Relids required_outer);
extern ParamPathInfo *pgqp_get_joinrel_parampathinfo(PlannerInfo *root, RelOptInfo *joinrel,
													 Path *outer_path,
													 Path *inner_path,
													 SpecialJoinInfo *sjinfo,
													 Relids required_outer,
													 List **restrict_clauses);


#endif
