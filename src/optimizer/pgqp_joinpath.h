/*-------------------------------------------------------------------------
 * pgqp_joinpath.h
 *
 * Copyright (c) 2021-2024, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_JOINPATH_H__
#define __PGQP_JOINPATH_H__

extern void pgqp_add_paths_to_joinrel(PlannerInfo *root,
									  RelOptInfo *joinrel,
									  RelOptInfo *outerrel,
									  RelOptInfo *innerrel,
									  JoinType jointype,
									  SpecialJoinInfo *sjinfo,
									  List *restrictlist);
#endif
