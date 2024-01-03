/*-------------------------------------------------------------------------
 * pgqp_inherit.h
 *
 * Copyright (c) 2021-2024, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_INHERIT_H__
#define __PGQP_INHERIT_H__

#include "postgres.h"
#include "nodes/pathnodes.h"

extern void pgqp_expand_inherited_rtentry(PlannerInfo *root, RelOptInfo *rel,
										  RangeTblEntry *rte, Index rti);


#endif							/* __PGQP_INHERIT_H__ */
