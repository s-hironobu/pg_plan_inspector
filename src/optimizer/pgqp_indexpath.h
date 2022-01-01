/*-------------------------------------------------------------------------
 * pgqp_indexpath.h
 *
 * Copyright (c) 2021-2022, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_INDEXPATH_H__
#define __PGQP_INDEXPATH_H__

#include "postgres.h"
#include "nodes/pathnodes.h"

extern void pgqp_create_index_paths(PlannerInfo *root, RelOptInfo *rel);

#endif							/* __PGQP_INDEXPATH_H__ */
