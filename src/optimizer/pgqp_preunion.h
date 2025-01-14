/*-------------------------------------------------------------------------
 * pgqp_preunion.h
 *
 * Copyright (c) 2021-2025, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_PREUNION_H__
#define __PGQP_PREUNION_H__

#include "postgres.h"
#include "nodes/pathnodes.h"

extern RelOptInfo *pgqp_plan_set_operations(PlannerInfo *root);

#endif							/* __PGQP_PREUNION_H__ */
