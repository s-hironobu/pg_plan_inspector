/*-------------------------------------------------------------------------
 * pgqp_setrefs.h
 *
 * Copyright (c) 2021-2022, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_SETREFS_H__
#define __PGQP_SETREFS_H__

#include "postgres.h"
#include "nodes/pathnodes.h"

extern Plan *pgqp_set_plan_references(PlannerInfo *root, Plan *plan);

#endif							/* __PGQP_SETREFS_H__ */
