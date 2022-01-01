/*-------------------------------------------------------------------------
 * pgqp_planmain.h
 *
 * Copyright (c) 2021-2022, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_PLANMAIN_H__
#define __PGQP_PLANMAIN_H__

#include "postgres.h"
#include "nodes/pathnodes.h"
#include "optimizer/planmain.h"

extern RelOptInfo *pgqp_query_planner(PlannerInfo *root,
									  query_pathkeys_callback qp_callback, void *qp_extra);

#endif							/* __PGQP_PLANMAIN_H__ */
