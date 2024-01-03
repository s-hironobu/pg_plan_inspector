/*-------------------------------------------------------------------------
 * pgqp_planagg.h
 *
 * Copyright (c) 2021-2024, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_PLANAGG_H__
#define __PGQP_PLANAGG_H__

#include "postgres.h"
#include "nodes/pathnodes.h"

extern void pgqp_preprocess_minmax_aggregates(PlannerInfo *root);

#endif							/* __PGQP_PLANAGG_H__ */
