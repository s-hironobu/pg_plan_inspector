/*-------------------------------------------------------------------------
 * pgqp_initsplan.h
 *
 * Copyright (c) 2021-2024, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_INITSPLAN_H__
#define __PGQP_INITSPLAN_H__

#include "postgres.h"
#include "nodes/pathnodes.h"

extern void pgqp_add_other_rels_to_query(PlannerInfo *root);

#endif							/* __PGQP_INITSPLAN_H__ */
