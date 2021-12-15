/*-------------------------------------------------------------------------
 * pgqp_subselect.h
 *
 * Copyright (c) 2021, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PGQP_SUBSELECT_H__
#define __PGQP_SUBSELECT_H__

#include "postgres.h"
#include "nodes/pathnodes.h"

extern void pgqp_SS_process_ctes(PlannerInfo *root);
extern Node *pgqp_SS_process_sublinks(PlannerInfo *root, Node *expr, bool isQual);

#endif							/* __PGQP_SUBSELECT_H__ */
