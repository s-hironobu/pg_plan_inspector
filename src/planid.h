/*-------------------------------------------------------------------------
 * planid.h
 *
 * Copyright (c) 2021, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PLANID_H__
#define __PLANID_H__

#include "planid_parser.h"

/* The pointer to a json buffer for YY_INPUT()@flex */
char	   *pgqp_json_plan;

extern uint64 get_planId(void);
extern void pre_plan_parse(const int plan_len);

#endif
