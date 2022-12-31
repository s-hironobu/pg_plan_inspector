/*-------------------------------------------------------------------------
 * planid.h
 *
 * Copyright (c) 2021-2023, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PLANID_H__
#define __PLANID_H__

#include "planid_parser.h"

extern uint64 get_planId(void);
extern void pre_plan_parse(const int plan_len);

#endif
