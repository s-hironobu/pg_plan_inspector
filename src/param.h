/*-------------------------------------------------------------------------
 * param.h
 *
 * Copyright (c) 2021-2022, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PARAM_H__
#define __PARAM_H__

#include "param_parser.h"

/* The pointer to a json buffer for YY_INPUT()@flex */
char	   *pgqp_reg_param;

extern void pre_param_parse(const int plan_len);
extern void init_param_parse_env(void);

#endif
