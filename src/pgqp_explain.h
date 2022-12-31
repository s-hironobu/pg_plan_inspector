/*-------------------------------------------------------------------------
 * pgqp_explain.h
 *
 * Copyright (c) 2021-2023, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGQP_EXPLAIN_H
#define PGQP_EXPLAIN_H

#include "commands/explain.h"
#include "common.h"

extern void pgqpExplainPrintQueryState(ExplainState *es, QueryDesc *queryDesc);
extern void pgqpExplainBeginOutput(ExplainState *es);
extern void pgqpExplainEndOutput(ExplainState *es);

#endif							/* PGQP_EXPLAIN_H */
