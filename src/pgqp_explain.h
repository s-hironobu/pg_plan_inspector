/*-------------------------------------------------------------------------
 * pgqp_explain.h
 *
 * Copyright (c) 2021-2025, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGQP_EXPLAIN_H
#define PGQP_EXPLAIN_H

#include "commands/explain.h"
#include "commands/explain_state.h"
#include "common.h"

void pgqpExplainPrintQueryState(ExplainState *es, QueryDesc *queryDesc);
void pgqpExplainBeginOutput(ExplainState *es);
void pgqpExplainEndOutput(ExplainState *es);

#endif							/* PGQP_EXPLAIN_H */
