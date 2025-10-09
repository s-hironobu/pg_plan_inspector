/*-------------------------------------------------------------------------
 * pg_query_plan.h
 *
 * Copyright (c) 2021-2025, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_QUERY_PLAN_H
#define PG_QUERY_PLAN_H

#include "executor/executor.h"
#include "common.h"
#include "storage/ipc.h"
#include "tcop/utility.h"
#include "optimizer/planner.h"
#include "optimizer/paths.h"
#include "miscadmin.h"
#include "libpq/auth.h"



extern pgqpSharedState *pgqp;
extern HTAB	   *pgqp_hash;

extern bool		pgqp_enable_adjust_joinrel_rows;
extern bool		pgqp_enable_adjust_rel_rows;
extern bool		pgqp_log_buffers;
extern bool		pgqp_log_wal;
extern bool		pgqp_work_mem;
extern int		tmp_work_mem;

extern bool		pgqp_received_signal;

extern QueryDesc  *qp_qd[MAX_NESTED_LEVEL];
extern struct ExplainState *qp_es[MAX_NESTED_LEVEL];

extern shmem_request_hook_type prev_shmem_request_hook;
extern shmem_startup_hook_type prev_shmem_startup_hook;
extern ExecutorStart_hook_type prev_ExecutorStart;
extern ExecutorRun_hook_type prev_ExecutorRun;
extern ExecutorFinish_hook_type prev_ExecutorFinish;
extern ExecutorEnd_hook_type prev_ExecutorEnd;
extern ProcessUtility_hook_type prev_ProcessUtility;
extern ClientAuthentication_hook_type prev_ClientAuthentication;
extern planner_hook_type prev_planner;
extern join_search_hook_type prev_join_search;
extern set_rel_pathlist_hook_type prev_set_rel_pathlist;
extern set_join_pathlist_hook_type prev_set_join_pathlist;

#endif
