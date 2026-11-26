/*-------------------------------------------------------------------------
 * pgpi.h
 *	  pg_plan_inspector 2 -- cardinality intervention and observation.
 *
 * Two intervention routes share one request/answer core:
 *   route H : PostgreSQL 19 planner hooks
 *   route D : run-time detours of exported size-estimation functions
 *
 * Observation records every executed plan together with
 * descriptors of its nodes. The descriptors have the same format as the
 * requests. So an observation can be turned into a training example
 * without guessing.
 *-------------------------------------------------------------------------
 */
#ifndef PGPI_H
#define PGPI_H

#include "postgres.h"

#include <signal.h>

#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"

#if PG_VERSION_NUM < 190000
#error "pgpi requires PostgreSQL 19 or later"
#endif

typedef enum PgpiMode
{
	PGPI_MODE_OFF,
	PGPI_MODE_SHADOW,
	PGPI_MODE_ENFORCE
} PgpiMode;

/* Where in the planner the question is asked. */
typedef enum PgpiPoint
{
	PGPI_PT_BASE,				/* rows of a base relation */
	PGPI_PT_JOIN,				/* rows of a join relation */
	PGPI_PT_PARAM_BASE,			/* rows per loop of a parameterized base relation */
	PGPI_PT_PARAM_JOIN,			/* rows per loop of a parameterized join relation */
	PGPI_PT_CLAUSELIST			/* selectivity of a clause list on one relation */
} PgpiPoint;

typedef struct PgpiRequest
{
	PgpiPoint	point;
	char		route;			/* 'H' (hook) or 'D' (detour) */
	PlannerInfo *root;
	Relids		relids;			/* target base relids */
	Relids		outer_relids;	/* parameterization, or NULL */
	List	   *clauses;		/* RestrictInfo or Expr nodes */
	JoinType	jointype;
	double		pg_value;		/* rows, or selectivity for CLAUSELIST */
	double		tuples;			/* CLAUSELIST: rel->tuples */
	/*
	 * JOIN: the outer input from which pg_value was computed (its clauses
	 * are already in the rows of the input), or NULL
	 */
	Relids		split;
	double		outer_rows;		/* JOIN with split: rows of the two inputs */
	double		inner_rows;
} PgpiRequest;

/*
 * Per-planning observation state. It is attached to the PlannerGlobal through the
 * planner-extension mechanism and allocated in the memory context of the planner.
 */
typedef struct PgpiPlanReq
{
	PlannerInfo *root;
	PgpiPoint	point;
	char		route;
	Relids		relids;			/* non-join members only */
	Relids		outer_relids;	/* non-join members only */
	double		pg_value;
	bool		has_answer;
	double		answer;
	char	   *source;
	bool		applied;
	int64		seq;
	char	   *json;			/* the request as sent */
} PgpiPlanReq;

typedef struct PgpiPlanState
{
	MemoryContext cxt;
	List	   *roots;			/* PlannerInfo seen during this planning */
	List	   *reqs;			/* PgpiPlanReq * */
	TimestampTz start;
} PgpiPlanState;

/* name of our DefElem in PlannedStmt->extension_state */
#define PGPI_PSTMT_KEY "pgpi"

/* GUC variables (pgpi.c) */
extern int	pgpi_mode;
extern bool pgpi_enable_hook;
extern bool pgpi_enable_detour;
extern bool pgpi_detour_clauselist;
extern bool pgpi_trace;
extern char *pgpi_directives;
extern char *pgpi_callback;
extern int	pgpi_callback_timeout;
extern int	pgpi_log_size;

/* observation GUCs (pgpi.c) */
extern bool pgpi_log_plans;
extern int	pgpi_log_min_duration;
extern double pgpi_log_sample_rate;
extern bool pgpi_log_nested;
extern bool pgpi_log_explain;
extern int	pgpi_log_requests;
extern bool pgpi_log_stats;
extern bool pgpi_log_parameters;
extern bool pgpi_log_canceled;
extern char *pgpi_label;
extern char *pgpi_hints;
extern char *pgpi_log_database;
extern int	pgpi_log_buffer_kb;
extern int	pgpi_log_retention;
extern int	pgpi_log_max_rows;
extern int	pgpi_stats_max;

typedef enum PgpiLogRequests
{
	PGPI_LOGREQ_NONE,
	PGPI_LOGREQ_MATCHED,
	PGPI_LOGREQ_ALL
} PgpiLogRequests;

/* core.c */
extern int	pgpi_ext_id;
extern void pgpi_core_init(void);
extern void pgpi_planning_begin(const char *query_string);
extern void pgpi_planning_end(void);
extern void pgpi_planning_reset(void);
extern double pgpi_intervene(PgpiRequest *req);
extern void pgpi_record_joinrel(PlannerInfo *root, RelOptInfo *joinrel,
								RelOptInfo *outer_rel, RelOptInfo *inner_rel,
								List *restrictlist);
extern List *pgpi_all_clauses(PlannerInfo *root, RelOptInfo *rel);
extern bool pgpi_is_plain_baserel(PlannerInfo *root, RelOptInfo *rel);
extern const char *pgpi_point_name(PgpiPoint p);
extern void pgpi_append_descriptor(StringInfo buf, PlannerInfo *root,
								   PgpiPoint point, Relids relids,
								   Relids outer_relids, List *clauses,
								   JoinType jointype);
extern Relids pgpi_nonjoin_relids(PlannerInfo *root, Relids relids);
extern PgpiPlanState *pgpi_plan_state(PlannerGlobal *glob);
extern void pgpi_note_root(PlannerInfo *root);

/* deparse.c */
extern char *pgpi_deparse_clause(PlannerInfo *root, Node *clause);
extern void pgpi_append_json_string(StringInfo buf, const char *s);

/* detour.c */
typedef struct PgpiDetour
{
	const char *name;
	void	   *target;
	void	   *replacement;
	void	  **orig;
	bool		installed;
	int64		hits;
	char		note[160];
} PgpiDetour;

extern void pgpi_install_detours(void);
extern void pgpi_detour_reset_state(void);
extern bool pgpi_detour_active(const char *name);
extern int	pgpi_detour_count(void);
extern PgpiDetour *pgpi_detour_get(int i);

/* obs_plan.c */
extern void pgpi_obs_planner_setup(PlannerGlobal *glob);
extern void pgpi_obs_planner_shutdown(PlannerGlobal *glob, Query *parse,
									  PlannedStmt *pstmt);
extern bool pgpi_obs_enabled(void);
extern bool pgpi_obs_sampled(void);
extern const char *pgpi_node_type_name(Plan *plan);
extern const char *pgpi_join_type_name(JoinType jt);

/* obs_exec.c */
extern void pgpi_obs_exec_init(void);

/* obs_shm.c */
typedef enum PgpiRecType
{
	PGPI_REC_PLAN = 1,
	PGPI_REC_STATS = 2
} PgpiRecType;

/* fixed part of a plan record, followed by the string fields */
typedef struct PgpiPlanRecHdr
{
	TimestampTz start_time;
	TimestampTz end_time;
	double		planning_ms;
	double		execution_ms;
	int64		queryid;
	int64		planid;
	int32		pid;
	int32		nested_level;
	int32		status;			/* PgpiPlanStatus */
	int32		encoding;
	Oid			dbid;
	Oid			userid;
} PgpiPlanRecHdr;

typedef enum PgpiPlanStatus
{
	PGPI_ST_COMPLETE = 0,
	PGPI_ST_PARTIAL = 1,
	PGPI_ST_CANCELED = 2
} PgpiPlanStatus;

/* string fields of a plan record, in this order */
enum
{
	PGPI_PF_DATABASE, PGPI_PF_USER, PGPI_PF_LABEL, PGPI_PF_QUERY,
	PGPI_PF_PARAMS, PGPI_PF_EXPLAIN, PGPI_PF_NODES,
	PGPI_PF_META, PGPI_PF_NFIELDS
};

typedef struct PgpiStatsRecHdr
{
	TimestampTz captured_at;
	int64		version;
	Oid			dbid;
	Oid			relid;
	int32		encoding;
} PgpiStatsRecHdr;

enum
{
	PGPI_SF_DATABASE, PGPI_SF_REL, PGPI_SF_STATS, PGPI_SF_NFIELDS
};

extern bool pgpi_shm_available(void);
extern void pgpi_shm_register(void);
extern bool pgpi_ring_put(int type, const void *hdr, Size hdrlen,
						  const char *const *fields, int nfields);
extern Size pgpi_ring_take(char *buf, Size bufsize, uint64 *endpos);
extern void pgpi_ring_mark_committed(uint64 endpos);
extern void pgpi_ring_set_worker(void);
extern bool pgpi_stats_seen(Oid dbid, Oid relid, int64 version, bool insert);
extern void pgpi_stats_seen_reset(void);

/* obs_worker.c */
extern void pgpi_register_worker(void);
extern PGDLLEXPORT void pgpi_worker_main(Datum main_arg);

/* stats.c */
extern int64 pgpi_stats_version(Oid relid);
extern char *pgpi_stats_capture(Oid relid);

/* utility */
extern void pgpi_append_int64(StringInfo buf, int64 v);
extern void pgpi_append_double(StringInfo buf, double v);

#endif							/* PGPI_H */
