/*-------------------------------------------------------------------------
 * pgpi.c
 *	  Module entry point: GUCs, route H (PostgreSQL 19 planner hooks) and
 *	  installation of route D (detours).
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/queryjumble.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "utils/guc.h"

#include "pgpi.h"

PG_MODULE_MAGIC_EXT(.name = "pgpi", .version = "0.2");

/* GUC variables */
int			pgpi_mode = PGPI_MODE_OFF;
bool		pgpi_enable_hook = true;
bool		pgpi_enable_detour = true;
bool		pgpi_detour_clauselist = true;
bool		pgpi_trace = false;
char	   *pgpi_directives = NULL;
char	   *pgpi_callback = NULL;
int			pgpi_callback_timeout = 2000;
int			pgpi_log_size = 1000;

/* observation */
bool		pgpi_log_plans = false;
int			pgpi_log_min_duration = 0;
double		pgpi_log_sample_rate = 1.0;
bool		pgpi_log_nested = false;
bool		pgpi_log_explain = true;
int			pgpi_log_requests = PGPI_LOGREQ_MATCHED;
bool		pgpi_log_stats = true;
bool		pgpi_log_parameters = true;
bool		pgpi_log_canceled = true;
char	   *pgpi_label = NULL;
char	   *pgpi_hints = NULL;
char	   *pgpi_log_database = NULL;
int			pgpi_log_buffer_kb = 16384;
int			pgpi_log_retention = 0;
int			pgpi_log_max_rows = 0;
int			pgpi_stats_max = 10000;

static const struct config_enum_entry log_requests_options[] = {
	{"none", PGPI_LOGREQ_NONE, false},
	{"matched", PGPI_LOGREQ_MATCHED, false},
	{"all", PGPI_LOGREQ_ALL, false},
	{NULL, 0, false}
};

static const struct config_enum_entry mode_options[] = {
	{"off", PGPI_MODE_OFF, false},
	{"shadow", PGPI_MODE_SHADOW, false},
	{"enforce", PGPI_MODE_ENFORCE, false},
	{NULL, 0, false}
};

static planner_setup_hook_type prev_planner_setup = NULL;
static planner_shutdown_hook_type prev_planner_shutdown = NULL;
static joinrel_setup_hook_type prev_joinrel_setup = NULL;
static set_rel_pathlist_hook_type prev_set_rel_pathlist = NULL;
static create_upper_paths_hook_type prev_create_upper_paths = NULL;

/* Remember every query level, including pure set operations. */
static void
pgpi_create_upper_paths(PlannerInfo *root, UpperRelationKind stage,
						RelOptInfo *input_rel, RelOptInfo *output_rel,
						void *extra)
{
	if (prev_create_upper_paths)
		prev_create_upper_paths(root, stage, input_rel, output_rel, extra);
	pgpi_note_root(root);
}

/* ---------- planning scope ---------- */

static void
pgpi_planner_setup(PlannerGlobal *glob, Query *parse, const char *query_string,
				   int cursorOptions, double *tuple_fraction, ExplainState *es)
{
	pgpi_planning_begin(query_string);
	pgpi_obs_planner_setup(glob);
	if (prev_planner_setup)
		prev_planner_setup(glob, parse, query_string, cursorOptions,
						   tuple_fraction, es);
}

static void
pgpi_planner_shutdown(PlannerGlobal *glob, Query *parse, const char *query_string,
					  PlannedStmt *pstmt)
{
	/* after the hooks of the other extensions, so that pstmt is final */
	if (prev_planner_shutdown)
		prev_planner_shutdown(glob, parse, query_string, pstmt);
	pgpi_obs_planner_shutdown(glob, parse, pstmt);
	pgpi_planning_end();
}

/* ---------- route H: join relations ---------- */

static void
pgpi_joinrel_setup(PlannerInfo *root, RelOptInfo *joinrel,
				   RelOptInfo *outer_rel, RelOptInfo *inner_rel,
				   SpecialJoinInfo *sjinfo, List *restrictlist)
{
	if (prev_joinrel_setup)
		prev_joinrel_setup(root, joinrel, outer_rel, inner_rel, sjinfo,
						   restrictlist);

	pgpi_note_root(root);
	pgpi_record_joinrel(root, joinrel, outer_rel, inner_rel, restrictlist);

	if (pgpi_mode != PGPI_MODE_OFF && pgpi_enable_hook &&
		!pgpi_detour_active("set_joinrel_size_estimates"))
	{
		PgpiRequest req = {0};

		req.point = PGPI_PT_JOIN;
		req.route = 'H';
		req.root = root;
		req.relids = joinrel->relids;
		req.split = outer_rel->relids;
		req.outer_rows = outer_rel->rows;
		req.inner_rows = inner_rel->rows;
		req.clauses = pgpi_all_clauses(root, joinrel);
		req.jointype = sjinfo ? sjinfo->jointype : JOIN_INNER;
		req.pg_value = joinrel->rows;
		joinrel->rows = pgpi_intervene(&req);
	}
}

/* ---------- route H: base relations (after path generation) ---------- */

static double
parallel_divisor(Path *path)
{
	double		divisor = path->parallel_workers;

	if (parallel_leader_participation)
	{
		double		leader = 1.0 - (0.3 * path->parallel_workers);

		if (leader > 0)
			divisor += leader;
	}
	return divisor;
}

static void
pgpi_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti,
					  RangeTblEntry *rte)
{
	if (prev_set_rel_pathlist)
		prev_set_rel_pathlist(root, rel, rti, rte);

	pgpi_note_root(root);

	if (pgpi_mode != PGPI_MODE_OFF && pgpi_enable_hook &&
		!pgpi_detour_active("set_baserel_size_estimates") &&
		pgpi_is_plain_baserel(root, rel))
	{
		PgpiRequest req = {0};
		double		rows;
		ListCell   *lc;

		req.point = PGPI_PT_BASE;
		req.route = 'H';
		req.root = root;
		req.relids = rel->relids;
		req.clauses = rel->baserestrictinfo;
		req.jointype = JOIN_INNER;
		req.pg_value = rel->rows;
		req.tuples = rel->tuples;
		rows = pgpi_intervene(&req);
		if (rows == rel->rows)
			return;

		rel->rows = rows;
		foreach(lc, rel->pathlist)
		{
			Path	   *path = (Path *) lfirst(lc);

			if (path->param_info == NULL)
				path->rows = rows;
		}
		foreach(lc, rel->partial_pathlist)
		{
			Path	   *path = (Path *) lfirst(lc);

			if (path->param_info == NULL && path->parallel_workers > 0)
				path->rows = clamp_row_est(rows / parallel_divisor(path));
		}
	}
}

/* ---------- module init ---------- */

void
_PG_init(void)
{
	DefineCustomEnumVariable("pgpi.mode",
							 "off: do nothing; shadow: ask and log only; enforce: apply answers.",
							 NULL, &pgpi_mode, PGPI_MODE_OFF, mode_options,
							 PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("pgpi.enable_hook", "Use route H (planner hooks).",
							 NULL, &pgpi_enable_hook, true,
							 PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("pgpi.enable_detour", "Use route D (function detours) where installed.",
							 NULL, &pgpi_enable_detour, true,
							 PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("pgpi.detour_clauselist",
							 "Route D: also intercept single-relation clauselist_selectivity_ext calls.",
							 NULL, &pgpi_detour_clauselist, true,
							 PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("pgpi.trace", "Report every request as a NOTICE.",
							 NULL, &pgpi_trace, false,
							 PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomStringVariable("pgpi.directives",
							   "Static answers: ROWS(a b)=N; SCALE(a)=F; PROWS(t | o)=N; PSCALE(t | o)=F; SEL(t | o)=S",
							   NULL, &pgpi_directives, "",
							   PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomStringVariable("pgpi.callback",
							   "Unix socket path of the external estimator server (empty: none).",
							   NULL, &pgpi_callback, "",
							   PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("pgpi.callback_timeout",
							"Timeout for one estimator round trip.",
							NULL, &pgpi_callback_timeout, 2000, 10, 600000,
							PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);
	DefineCustomIntVariable("pgpi.log_size",
							"Number of requests kept in the per-backend log.",
							NULL, &pgpi_log_size, 1000, 1, 1000000,
							PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("pgpi.log_plans",
							 "Log every executed plan with per-node descriptors and actual rows.",
							 NULL, &pgpi_log_plans, false,
							 PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("pgpi.log_min_duration",
							"Log only queries that ran at least this long.",
							NULL, &pgpi_log_min_duration, 0, 0, INT_MAX,
							PGC_SUSET, GUC_UNIT_MS, NULL, NULL, NULL);
	DefineCustomRealVariable("pgpi.log_sample_rate",
							 "Fraction of client statements to log (decided per statement).",
							 NULL, &pgpi_log_sample_rate, 1.0, 0.0, 1.0,
							 PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("pgpi.log_nested", "Also log statements executed inside functions.",
							 NULL, &pgpi_log_nested, false,
							 PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("pgpi.log_explain",
							 "Store EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) with each plan.",
							 NULL, &pgpi_log_explain, true,
							 PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomEnumVariable("pgpi.log_requests",
							 "Which intervention requests of the planning cycle to store with a plan.",
							 NULL, &pgpi_log_requests, PGPI_LOGREQ_MATCHED,
							 log_requests_options, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("pgpi.log_stats",
							 "Record statistics versions and capture each version's contents once.",
							 NULL, &pgpi_log_stats, true,
							 PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("pgpi.log_parameters", "Store bound parameter values.",
							 NULL, &pgpi_log_parameters, true,
							 PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("pgpi.log_canceled",
							 "Also log queries canceled by statement_timeout or a cancel request.",
							 NULL, &pgpi_log_canceled, true,
							 PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomStringVariable("pgpi.label",
							   "Free-form label stored with requests and logged plans.",
							   NULL, &pgpi_label, "",
							   PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomStringVariable("pgpi.hints",
							   "Free-form hints passed to the estimator with every request (e.g. model assumptions).",
							   NULL, &pgpi_hints, "",
							   PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomStringVariable("pgpi.log_database",
							   "Database in which the log writer stores the pgpi.* tables.",
							   NULL, &pgpi_log_database, "postgres",
							   PGC_POSTMASTER, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("pgpi.log_buffer_size",
							"Size of the shared log ring.",
							NULL, &pgpi_log_buffer_kb, 16384, 1024, 1048576,
							PGC_POSTMASTER, GUC_UNIT_KB, NULL, NULL, NULL);
	DefineCustomIntVariable("pgpi.log_retention",
							"Delete logged plans older than this (0: keep).",
							NULL, &pgpi_log_retention, 0, 0, INT_MAX,
							PGC_SIGHUP, GUC_UNIT_MIN, NULL, NULL, NULL);
	DefineCustomIntVariable("pgpi.log_max_rows",
							"Keep at most this many logged plans (0: no limit).",
							NULL, &pgpi_log_max_rows, 0, 0, INT_MAX,
							PGC_SIGHUP, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("pgpi.stats_max",
							"Number of statistics versions remembered as already captured.",
							NULL, &pgpi_stats_max, 10000, 100, 10000000,
							PGC_POSTMASTER, 0, NULL, NULL, NULL);
	MarkGUCPrefixReserved("pgpi");

	pgpi_core_init();

	if (process_shared_preload_libraries_in_progress)
	{
		pgpi_shm_register();
		pgpi_register_worker();
		EnableQueryId();
	}

	prev_planner_setup = planner_setup_hook;
	planner_setup_hook = pgpi_planner_setup;
	prev_planner_shutdown = planner_shutdown_hook;
	planner_shutdown_hook = pgpi_planner_shutdown;
	prev_joinrel_setup = joinrel_setup_hook;
	joinrel_setup_hook = pgpi_joinrel_setup;
	prev_set_rel_pathlist = set_rel_pathlist_hook;
	set_rel_pathlist_hook = pgpi_set_rel_pathlist;
	prev_create_upper_paths = create_upper_paths_hook;
	create_upper_paths_hook = pgpi_create_upper_paths;

	pgpi_obs_exec_init();
	pgpi_install_detours();
}
