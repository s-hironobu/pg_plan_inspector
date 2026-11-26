/*-------------------------------------------------------------------------
 * obs_worker.c
 *	  The pgpi log writer: a background worker that drains the log ring
 *	  into tables of the log database (pgpi.log_database). It also applies
 *	  the retention settings.
 *
 * Tables (schema pgpi, created on first start):
 *   pgpi.plans  one row per executed (or canceled) query
 *   pgpi.stats  one row per (database, relation, statistics version)
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "pgpi.h"

static const char *ddl =
"CREATE SCHEMA IF NOT EXISTS pgpi;"
"CREATE TABLE IF NOT EXISTS pgpi.plans ("
"  seqid        bigserial PRIMARY KEY,"
"  logged_at    timestamptz NOT NULL DEFAULT now(),"
"  start_time   timestamptz,"
"  end_time     timestamptz,"
"  planning_ms  float8,"
"  execution_ms float8,"
"  dbid         oid,"
"  database     name,"
"  username     name,"
"  pid          int,"
"  nested_level int,"
"  queryid      bigint,"
"  planid       bigint,"
"  status       text,"
"  label        text,"
"  query        text,"
"  params       text,"
"  explain      json,"
"  nodes        jsonb,"
"  meta         jsonb);"
"CREATE INDEX IF NOT EXISTS plans_query_idx ON pgpi.plans (queryid, planid);"
"CREATE INDEX IF NOT EXISTS plans_logged_idx ON pgpi.plans (logged_at);"
"CREATE INDEX IF NOT EXISTS plans_label_idx ON pgpi.plans (label);"
"CREATE TABLE IF NOT EXISTS pgpi.stats ("
"  dbid         oid,"
"  database     name,"
"  relid        oid,"
"  rel          text,"
"  version      bigint,"
"  captured_at  timestamptz,"
"  stats        jsonb,"
"  PRIMARY KEY (dbid, relid, version));";

static SPIPlanPtr plan_insert = NULL;
static SPIPlanPtr stats_insert = NULL;

void
pgpi_register_worker(void)
{
	BackgroundWorker w;

	memset(&w, 0, sizeof(w));
	w.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	w.bgw_start_time = BgWorkerStart_RecoveryFinished;
	w.bgw_restart_time = 10;
	strlcpy(w.bgw_library_name, "pgpi", sizeof(w.bgw_library_name));
	strlcpy(w.bgw_function_name, "pgpi_worker_main", sizeof(w.bgw_function_name));
	strlcpy(w.bgw_name, "pgpi log writer", sizeof(w.bgw_name));
	strlcpy(w.bgw_type, "pgpi log writer", sizeof(w.bgw_type));
	RegisterBackgroundWorker(&w);
}

static void
prepare_statements(void)
{
	Oid			ptypes[18] = {TIMESTAMPTZOID, TIMESTAMPTZOID, FLOAT8OID, FLOAT8OID,
		OIDOID, TEXTOID, TEXTOID, INT4OID, INT4OID, INT8OID, INT8OID, TEXTOID,
	TEXTOID, TEXTOID, TEXTOID, TEXTOID, TEXTOID, TEXTOID};
	Oid			stypes[7] = {OIDOID, TEXTOID, OIDOID, TEXTOID, INT8OID,
	TIMESTAMPTZOID, TEXTOID};

	plan_insert = SPI_prepare("INSERT INTO pgpi.plans (start_time, end_time, "
							  "planning_ms, execution_ms, dbid, database, username, pid, "
							  "nested_level, queryid, planid, status, label, query, params, "
							  "explain, nodes, meta) VALUES ($1, $2, $3, $4, "
							  "$5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, "
							  "$16::json, $17::jsonb, $18::jsonb)", 18, ptypes);
	if (plan_insert == NULL)
		elog(ERROR, "pgpi: SPI_prepare failed: %s", SPI_result_code_string(SPI_result));
	SPI_keepplan(plan_insert);
	stats_insert = SPI_prepare("INSERT INTO pgpi.stats (dbid, database, relid, rel, "
							   "version, captured_at, stats) VALUES ($1, $2, $3, $4, $5, "
							   "$6, $7::jsonb) ON CONFLICT DO NOTHING", 7, stypes);
	if (stats_insert == NULL)
		elog(ERROR, "pgpi: SPI_prepare failed: %s", SPI_result_code_string(SPI_result));
	SPI_keepplan(stats_insert);
}

static void
setup_tables(void)
{
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "pgpi: creating log tables");
	if (SPI_execute(ddl, false, 0) < 0)
		elog(ERROR, "pgpi: could not create the log tables");
	prepare_statements();
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

/* ---------- decoding ---------- */

typedef struct RecView
{
	int			type;
	const char *hdr;
	uint32		hdrlen;
	int			nfields;
	const char *field[16];
	uint32		len[16];		/* PG_UINT32_MAX = NULL */
} RecView;

static bool
decode(const char *rec, RecView *rv)
{
	const char *p = rec + 4;
	uint32		u;

	memcpy(&u, p, 4);
	p += 4;
	rv->type = (int) u;
	memcpy(&rv->hdrlen, p, 4);
	p += 4;
	rv->hdr = p;
	p += rv->hdrlen;
	memcpy(&u, p, 4);
	p += 4;
	if (u > 16)
		return false;
	rv->nfields = (int) u;
	for (int i = 0; i < rv->nfields; i++)
	{
		memcpy(&rv->len[i], p, 4);
		p += 4;
		rv->field[i] = p;
		if (rv->len[i] != PG_UINT32_MAX)
			p += rv->len[i];
	}
	return true;
}

static Datum
text_arg(RecView *rv, int i, int encoding, char *isnull)
{
	char	   *s;
	char	   *conv;

	if (i >= rv->nfields || rv->len[i] == PG_UINT32_MAX)
	{
		*isnull = 'n';
		return (Datum) 0;
	}
	*isnull = ' ';
	s = pnstrdup(rv->field[i], rv->len[i]);
	conv = (char *) pg_do_encoding_conversion((unsigned char *) s, rv->len[i],
											  encoding, GetDatabaseEncoding());
	return CStringGetTextDatum(conv);
}

static const char *
status_name(int st)
{
	switch (st)
	{
		case PGPI_ST_COMPLETE:
			return "complete";
		case PGPI_ST_PARTIAL:
			return "partial";
		case PGPI_ST_CANCELED:
			return "canceled";
		default:
			return "unknown";
	}
}

static void
store_one(RecView *rv, bool drop_explain)
{
	if (rv->type == PGPI_REC_PLAN && rv->hdrlen == sizeof(PgpiPlanRecHdr))
	{
		PgpiPlanRecHdr h;
		Datum		v[18];
		char		n[19];

		memcpy(&h, rv->hdr, sizeof(h));
		memset(n, ' ', sizeof(n));
		n[18] = '\0';
		v[0] = TimestampTzGetDatum(h.start_time);
		v[1] = TimestampTzGetDatum(h.end_time);
		v[2] = Float8GetDatum(h.planning_ms);
		v[3] = Float8GetDatum(h.execution_ms);
		v[4] = ObjectIdGetDatum(h.dbid);
		v[5] = text_arg(rv, PGPI_PF_DATABASE, h.encoding, &n[5]);
		v[6] = text_arg(rv, PGPI_PF_USER, h.encoding, &n[6]);
		v[7] = Int32GetDatum(h.pid);
		v[8] = Int32GetDatum(h.nested_level);
		v[9] = Int64GetDatum(h.queryid);
		v[10] = Int64GetDatum(h.planid);
		v[11] = CStringGetTextDatum(status_name(h.status));
		v[12] = text_arg(rv, PGPI_PF_LABEL, h.encoding, &n[12]);
		v[13] = text_arg(rv, PGPI_PF_QUERY, h.encoding, &n[13]);
		v[14] = text_arg(rv, PGPI_PF_PARAMS, h.encoding, &n[14]);
		v[15] = text_arg(rv, PGPI_PF_EXPLAIN, h.encoding, &n[15]);
		if (drop_explain)
			n[15] = 'n';
		v[16] = text_arg(rv, PGPI_PF_NODES, h.encoding, &n[16]);
		v[17] = text_arg(rv, PGPI_PF_META, h.encoding, &n[17]);
		if (h.planning_ms <= 0 && n[17] == 'n')
			n[2] = 'n';
		if (SPI_execute_plan(plan_insert, v, n, false, 0) != SPI_OK_INSERT)
			elog(ERROR, "pgpi: insert into pgpi.plans failed");
	}
	else if (rv->type == PGPI_REC_STATS && rv->hdrlen == sizeof(PgpiStatsRecHdr))
	{
		PgpiStatsRecHdr h;
		Datum		v[7];
		char		n[8];

		memcpy(&h, rv->hdr, sizeof(h));
		memset(n, ' ', sizeof(n));
		n[7] = '\0';
		v[0] = ObjectIdGetDatum(h.dbid);
		v[1] = text_arg(rv, PGPI_SF_DATABASE, h.encoding, &n[1]);
		v[2] = ObjectIdGetDatum(h.relid);
		v[3] = text_arg(rv, PGPI_SF_REL, h.encoding, &n[3]);
		v[4] = Int64GetDatum(h.version);
		v[5] = TimestampTzGetDatum(h.captured_at);
		v[6] = text_arg(rv, PGPI_SF_STATS, h.encoding, &n[6]);
		if (SPI_execute_plan(stats_insert, v, n, false, 0) < 0)
			elog(ERROR, "pgpi: insert into pgpi.stats failed");
	}
}

/*
 * Store one record in its own subtransaction. If the insert fails and the
 * record has an EXPLAIN text, try again without it (core EXPLAIN can
 * emit invalid JSON in rare cases). Otherwise the record is skipped.
 */
static bool
try_store(RecView *rv, bool drop_explain, bool report)
{
	MemoryContext oldcxt = CurrentMemoryContext;
	ResourceOwner oldowner = CurrentResourceOwner;
	bool		ok = true;

	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(oldcxt);
	PG_TRY();
	{
		store_one(rv, drop_explain);
		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcxt);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(oldcxt);
		edata = CopyErrorData();
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcxt);
		CurrentResourceOwner = oldowner;
		if (report)
			ereport(WARNING,
					(errmsg("pgpi: could not store a log record: %s", edata->message)));
		FreeErrorData(edata);
		ok = false;
	}
	PG_END_TRY();
	return ok;
}

static void
store_record(const char *rec)
{
	RecView		rv;
	bool		has_explain;

	if (!decode(rec, &rv))
		return;
	has_explain = rv.type == PGPI_REC_PLAN && rv.nfields > PGPI_PF_EXPLAIN &&
		rv.len[PGPI_PF_EXPLAIN] != PG_UINT32_MAX;
	if (try_store(&rv, false, !has_explain))
		return;
	if (has_explain && try_store(&rv, true, true))
		ereport(LOG,
				(errmsg("pgpi: stored a plan without its EXPLAIN text, which was not valid JSON")));
}

static void
drain(char *buf, Size bufsize)
{
	MemoryContext batch_cxt = AllocSetContextCreate(TopMemoryContext,
													"pgpi batch",
													ALLOCSET_DEFAULT_SIZES);

	for (;;)
	{
		uint64		endpos;
		Size		n = pgpi_ring_take(buf, bufsize, &endpos);
		Size		off = 0;
		MemoryContext old;

		if (n == 0)
			break;
		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();
		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		pgstat_report_activity(STATE_RUNNING, "pgpi: storing log records");
		old = MemoryContextSwitchTo(batch_cxt);
		while (off < n)
		{
			uint32		len;

			memcpy(&len, buf + off, 4);
			if (len == 0)
				break;
			store_record(buf + off);
			off += len;
			MemoryContextReset(batch_cxt);
		}
		MemoryContextSwitchTo(old);
		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
		pgstat_report_activity(STATE_IDLE, NULL);
		pgpi_ring_mark_committed(endpos);
		CHECK_FOR_INTERRUPTS();
	}
	MemoryContextDelete(batch_cxt);
}

static void
apply_retention(void)
{
	StringInfoData sql;
	uint64		deleted = 0;

	if (pgpi_log_retention <= 0 && pgpi_log_max_rows <= 0)
		return;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "pgpi: applying retention");
	initStringInfo(&sql);
	if (pgpi_log_retention > 0)
	{
		appendStringInfo(&sql, "DELETE FROM pgpi.plans WHERE logged_at < now() - "
						 "make_interval(mins => %d)", pgpi_log_retention);
		if (SPI_execute(sql.data, false, 0) == SPI_OK_DELETE)
			deleted += SPI_processed;
		resetStringInfo(&sql);
	}
	if (pgpi_log_max_rows > 0)
	{
		appendStringInfo(&sql, "DELETE FROM pgpi.plans WHERE seqid <= "
						 "(SELECT max(seqid) FROM pgpi.plans) - %d", pgpi_log_max_rows);
		if (SPI_execute(sql.data, false, 0) == SPI_OK_DELETE)
			deleted += SPI_processed;
		resetStringInfo(&sql);
	}
	if (deleted > 0)
	{
		/*
		 * Statistics that no remaining plan refers to. Keep recent captures,
		 * because their plans may still be on their way through the ring.
		 */
		if (SPI_execute("DELETE FROM pgpi.stats s WHERE captured_at < now() - "
						"interval '10 minutes' AND NOT EXISTS (SELECT 1 FROM "
						"pgpi.plans p, jsonb_array_elements(p.meta->'stats_refs') r "
						"WHERE p.dbid = s.dbid AND (r->>'relid')::oid = s.relid AND "
						"(r->>'version')::bigint = s.version)", false, 0) == SPI_OK_DELETE &&
			SPI_processed > 0)
			pgpi_stats_seen_reset();
	}
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

void
pgpi_worker_main(Datum main_arg)
{
	Size		bufsize;
	char	   *buf;
	TimestampTz last_retention = 0;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(pgpi_log_database, NULL, 0);

	/* never observe or intervene in our own statements */
	SetConfigOption("pgpi.log_plans", "off", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("pgpi.mode", "off", PGC_SUSET, PGC_S_OVERRIDE);
	/* a log, not a ledger: no need to wait for WAL flushes */
	SetConfigOption("synchronous_commit", "off", PGC_SUSET, PGC_S_OVERRIDE);

	if (!pgpi_shm_available())
		proc_exit(1);

	setup_tables();
	pgpi_ring_set_worker();

	bufsize = (Size) pgpi_log_buffer_kb * 1024 / 2 + 1024;
	buf = MemoryContextAlloc(TopMemoryContext, bufsize);

	for (;;)
	{
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 1000, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			last_retention = 0;	/* apply new retention settings now */
		}
		/* let a few records accumulate: fewer and larger transactions */
		(void) WaitLatch(MyLatch, WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 50,
						 PG_WAIT_EXTENSION);
		drain(buf, bufsize);
		if (TimestampDifferenceExceeds(last_retention, GetCurrentTimestamp(), 60000))
		{
			apply_retention();
			last_retention = GetCurrentTimestamp();
		}
	}
}
