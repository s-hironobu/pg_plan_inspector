/*-------------------------------------------------------------------------
 * bgworker.c
 *
 * The main roles of the bgworker are to create the log table and the
 * pg_query_plan function when this module is run for the first time, and
 * to manage the ring buffer.
 *
 * Whenever the query processing is completed, the leader process writes
 * the query strings and executed plans into a slot of the ring buffer.
 *
 * the bgworker periodically sweeps the ring buffer, and inserts the data in
 * buffer slots into the query_plan.log table.
 *
 * Although the bgworker runs even if it is in standby mode, it does nothing
 * practically. This is because PostgreSQL in standby mode cannot write
 * to tables anything.
 *
 * I am thinking to add a feature that the bgworker writes the log data to
 * the message broker systems such as Kafka. However, the specifics
 * such as the schedule have not been decided yet.
 *
 * Copyright (c) 2021-2024, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "pgstat.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

#include "common.h"
#include "buffer.h"
#include "bgworker.h"
#include "hash.h"

#define GC_INTERVAL 10			/* A constant to control how often GC(Garbage
								 * Collection) is performed to the hash table.
								 * More precisely, whenever the sweepBuffer()
								 * function is executed GC_INTERVAL times, GC
								 * is executed once. */

/*
 * Global variable declarations
 */
extern pgqpSharedState * pgqp;

/*
 * Function declarations
 */
void		pg_query_plan_main(Datum) pg_attribute_noreturn();
#if PG_VERSION_NUM >= 160000
PGDLLEXPORT void		pg_query_plan_main(Datum main_arg);
#else
void		pg_query_plan_main(Datum main_arg);
#endif
void		start_tx(void);
void		commit_tx(void);

static void pg_query_plan_sigterm(SIGNAL_ARGS);
static void pg_query_plan_sighup(SIGNAL_ARGS);

static void initialize_pg_query_plan(void);

/*
 * Flags set by signal handlers
 */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

static int	gc_interval;		/* A counter variable to control how often GC
								 * is performed. */
static bool recoveryInProgress;

/*
 * Signal handler for SIGTERM
 */
static void
pg_query_plan_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(pgqp->bgLatch);
	errno = save_errno;
}


/*
 * Signal handler for SIGHUP
 */
static void
pg_query_plan_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(pgqp->bgLatch);
	errno = save_errno;
}


/*
 * Helper function to start/commit transaction.
 */
void
start_tx(void)
{
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
}


void
commit_tx(void)
{
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_stat(false);
	pgstat_report_activity(STATE_IDLE, NULL);
}


/*
 * Initialize the workspace for a bgworker process, i.e. create the schema
 * if it doesn't exist yet.
 */
static void
initialize_pg_query_plan(void)
{
	int			ret;
	int			ntup;
	bool		isnull;
	StringInfoData buf;

	start_tx();

	/*
	 * Check whether the query_plan schema exists or not.
	 */
	initStringInfo(&buf);
	appendStringInfo(&buf, "SELECT count(*) FROM pg_namespace WHERE nspname = %s;",
					 quote_literal_cstr(SCHEMA));

	pgstat_report_activity(STATE_RUNNING, "initializing schema");
	SetCurrentStatementStartTimestamp();
	ret = SPI_execute(buf.data, true, 0);

	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	if (SPI_processed != 1)
		elog(FATAL, "not a singleton result");

	ntup = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc,
									   1, &isnull));

	if (isnull)
		elog(FATAL, "null result");

	/*
	 * Create query_plan schema, log table and a function if the schema not
	 * found.
	 */
	if (ntup == 0)
	{
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "CREATE SCHEMA IF NOT EXISTS %s;",
						 SCHEMA);
		appendStringInfo(&buf,
						 "GRANT ALL ON SCHEMA %s TO PUBLIC;",
						 SCHEMA);

		pgstat_report_activity(STATE_RUNNING, buf.data);
		SetCurrentStatementStartTimestamp();

		ret = SPI_execute(buf.data, false, 0);

		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "failed to create a schema");

		/* Create table */
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "CREATE TABLE %s.%s ("
						 "       seqid 		    BIGSERIAL PRIMARY KEY,"
						 "       starttime 	    TIMESTAMP WITH TIME ZONE,"
						 "       endtime 	    TIMESTAMP WITH TIME ZONE,"
						 "       database       TEXT,"
						 "       pid            INT,"
						 "       nested_level	INT,"
						 "       queryid		TEXT,"
						 "       query		    TEXT,"
						 "       planid		    TEXT,"
						 "       plan		    TEXT,"
						 "       plan_json	    TEXT"
						 ");",
						 SCHEMA, LOG_TABLE
			);
		appendStringInfo(&buf,
						 "COMMENT ON COLUMN %s.%s.queryid IS \'"
						 "Although a queryid is internally treated as uint64, "
						 "we store the queryid into the text column\n"
						 "because Postgres only supports int64 (bigint).\';",
						 SCHEMA, LOG_TABLE
			);
		appendStringInfo(&buf,
						 "REVOKE ALL ON %s.%s FROM PUBLIC;",
						 SCHEMA, LOG_TABLE
			);
		appendStringInfo(&buf,
						 "ALTER TABLE %s.%s ALTER COLUMN query     SET STORAGE EXTENDED;"
						 "ALTER TABLE %s.%s ALTER COLUMN plan      SET STORAGE EXTENDED;"
						 "ALTER TABLE %s.%s ALTER COLUMN plan_json SET STORAGE EXTENDED;",
						 SCHEMA, LOG_TABLE,
						 SCHEMA, LOG_TABLE,
						 SCHEMA, LOG_TABLE
			);

		pgstat_report_activity(STATE_RUNNING, buf.data);
		SetCurrentStatementStartTimestamp();
		ret = SPI_execute(buf.data, false, 0);
		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "failed to create table %s", LOG_TABLE);

		/* Create function */
		resetStringInfo(&buf);
		appendStringInfo(&buf,
						 "CREATE OR REPLACE FUNCTION %s.pg_query_plan("
						 "    IN  pid          INT,"
						 "    OUT pid          INT,"
						 "    OUT database     TEXT,"
						 "    OUT worker_type  TEXT,"
						 "    OUT nested_level INT,"
						 "    OUT queryid      TEXT,"
						 "    OUT query_start  TIMESTAMP WITH TIME ZONE,"
						 "    OUT query        TEXT,"
						 "    OUT planid       TEXT,"
						 "    OUT plan         TEXT,"
						 "    OUT plan_json    TEXT"
						 ")"
						 "  RETURNS SETOF record"
						 "  AS \'pg_query_plan\'"
						 "LANGUAGE C;",
						 "PUBLIC"
			);

		appendStringInfo(&buf,
						 "CREATE OR REPLACE FUNCTION %s.get_planid("
						 "    IN  plan_json    TEXT"
						 ")"
						 "  RETURNS TEXT"
						 "  AS \'pg_query_plan\'"
						 "LANGUAGE C;",
						 SCHEMA
			);

		pgstat_report_activity(STATE_RUNNING, buf.data);
		SetCurrentStatementStartTimestamp();
		ret = SPI_execute(buf.data, false, 0);
		if (ret != SPI_OK_UTILITY)
			elog(FATAL, "failed to create function.");
	}

	commit_tx();
}


/*
 * bgworker main function
 */
void
pg_query_plan_main(Datum main_arg)
{
	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGHUP, pg_query_plan_sighup);
	pqsignal(SIGTERM, pg_query_plan_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Set latch */
	pgqp->bgLatch = MyLatch;

	recoveryInProgress = RecoveryInProgress();
	if (!recoveryInProgress)
	{
		/* Connect to postgres database */
		BackgroundWorkerInitializeConnection("postgres", NULL, 0);

		/* Initialize workspace for a worker process */
		initialize_pg_query_plan();
	}

	/* Create ringBuffer on dynamic shared memory */
	create_ring_buffer();

	/* Set my pid */
	pgqp->bgworkerPid = MyProcPid;

	/* Initialize gc_interval */
	gc_interval = 0;

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate.
	 */
	while (!got_sigterm)
	{
		int			rc = WaitLatch(pgqp->bgLatch,
								   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
								   BGWORKER_NAPTIME * 1000L,
								   PG_WAIT_EXTENSION);

		ResetLatch(pgqp->bgLatch);

		/* Emergency bailout if postmaster has died. */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		CHECK_FOR_INTERRUPTS();

		/* In case of a SIGHUP, just reload the configuration. */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * Insert the executed plan data stored in the ring buffer into the
		 * query_plan.log table.
		 */
		sweepBuffer(recoveryInProgress);

		/*
		 * Garbage collection for the hash table
		 */
		if (++gc_interval > GC_INTERVAL)
		{
			gc_hashtable();
			gc_interval = 0;
		}
	}

	proc_exit(1);
}
