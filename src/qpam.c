/*-------------------------------------------------------------------------
 * qpam.c
 *
 * This file contains the access methods to read/write queries and [executed|query]
 * plans from/into the queryPlanData structure.
 *
 * The instances of queryPlanData are allocated on two places: the shared memory
 * and the dynamic shared memory on bgworker as the slots of the ring buffer.
 *
 * Copyright (c) 2021-2024, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/explain.h"
#include "common/hashfn.h"
#include "libpq/auth.h"
#include "miscadmin.h"

#include "common.h"
#include "pgqp_explain.h"
#include "planid.h"


char	   *pgqp_json_plan;		/* This is read by
								 * input_params()@planid_scanner.l. */

/* Link to shared memory state */
extern pgqpSharedState * pgqp;

/* Links to QueryDesc and ExplainState */
extern QueryDesc *qp_qd[MAX_NESTED_LEVEL];
extern ExplainState *qp_es[MAX_NESTED_LEVEL];


/*
 * Function declarations
 */
void		set_query(struct queryPlanData *qpd, const int nested_level,
					  const uint64 queryId, const bool nested);
void		set_plan(struct queryPlanData *qpd, const int nested_level,
					 const bool nested, const PrintFormat format);
char	   *get_query_plan(struct queryPlanData *qpd, const PrintFormat format,
						   bool *is_null, const int nested_level);
void		init_qpd(struct queryPlanData *qpd);
void		init_qi(struct queryInfo *qi);

static void set_message(struct queryPlanData *qpd, const char *msg, const int len,
						const int nested_level, const PrintFormat format);


/*
 * Set messages to qpd->queryPlan_XXX[]
 *
 * Internal format:
 * Messages are packed in the qpd->queryPlan_XXX[] array. Each message is
 * terminated by '\0'.
 * The positions of the terminators are recorded in the qpd->terminalByte[]
 * array; the total message length is stored in the qpd->totalLen[].
 *
 * Example:
 * Three massages are packed in the qpd->queryPlan_text:
 *   msg1 = 'abc', msg2 = 'ABCD', msg3 = 'xyz'
 *
 *           byte   0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  ...
 * queryPlan_text | a | b | c |\0 | A | B | C | D |\0 | x | y | z |\0 |   |   |   |
 *
 * In this case,
 * totalLen[1] = 13, terminalByte[1][0] = 3, terminalByte[1][1] = 8,
 * terminalByte[1][2] = 12.
 */
static void
set_message(struct queryPlanData *qpd, const char *msg, const int len,
			const int nested_level, const PrintFormat format)
{
	int			offset = qpd->totalLen[format];
	int			msglen;
	int			max_len = 0;

	Assert(PRINT_QUERY == format
		   || PRINT_PLAN == format
		   || PRINT_PLAN_JSON == format);

	switch (format)
	{
		case PRINT_QUERY:
			max_len = MAX_QUERY_LEN;
			break;
		case PRINT_PLAN:
			max_len = MAX_QUERY_PLAN_LEN;
			break;
		case PRINT_PLAN_JSON:
			max_len = MAX_QUERY_PLAN_JSON_LEN;
			break;
	}

	if ((max_len - 1) <= (offset + len))
	{
		elog(WARNING, "Buffer is already full," \
			 "so, the plans of query cannot be written.");
		return;
	}

	qpd->totalLen[format]
		= ((offset + len) > (max_len - 1))
		? (max_len - 1)
		: (offset + len);
	msglen
		= ((offset + len) > (max_len - 1))
		? max_len - offset
		: len;

	switch (format)
	{
		case PRINT_QUERY:
			memcpy((void *) (qpd->queryPlan_query + offset), msg, msglen);
			qpd->queryPlan_query[qpd->totalLen[format]] = '\0';
			break;
		case PRINT_PLAN:
			memcpy((void *) (qpd->queryPlan_text + offset), msg, msglen);
			qpd->queryPlan_text[qpd->totalLen[format]] = '\0';
			break;
		case PRINT_PLAN_JSON:
			memcpy((void *) (qpd->queryPlan_json + offset), msg, msglen);
			qpd->queryPlan_json[qpd->totalLen[format]] = '\0';
			break;
	}

	/* Assume that nested_level is incremented by 1 from 0. */
	qpd->nested_level[format] = nested_level;
	qpd->terminalByte[format][nested_level] = qpd->totalLen[format];
	qpd->totalLen[format] += 1;

	return;
}


/*
 * Set the query to qpd->queryPlan_query
 */
void
set_query(struct queryPlanData *qpd, const int nested_level,
		  const uint64 queryId, const bool nested)
{
	int			level = (nested == true) ? nested_level : 0;

	/* Set queryId. */
	qpd->queryId[level] = queryId;

	/* Set the query, which is stored in QueryDesc, to qpd->queryPlan_query[]. */
	set_message(qpd,
				qp_qd[nested_level]->sourceText,
				strlen(qp_qd[nested_level]->sourceText),
				level, PRINT_QUERY);
}


/*
 * Set the [executed|query] plan to pgqp->queryPlan_XXX.
 */
void
set_plan(struct queryPlanData *qpd, const int nested_level,
		 const bool nested, const PrintFormat format)
{
	int			level = (nested == true) ? nested_level : 0;
	bool		_analyze = qp_es[nested_level]->analyze;
	bool		_verbose = qp_es[nested_level]->verbose;

	Assert(format == PRINT_PLAN || format == PRINT_PLAN_JSON);

	/*
	 * Set analyze and verbose options, and format. Ignore buffers and wal
	 * options.
	 */
	qp_es[nested_level]->analyze = true;
	qp_es[nested_level]->verbose = true;
	qp_es[nested_level]->format
		= (format == PRINT_PLAN)
		? EXPLAIN_FORMAT_TEXT
		: EXPLAIN_FORMAT_JSON;

	resetStringInfo(qp_es[nested_level]->str);

	/*
	 * Get the [executed|query] plan state in the specified format and the
	 * specified nested_level.
	 */
	pgqpExplainBeginOutput(qp_es[nested_level]);
	pgqpExplainPrintQueryState(qp_es[nested_level], qp_qd[nested_level]);
	pgqpExplainEndOutput(qp_es[nested_level]);

	/*
	 * Set the getting [executed|query] plan to qpd->queryPlan_plan[] or
	 * qpd->queryPlan_json[].
	 */
	set_message(qpd,
				qp_es[nested_level]->str->data,
				qp_es[nested_level]->str->len,
				level,
				format);

	/*
	 * Set planId
	 */
	if (format == PRINT_PLAN_JSON)
	{
		uint64		planid;

		pgqp_json_plan = qp_es[nested_level]->str->data;
		pre_plan_parse(qp_es[nested_level]->str->len);
		if (plan_parse() != 0)
			elog(WARNING, "Warning: Parse error in the json plan.");
		planid = get_planId();
		qpd->planId[nested_level] = planid;

#ifdef __DEBUG__
		elog(LOG, "planid = %lu  plan=%s", planid, qp_es[nested_level]->str->data);
#endif
	}

	qp_es[nested_level]->analyze = _analyze;
	qp_es[nested_level]->verbose = _verbose;
}


/*
 * Initialize qpd
 */
void
init_qpd(struct queryPlanData *qpd)
{
	int			i,
				j;

	for (i = PRINT_QUERY; i <= PRINT_PLAN_JSON; i++)
	{
		switch (i)
		{
			case PRINT_QUERY:
				qpd->queryPlan_query[0] = '\0';
				break;
			case PRINT_PLAN:
				qpd->queryPlan_text[0] = '\0';
				break;
			case PRINT_PLAN_JSON:
				qpd->queryPlan_json[0] = '\0';
				break;
		};
		qpd->totalLen[i] = 0;
		qpd->nested_level[i] = -1;
		for (j = 0; j < MAX_NESTED_LEVEL; j++)
			qpd->terminalByte[i][j] = 0;
	}

	for (i = 0; i < MAX_NESTED_LEVEL; i++)
	{
		qpd->queryId[i] = 0;
		qpd->planId[i] = 0;
	}

	qpd->pid = InvalidPid;
}


/*
 * Initialize queryInfo qi
 */
void
init_qi(struct queryInfo *qi)
{
	qi->database_name[0] = '\0';
	qi->user_name[0] = '\0';
	qi->starttime = 0;
	qi->endtime = 0;
}


/*
 * Extract the appropriate data from qpd->queryPlan_XXX.
 *
 * The data format of qpd->queyPlan_XXX is described in the comment
 * of set_message() function.
 */
char *
get_query_plan(struct queryPlanData *qpd, const PrintFormat format,
			   bool *is_null, const int nested_level)
{
	int			offset,
				len;
	char	   *pstr = NULL;

	Assert(PRINT_QUERY == format
		   || PRINT_PLAN == format
		   || PRINT_PLAN_JSON == format);

	switch (format)
	{
		case PRINT_QUERY:
			pstr = qpd->queryPlan_query;
			break;
		case PRINT_PLAN:
			pstr = qpd->queryPlan_text;
			break;
		case PRINT_PLAN_JSON:
			pstr = qpd->queryPlan_json;
			break;
	};

	offset = (nested_level == 0)
		? 0
		: qpd->terminalByte[format][nested_level - 1] + 1;
	len = (nested_level == 0)
		? qpd->terminalByte[format][0]
		: qpd->terminalByte[format][nested_level] - qpd->terminalByte[format][nested_level - 1];

	if (len > 0)
	{
		*is_null = false;
		return (char *) pg_do_encoding_conversion(
												  (unsigned char *) (pstr + offset),
												  len,
												  qpd->encoding,
												  GetDatabaseEncoding());
	}
	else
		*is_null = true;

	return NULL;
}
