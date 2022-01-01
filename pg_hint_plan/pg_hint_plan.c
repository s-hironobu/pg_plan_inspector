/*
 * This file is a part of pg_hint_plan.c https://github.com/ossc-db/pg_hint_plan
 */

/*-------------------------------------------------------------------------
 *
 * pg_hint_plan.c
 *		  hinting on how to execute a query for PostgreSQL
 *
 * Copyright (c) 2012-2022, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

static int	get_current_join_mask(void);
static int	set_config_option_noerror(const char *name, const char *value,
									  GucContext context, GucSource source,
									  GucAction action, bool changeVal, int elevel);

/*
 * set GUC parameter functions
 */
static int
get_current_join_mask(void)
{
	int			mask = 0;

	if (enable_nestloop)
		mask |= ENABLE_NESTLOOP;
	if (enable_mergejoin)
		mask |= ENABLE_MERGEJOIN;
	if (enable_hashjoin)
		mask |= ENABLE_HASHJOIN;
	if (enable_memoize)
		mask |= ENABLE_MEMOIZE;

	return mask;
}

/*
 * Sets GUC prameters without throwing exception. Reutrns false if something
 * wrong.
 *
 */
static int
set_config_option_noerror(const char *name, const char *value,
						  GucContext context, GucSource source,
						  GucAction action, bool changeVal, int elevel)
{
	int			result = 0;
	MemoryContext ccxt = CurrentMemoryContext;

	PG_TRY();
	{
		result = set_config_option(name, value, context, source,
								   action, changeVal, 0, false);
	}
	PG_CATCH();
	{
		ErrorData  *errdata;

		/* Save error info */
		MemoryContextSwitchTo(ccxt);
		errdata = CopyErrorData();
		FlushErrorState();

		ereport(elevel,
				(errcode(errdata->sqlerrcode),
				 errmsg("%s", errdata->message),
				 errdata->detail ? errdetail("%s", errdata->detail) : 0,
				 errdata->hint ? errhint("%s", errdata->hint) : 0));
		msgqno = qno;
		FreeErrorData(errdata);
	}
	PG_END_TRY();

	return result;
}

#define SET_CONFIG_OPTION(name, type_bits) \
	set_config_option_noerror((name), \
		(mask & (type_bits)) ? "true" : "false", \
		context, PGC_S_SESSION, GUC_ACTION_SAVE, true, ERROR)


void
set_join_config_options(unsigned char enforce_mask, bool set_memoize,
						GucContext context)
{
	unsigned char mask;

	if (enforce_mask == ENABLE_NESTLOOP || enforce_mask == ENABLE_MERGEJOIN ||
		enforce_mask == ENABLE_HASHJOIN)
		mask = enforce_mask;
	else
		mask = enforce_mask & current_state.init_join_mask;

	SET_CONFIG_OPTION("enable_nestloop", ENABLE_NESTLOOP);
	SET_CONFIG_OPTION("enable_mergejoin", ENABLE_MERGEJOIN);
	SET_CONFIG_OPTION("enable_hashjoin", ENABLE_HASHJOIN);

#ifdef __DEBUG__
	elog(LOG, "TEST %s  nestloop=%s  mergejoin=%s  hashjoin=%s",
		 __func__,
		 (mask & (ENABLE_NESTLOOP)) ? "true" : "false",
		 (mask & (ENABLE_MERGEJOIN)) ? "true" : "false",
		 (mask & (ENABLE_HASHJOIN)) ? "true" : "false"
		);
#endif

	/*
	 * Hash join may be rejected for the reason of estimated memory usage. Try
	 * getting rid of that limitation.
	 */
	if (enforce_mask == ENABLE_HASHJOIN)
	{
		char		buf[32];
		int			new_multipler;

		/* See final_cost_hashjoin(). */
		new_multipler = MAX_KILOBYTES / work_mem;

		/* See guc.c for the upper limit */
		if (new_multipler >= 1000)
			new_multipler = 1000;

		if (new_multipler > hash_mem_multiplier)
		{
			snprintf(buf, sizeof(buf), UINT64_FORMAT, (uint64) new_multipler);
			set_config_option_noerror("hash_mem_multiplier", buf,
									  context, PGC_S_SESSION, GUC_ACTION_SAVE,
									  true, ERROR);
		}
	}
}
