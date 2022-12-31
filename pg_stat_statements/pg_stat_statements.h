/*-------------------------------------------------------------------------
 * pg_stat_statements.h
 *
 *
 * Copyright (c) 2021-2023, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PG_STAT_STATEMENTS_H__
#define __PG_STAT_STATEMENTS_H__

#include "postgres.h"

#include "../src/common.h"

#define JUMBLE_SIZE				MAX_QUERY_LEN	/* query serialization buffer
												 * size */

/*
 * Struct for tracking locations/lengths of constants during normalization
 */
typedef struct pgssLocationLen
{
	int			location;		/* start offset in query text */
	int			length;			/* length in bytes, or -1 to ignore */
}			pgssLocationLen;

/*
 * Working state for computing a query jumble and producing a normalized
 * query string
 */
typedef struct pgssJumbleState
{
	/* Jumble of current query tree */
	unsigned char *jumble;

	/* Number of bytes used in jumble[] */
	Size		jumble_len;

	/* Array of locations of constants that should be removed */
	pgssLocationLen *clocations;

	/* Allocated length of clocations array */
	int			clocations_buf_size;

	/* Current number of valid entries in clocations array */
	int			clocations_count;

	/* highest Param id we've seen, in order to start normalization correctly */
	int			highest_extern_param_id;
}			pgssJumbleState;



void		JumbleQuery(pgssJumbleState * jstate, Query *query);

#endif							/* __PG_STAT_STATEMENTS_H__ */
