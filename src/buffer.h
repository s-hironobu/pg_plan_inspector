/*-------------------------------------------------------------------------
 * buffer.h
 *
 * Copyright (c) 2021-2024, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BUFFER_H__
#define __BUFFER_H__

#include "common.h"

/*
 * Data structure of ring buffer slot
 */
typedef struct bufferSlot
{
	int			nested_level;	/* query's nested_level */
	struct queryPlanData qpd;	/* plan data of the query */
	struct queryInfo qi;		/* query's info */
}			bufferSlot;

extern void store_plan(const queryInfo qi, const int nested_level,
					   const uint64 queryId);
extern bool is_alive(pid_t pid);
extern void create_ring_buffer(void);
extern void sweepBuffer(const bool recoveryInProgress);

#endif							/* __BUFFER_H__ */
