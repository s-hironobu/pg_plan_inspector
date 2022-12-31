/*-------------------------------------------------------------------------
 * bgworker.h
 *
 * Copyright (c) 2021-2023, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __BGWORKER_H__
#define __BGWORKER_H__

#define BGWORKER_NAPTIME 5		/* unit = sec */
#define BGWORKER_RESTART_TIME  2	/* unit = sec */

extern void start_tx(void);
extern void commit_tx(void);

#endif							/* __BGWORKER_H__ */
