/*-------------------------------------------------------------------------
 * hash.h
 *
 * Copyright (c) 2021-2023, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#ifndef __HASH_H__
#define __HASH_H__

#include "common.h"

typedef enum entryType
{
	IS_EXPLAIN = 0,
	_DUMMY
}			entryType;

extern bool store_hash_entry(const int pid, const entryType et,
							 const bool value);
extern bool delete_hash_entry(const int pid);
extern bool find_hash_entry(const int pid, const entryType et);

extern void gc_hashtable(void);
#ifdef __DEBUG__
extern int	_test_count_hashtable(void);
#endif

#endif							/* __HASH_H__ */
