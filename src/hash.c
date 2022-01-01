/*-------------------------------------------------------------------------
 * hash.c
 *
 * This hash table is allocated on the shared memory and is used to share
 * the information between a leader process and the corresponding parallel
 * bgworker processes.
 *
 * Currently, the hash table only store the status of whether each
 * (leader) process is executing EXPLAIN statement or not.
 *
 * Whenever a (leader) process executes a query, it stores an entry that
 * contains the status into the hash table; in a parallel query, the parallel
 * bgworker processes refer to the entry and do the appropriate procedure.
 *
 * Copyright (c) 2021-2022, Hironobu Suzuki @ interdb.jp
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"

#include "buffer.h"
#include "common.h"
#include "hash.h"

#undef __DEBUG__

/*
 * Global variable declarations
 */
extern pgqpSharedState * pgqp;
extern HTAB *pgqp_hash;

/*
 * Static function declaration
 */
static pgqpEntry * alloc_entry(pgqpHashKey * key);

/*
 * Allocate and return a new hash entry if there is no specified key entry;
 * otherwise, i.e. a specified key entry exists, return the entry.
 */
static pgqpEntry *
alloc_entry(pgqpHashKey * key)
{
	pgqpEntry  *entry;
	bool		found;

	/* Find or create an entry with desired hash code */
	entry = (pgqpEntry *) hash_search(pgqp_hash, key, HASH_ENTER, &found);

	if (!found)
	{
		entry->is_explain = false;
		entry->_dummy = false;
	}
	return entry;
}

/*
 * access methods
 */
bool
store_hash_entry(const int pid, const entryType et, const bool value)
{
	pgqpHashKey key;
	pgqpEntry  *entry;

	/* Safety check... */
	if (!pgqp || !pgqp_hash)
	{
		elog(ERROR, "There is no pgqp or pgqp_hash.");
		return false;
	}

	key.pid = pid;

	LWLockAcquire(pgqp->lock, LW_EXCLUSIVE);
	entry = (pgqpEntry *) hash_search(pgqp_hash, &key, HASH_FIND, NULL);
	if (!entry)
		entry = alloc_entry(&key);

	if (et == IS_EXPLAIN)
		entry->is_explain = value;
	else if (et == _DUMMY)
		entry->_dummy = value;

	LWLockRelease(pgqp->htlock);

	return true;
}


bool
delete_hash_entry(const int pid)
{
	pgqpHashKey key;

	/* Safety check... */
	if (!pgqp || !pgqp_hash)
	{
		elog(ERROR, "There is no pgqp or pgqp_hash.");
		return false;
	}

	key.pid = pid;

	LWLockAcquire(pgqp->htlock, LW_EXCLUSIVE);
	hash_search(pgqp_hash, &key, HASH_REMOVE, NULL);
	LWLockRelease(pgqp->htlock);

	return true;
}

bool
find_hash_entry(const int pid, const entryType et)
{
	pgqpHashKey key;
	pgqpEntry  *entry;
	bool		found;
	bool		ret = false;

	/* Safety check... */
	if (!pgqp || !pgqp_hash)
	{
		elog(ERROR, "There is no pgqp or pgqp_hash.");
		return false;
	}

	key.pid = pid;

	LWLockAcquire(pgqp->htlock, LW_SHARED);
	entry = (pgqpEntry *) hash_search(pgqp_hash, &key, HASH_FIND, &found);
	if (found)
	{
		if (et == IS_EXPLAIN)
			ret = entry->is_explain;
		else if (et == _DUMMY)
			ret = entry->_dummy;
	}
	LWLockRelease(pgqp->htlock);

	return ret;
}

/*
 * Garbage collects the hash table.
 *
 * This function is periodically invoked by the bgworker, and deletes
 * the entries whose keys are the pids of already halted processes.
 * These orphan entries occur when a backend is closed (or crashed)
 * during executing an EXPLAIN statement.
 */
void
gc_hashtable(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgqpEntry  *entry;
#ifdef __DEBUG__
	elog(LOG, "HASHTEST %s", __func__);
	_test_count_hashtable();
#endif
	LWLockAcquire(pgqp->htlock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, pgqp_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		pgqpHashKey key;
		int			pid = entry->key.pid;

		if (!is_alive(pid))
		{
			key.pid = pid;
#ifdef __DEBUG__
			elog(LOG, "HASHTEST %s   DELETE %d", __func__, pid);
#endif
			hash_search(pgqp_hash, &key, HASH_REMOVE, NULL);
		}
	}
	LWLockRelease(pgqp->htlock);
}

#ifdef __DEBUG__
int
_test_count_hashtable(void)
{
	int			num_entries;

	LWLockAcquire(pgqp->htlock, LW_EXCLUSIVE);
	num_entries = hash_get_num_entries(pgqp_hash);
	LWLockRelease(pgqp->htlock);
	elog(LOG, "HASHTEST %s num_entries=%d", __func__, num_entries);

	return num_entries;
}
#endif
