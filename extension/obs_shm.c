/*-------------------------------------------------------------------------
 * obs_shm.c
 *	  Shared memory for observation:
 *
 *   - the log ring: a byte ring of variable-length records. Backends write
 *     to it at the end of each query, and the pgpi worker drains it into
 *     tables. Writers never wait. If the ring is full, the record is
 *     dropped and counted.
 *   - the set of statistics versions that are already captured (first-sight
 *     capture).
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "pgpi.h"

/* ---------- shared state ---------- */

typedef struct PgpiShared
{
	LWLock		ring_lock;
	LWLock		seen_lock;
	uint64		head;			/* bytes ever written */
	uint64		tail;			/* bytes ever consumed */
	uint64		committed;		/* bytes stored into tables by the worker */
	uint64		nwritten;
	uint64		ndropped;
	Size		ring_size;
	ProcNumber	worker_procno;
	int			worker_pid;
} PgpiShared;

typedef struct SeenKey
{
	Oid			dbid;
	Oid			relid;
	int64		version;
} SeenKey;

static PgpiShared *shared = NULL;
static char *ring = NULL;
static HTAB *seen = NULL;

static void shm_request(void *arg);
static void shm_init(void *arg);

static const ShmemCallbacks pgpi_shmem_callbacks = {
	.request_fn = shm_request,
	.init_fn = shm_init,
};

/* ---------- setup ---------- */

void
pgpi_shm_register(void)
{
	RegisterShmemCallbacks(&pgpi_shmem_callbacks);
}

bool
pgpi_shm_available(void)
{
	return shared != NULL && ring != NULL && seen != NULL;
}

static Size
ring_bytes(void)
{
	return (Size) pgpi_log_buffer_kb * 1024;
}

static void
shm_request(void *arg)
{
	ShmemRequestStruct(.name = "pgpi shared state",
					   .size = sizeof(PgpiShared),
					   .ptr = (void **) &shared);
	ShmemRequestStruct(.name = "pgpi log ring",
					   .size = ring_bytes(),
					   .ptr = (void **) &ring);
	ShmemRequestHash(.name = "pgpi stats seen",
					 .nelems = pgpi_stats_max,
					 .hash_info.keysize = sizeof(SeenKey),
					 .hash_info.entrysize = sizeof(SeenKey),
					 .hash_flags = HASH_ELEM | HASH_BLOBS,
					 .ptr = &seen);
}

static void
shm_init(void *arg)
{
	int			tranche = LWLockNewTrancheId("pgpi");

	LWLockInitialize(&shared->ring_lock, tranche);
	LWLockInitialize(&shared->seen_lock, tranche);
	shared->head = shared->tail = shared->committed = 0;
	shared->nwritten = shared->ndropped = 0;
	shared->ring_size = ring_bytes();
	shared->worker_procno = INVALID_PROC_NUMBER;
	shared->worker_pid = 0;
}

/* ---------- the log ring ---------- */

static void
ring_copy_in(uint64 pos, const char *src, Size len)
{
	Size		size = shared->ring_size;
	Size		off = pos % size;
	Size		first = Min(len, size - off);

	memcpy(ring + off, src, first);
	if (first < len)
		memcpy(ring, src + first, len - first);
}

static void
ring_copy_out(uint64 pos, char *dst, Size len)
{
	Size		size = shared->ring_size;
	Size		off = pos % size;
	Size		first = Min(len, size - off);

	memcpy(dst, ring + off, first);
	if (first < len)
		memcpy(dst + first, ring, len - first);
}

/*
 * Record layout:
 *   uint32 total length (including this word, MAXALIGN'ed)
 *   uint32 type
 *   uint32 header length, header bytes
 *   uint32 nfields, then per field: uint32 length (UINT32_MAX = NULL), bytes
 */
bool
pgpi_ring_put(int type, const void *hdr, Size hdrlen,
			  const char *const *fields, int nfields)
{
	Size		total = 4 * sizeof(uint32) + hdrlen;
	uint32	   *lens;
	char	   *rec;
	char	   *p;
	uint32		u;
	ProcNumber	worker;

	if (!pgpi_shm_available())
		return false;

	lens = palloc(sizeof(uint32) * nfields);
	for (int i = 0; i < nfields; i++)
	{
		lens[i] = fields[i] ? (uint32) strlen(fields[i]) : PG_UINT32_MAX;
		total += sizeof(uint32) + (fields[i] ? lens[i] : 0);
	}
	total = MAXALIGN(total);

	if (total > shared->ring_size / 2)
	{
		LWLockAcquire(&shared->ring_lock, LW_EXCLUSIVE);
		shared->ndropped++;
		LWLockRelease(&shared->ring_lock);
		pfree(lens);
		return false;
	}

	rec = palloc0(total);
	p = rec;
	u = (uint32) total;
	memcpy(p, &u, 4);
	p += 4;
	u = (uint32) type;
	memcpy(p, &u, 4);
	p += 4;
	u = (uint32) hdrlen;
	memcpy(p, &u, 4);
	p += 4;
	memcpy(p, hdr, hdrlen);
	p += hdrlen;
	u = (uint32) nfields;
	memcpy(p, &u, 4);
	p += 4;
	for (int i = 0; i < nfields; i++)
	{
		memcpy(p, &lens[i], 4);
		p += 4;
		if (fields[i])
		{
			memcpy(p, fields[i], lens[i]);
			p += lens[i];
		}
	}

	LWLockAcquire(&shared->ring_lock, LW_EXCLUSIVE);
	if (shared->ring_size - (Size) (shared->head - shared->tail) < total)
	{
		shared->ndropped++;
		LWLockRelease(&shared->ring_lock);
		pfree(rec);
		pfree(lens);
		return false;
	}
	ring_copy_in(shared->head, rec, total);
	shared->head += total;
	shared->nwritten++;
	worker = shared->worker_procno;
	LWLockRelease(&shared->ring_lock);

	if (worker != INVALID_PROC_NUMBER)
		SetLatch(&GetPGProcByNumber(worker)->procLatch);

	pfree(rec);
	pfree(lens);
	return true;
}

/* Worker: take whole records, at most bufsize bytes. *endpos is the new tail. */
Size
pgpi_ring_take(char *buf, Size bufsize, uint64 *endpos)
{
	Size		taken = 0;

	LWLockAcquire(&shared->ring_lock, LW_EXCLUSIVE);
	while (shared->head > shared->tail)
	{
		uint32		len;

		ring_copy_out(shared->tail, (char *) &len, 4);
		if (taken + len > bufsize)
			break;
		ring_copy_out(shared->tail, buf + taken, len);
		shared->tail += len;
		taken += len;
	}
	*endpos = shared->tail;
	LWLockRelease(&shared->ring_lock);
	return taken;
}

/* Worker: everything up to endpos has been committed (or given up). */
void
pgpi_ring_mark_committed(uint64 endpos)
{
	LWLockAcquire(&shared->ring_lock, LW_EXCLUSIVE);
	if (endpos > shared->committed)
		shared->committed = endpos;
	LWLockRelease(&shared->ring_lock);
}

static void
worker_detach(int code, Datum arg)
{
	LWLockAcquire(&shared->ring_lock, LW_EXCLUSIVE);
	if (shared->worker_procno == MyProcNumber)
	{
		shared->worker_procno = INVALID_PROC_NUMBER;
		shared->worker_pid = 0;
	}
	LWLockRelease(&shared->ring_lock);
}

void
pgpi_ring_set_worker(void)
{
	LWLockAcquire(&shared->ring_lock, LW_EXCLUSIVE);
	shared->worker_procno = MyProcNumber;
	shared->worker_pid = MyProcPid;
	LWLockRelease(&shared->ring_lock);
	before_shmem_exit(worker_detach, (Datum) 0);
}

/* ---------- statistics versions already captured ---------- */

bool
pgpi_stats_seen(Oid dbid, Oid relid, int64 version, bool insert)
{
	SeenKey		key;
	bool		found;

	if (!pgpi_shm_available())
		return true;
	memset(&key, 0, sizeof(key));
	key.dbid = dbid;
	key.relid = relid;
	key.version = version;

	if (!insert)
	{
		LWLockAcquire(&shared->seen_lock, LW_SHARED);
		(void) hash_search(seen, &key, HASH_FIND, &found);
		LWLockRelease(&shared->seen_lock);
		return found;
	}

	LWLockAcquire(&shared->seen_lock, LW_EXCLUSIVE);
	if (hash_search(seen, &key, HASH_ENTER_NULL, &found) == NULL)
	{
		/* full: forget everything. The table removes duplicates when the versions are captured again. */
		HASH_SEQ_STATUS st;
		SeenKey    *e;

		hash_seq_init(&st, seen);
		while ((e = hash_seq_search(&st)) != NULL)
			(void) hash_search(seen, e, HASH_REMOVE, NULL);
		(void) hash_search(seen, &key, HASH_ENTER_NULL, &found);
	}
	LWLockRelease(&shared->seen_lock);
	return found;
}

void
pgpi_stats_seen_reset(void)
{
	HASH_SEQ_STATUS st;
	SeenKey    *e;

	if (!pgpi_shm_available())
		return;
	LWLockAcquire(&shared->seen_lock, LW_EXCLUSIVE);
	hash_seq_init(&st, seen);
	while ((e = hash_seq_search(&st)) != NULL)
		(void) hash_search(seen, e, HASH_REMOVE, NULL);
	LWLockRelease(&shared->seen_lock);
}

PG_FUNCTION_INFO_V1(pgpi_log_status);
PG_FUNCTION_INFO_V1(pgpi_log_flush);
PG_FUNCTION_INFO_V1(pgpi_stats_reset_seen);

/* Forget which statistics versions were captured (after deleting rows of pgpi.stats). */
Datum
pgpi_stats_reset_seen(PG_FUNCTION_ARGS)
{
	pgpi_stats_seen_reset();
	PG_RETURN_VOID();
}

Datum
pgpi_log_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[7];
	bool		nulls[7] = {0};
	long		nseen = 0;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	if (!pgpi_shm_available())
	{
		for (int i = 0; i < 7; i++)
			nulls[i] = true;
		PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
	}
	LWLockAcquire(&shared->seen_lock, LW_SHARED);
	nseen = hash_get_num_entries(seen);
	LWLockRelease(&shared->seen_lock);
	LWLockAcquire(&shared->ring_lock, LW_SHARED);
	values[0] = Int64GetDatum((int64) shared->ring_size);
	values[1] = Int64GetDatum((int64) (shared->head - shared->tail));
	values[2] = Int64GetDatum((int64) shared->nwritten);
	values[3] = Int64GetDatum((int64) shared->ndropped);
	values[4] = Int32GetDatum(shared->worker_pid);
	LWLockRelease(&shared->ring_lock);
	values[5] = Int64GetDatum((int64) nseen);
	values[6] = BoolGetDatum(pgpi_log_plans);
	if (values[4] == Int32GetDatum(0))
		nulls[4] = true;
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/* Wait until the worker has stored everything written so far. */
Datum
pgpi_log_flush(PG_FUNCTION_ARGS)
{
	int			timeout_ms = PG_ARGISNULL(0) ? 10000 : PG_GETARG_INT32(0);
	TimestampTz start = GetCurrentTimestamp();
	uint64		target;

	if (!pgpi_shm_available())
		PG_RETURN_BOOL(false);
	LWLockAcquire(&shared->ring_lock, LW_SHARED);
	target = shared->head;
	LWLockRelease(&shared->ring_lock);
	for (;;)
	{
		uint64		committed;

		LWLockAcquire(&shared->ring_lock, LW_SHARED);
		committed = shared->committed;
		LWLockRelease(&shared->ring_lock);
		if (committed >= target)
			break;
		if (TimestampDifferenceExceeds(start, GetCurrentTimestamp(), timeout_ms))
			PG_RETURN_BOOL(false);
		(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 20, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
	PG_RETURN_BOOL(true);
}
