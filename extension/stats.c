/*-------------------------------------------------------------------------
 * stats.c
 *	  Statistics versions and their contents.
 *
 * A version identifies the statistical input of the planner for one relation.
 * It is made from:
 *   - the size fields of pg_class
 *   - every pg_statistic row of the relation (by its xmin, so a new ANALYZE
 *     gives a new version even if the numbers happen to be equal)
 *   - extended statistics data
 *   - the same for the indexes of the relation
 * The version is computed at every planning (syscache lookups only). The
 * contents are captured once per version, at first sight, by the backend that
 * planned with it. So this catches the ANALYZE of autovacuum as well as manual
 * ones.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/pg_class.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_statistic_ext_data.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "utils/array.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#include "pgpi.h"

/* ---------- version ---------- */

static void
add_u64(StringInfo key, uint64 v)
{
	appendBinaryStringInfo(key, (const char *) &v, sizeof(v));
}

static void
add_stat_rows(StringInfo key, Oid relid, int natts)
{
	for (int att = 1; att <= natts; att++)
	{
		for (int inh = 0; inh <= 1; inh++)
		{
			HeapTuple	tup = SearchSysCache3(STATRELATTINH,
											  ObjectIdGetDatum(relid),
											  Int16GetDatum(att),
											  BoolGetDatum(inh));

			if (!HeapTupleIsValid(tup))
				continue;
			add_u64(key, ((uint64) att << 8) | inh);
			add_u64(key, HeapTupleHeaderGetRawXmin(tup->t_data));
			ReleaseSysCache(tup);
		}
	}
}

static bool
add_class_row(StringInfo key, Oid relid, int *natts)
{
	HeapTuple	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	Form_pg_class c;
	float8		reltuples;

	if (!HeapTupleIsValid(tup))
		return false;
	c = (Form_pg_class) GETSTRUCT(tup);
	reltuples = c->reltuples;
	add_u64(key, relid);
	add_u64(key, *(uint64 *) &reltuples);
	add_u64(key, (uint64) c->relpages);
	add_u64(key, (uint64) c->relallvisible);
	add_u64(key, (uint64) c->relallfrozen);
	*natts = c->relnatts;
	ReleaseSysCache(tup);
	return true;
}

/*
 * Backend-local cache of versions. Any change to pg_class, pg_statistic or
 * extended statistics data arrives as a syscache invalidation and empties
 * the whole cache. So a cached version is always current.
 */
typedef struct VersionEntry
{
	Oid			relid;
	int64		version;
} VersionEntry;

static HTAB *version_cache = NULL;

static void
version_cache_inval(Datum arg, SysCacheIdentifier cacheid, uint32 hashvalue)
{
	if (version_cache != NULL)
	{
		hash_destroy(version_cache);
		version_cache = NULL;
	}
}

static int64 compute_version(Oid relid);

int64
pgpi_stats_version(Oid relid)
{
	static bool registered = false;
	VersionEntry *e;
	bool		found;
	int64		v;

	if (!registered)
	{
		CacheRegisterSyscacheCallback(RELOID, version_cache_inval, (Datum) 0);
		CacheRegisterSyscacheCallback(STATRELATTINH, version_cache_inval, (Datum) 0);
		CacheRegisterSyscacheCallback(STATEXTDATASTXOID, version_cache_inval, (Datum) 0);
		registered = true;
	}
	if (version_cache == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(VersionEntry);
		ctl.hcxt = TopMemoryContext;
		version_cache = hash_create("pgpi stats versions", 64, &ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	e = hash_search(version_cache, &relid, HASH_FIND, &found);
	if (found)
		return e->version;
	v = compute_version(relid);
	/* the computation may have processed invalidations */
	if (version_cache != NULL)
	{
		e = hash_search(version_cache, &relid, HASH_ENTER, &found);
		e->version = v;
	}
	return v;
}

static int64
compute_version(Oid relid)
{
	StringInfoData key;
	int			natts = 0;
	Relation	rel;
	List	   *l;
	ListCell   *lc;
	int64		result;

	initStringInfo(&key);
	if (!add_class_row(&key, relid, &natts))
		return 0;
	add_stat_rows(&key, relid, natts);

	rel = relation_open(relid, AccessShareLock);
	l = RelationGetStatExtList(rel);
	foreach(lc, l)
	{
		Oid			stxoid = lfirst_oid(lc);

		for (int inh = 0; inh <= 1; inh++)
		{
			HeapTuple	tup = SearchSysCache2(STATEXTDATASTXOID,
											  ObjectIdGetDatum(stxoid),
											  BoolGetDatum(inh));

			if (!HeapTupleIsValid(tup))
				continue;
			add_u64(&key, stxoid);
			add_u64(&key, HeapTupleHeaderGetRawXmin(tup->t_data));
			ReleaseSysCache(tup);
		}
	}
	l = RelationGetIndexList(rel);
	foreach(lc, l)
	{
		Oid			indexoid = lfirst_oid(lc);
		int			inatts = 0;

		if (add_class_row(&key, indexoid, &inatts))
			add_stat_rows(&key, indexoid, inatts);
	}
	relation_close(rel, AccessShareLock);

	result = (int64) hash_bytes_extended((const unsigned char *) key.data, key.len, 0);
	pfree(key.data);
	return result;
}

/* ---------- contents ---------- */

static const char *
slot_kind_name(int kind)
{
	switch (kind)
	{
		case STATISTIC_KIND_MCV:
			return "mcv";
		case STATISTIC_KIND_HISTOGRAM:
			return "histogram";
		case STATISTIC_KIND_CORRELATION:
			return "correlation";
		case STATISTIC_KIND_MCELEM:
			return "mcelem";
		case STATISTIC_KIND_DECHIST:
			return "dechist";
		case STATISTIC_KIND_RANGE_LENGTH_HISTOGRAM:
			return "range_length_histogram";
		case STATISTIC_KIND_BOUNDS_HISTOGRAM:
			return "bounds_histogram";
		default:
			return "other";
	}
}

static void
append_stat_row(StringInfo buf, HeapTuple tup, bool inh)
{
	Form_pg_statistic st = (Form_pg_statistic) GETSTRUCT(tup);
	bool		first = true;

	appendStringInfo(buf, "\"inh\":%s,\"null_frac\":", inh ? "true" : "false");
	pgpi_append_double(buf, st->stanullfrac);
	appendStringInfo(buf, ",\"avg_width\":%d,\"n_distinct\":", st->stawidth);
	pgpi_append_double(buf, st->stadistinct);
	appendStringInfoString(buf, ",\"slots\":[");
	for (int k = 0; k < STATISTIC_NUM_SLOTS; k++)
	{
		int16		kind = (&st->stakind1)[k];
		Oid			op = (&st->staop1)[k];
		Oid			coll = (&st->stacoll1)[k];
		Datum		d;
		bool		isnull;

		if (kind == 0)
			continue;
		if (!first)
			appendStringInfoChar(buf, ',');
		first = false;
		appendStringInfo(buf, "{\"kind\":%d,\"kind_name\":\"%s\",\"op\":%u,\"coll\":%u",
						 kind, slot_kind_name(kind), op, coll);

		d = SysCacheGetAttr(STATRELATTINH, tup, Anum_pg_statistic_stanumbers1 + k, &isnull);
		if (!isnull)
		{
			Datum	   *elems;
			bool	   *nulls;
			int			n;

			deconstruct_array(DatumGetArrayTypeP(d), FLOAT4OID, sizeof(float4),
							  true, TYPALIGN_INT, &elems, &nulls, &n);
			appendStringInfoString(buf, ",\"numbers\":[");
			for (int i = 0; i < n; i++)
			{
				if (i)
					appendStringInfoChar(buf, ',');
				if (nulls[i])
					appendStringInfoString(buf, "null");
				else
					pgpi_append_double(buf, DatumGetFloat4(elems[i]));
			}
			appendStringInfoChar(buf, ']');
		}

		d = SysCacheGetAttr(STATRELATTINH, tup, Anum_pg_statistic_stavalues1 + k, &isnull);
		if (!isnull)
		{
			ArrayType  *arr = DatumGetArrayTypeP(d);
			Oid			elemtype = ARR_ELEMTYPE(arr);
			int16		typlen;
			bool		typbyval;
			char		typalign;
			Oid			outfn;
			bool		isvarlena;
			Datum	   *elems;
			bool	   *nulls;
			int			n;

			get_typlenbyvalalign(elemtype, &typlen, &typbyval, &typalign);
			getTypeOutputInfo(elemtype, &outfn, &isvarlena);
			deconstruct_array(arr, elemtype, typlen, typbyval, typalign,
							  &elems, &nulls, &n);
			appendStringInfoString(buf, ",\"values_type\":");
			pgpi_append_json_string(buf, format_type_be(elemtype));
			appendStringInfoString(buf, ",\"values\":[");
			for (int i = 0; i < n; i++)
			{
				if (i)
					appendStringInfoChar(buf, ',');
				if (nulls[i])
					appendStringInfoString(buf, "null");
				else
					pgpi_append_json_string(buf, OidOutputFunctionCall(outfn, elems[i]));
			}
			appendStringInfoChar(buf, ']');
		}
		appendStringInfoChar(buf, '}');
	}
	appendStringInfoChar(buf, ']');
}

static void
append_columns(StringInfo buf, Oid relid, int natts)
{
	bool		first = true;

	appendStringInfoString(buf, "\"columns\":[");
	for (int att = 1; att <= natts; att++)
	{
		for (int inh = 0; inh <= 1; inh++)
		{
			HeapTuple	tup = SearchSysCache3(STATRELATTINH,
											  ObjectIdGetDatum(relid),
											  Int16GetDatum(att),
											  BoolGetDatum(inh));
			char	   *attname;

			if (!HeapTupleIsValid(tup))
				continue;
			if (!first)
				appendStringInfoChar(buf, ',');
			first = false;
			attname = get_attname(relid, att, true);
			appendStringInfo(buf, "{\"attnum\":%d,\"attname\":", att);
			pgpi_append_json_string(buf, attname);
			appendStringInfoString(buf, ",\"type\":");
			{
				Oid			typid = get_atttype(relid, att);

				pgpi_append_json_string(buf, OidIsValid(typid) ? format_type_be(typid) : NULL);
			}
			appendStringInfoChar(buf, ',');
			append_stat_row(buf, tup, inh);
			appendStringInfoChar(buf, '}');
			ReleaseSysCache(tup);
		}
	}
	appendStringInfoChar(buf, ']');
}

static bool
append_class(StringInfo buf, Oid relid, int *natts)
{
	HeapTuple	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	Form_pg_class c;

	if (!HeapTupleIsValid(tup))
		return false;
	c = (Form_pg_class) GETSTRUCT(tup);
	appendStringInfo(buf, "\"relid\":%u,\"name\":", relid);
	pgpi_append_json_string(buf,
							quote_qualified_identifier(get_namespace_name_or_temp(c->relnamespace),
													   NameStr(c->relname)));
	appendStringInfo(buf, ",\"relkind\":\"%c\",\"reltuples\":", c->relkind);
	pgpi_append_double(buf, c->reltuples);
	appendStringInfo(buf, ",\"relpages\":%d,\"relallvisible\":%d,\"relallfrozen\":%d,\"relhassubclass\":%s",
					 c->relpages, c->relallvisible, c->relallfrozen,
					 c->relhassubclass ? "true" : "false");
	*natts = c->relnatts;
	ReleaseSysCache(tup);
	return true;
}

static void
append_ext_value(StringInfo buf, const char *key, HeapTuple tup, AttrNumber attno,
				 Oid typid)
{
	bool		isnull;
	Datum		d = SysCacheGetAttr(STATEXTDATASTXOID, tup, attno, &isnull);
	Oid			outfn;
	bool		isvarlena;

	appendStringInfo(buf, ",\"%s\":", key);
	if (isnull)
	{
		appendStringInfoString(buf, "null");
		return;
	}
	getTypeOutputInfo(typid, &outfn, &isvarlena);
	pgpi_append_json_string(buf, OidOutputFunctionCall(outfn, d));
}

char *
pgpi_stats_capture(Oid relid)
{
	StringInfoData buf;
	int			natts = 0;
	Relation	rel;
	List	   *l;
	ListCell   *lc;
	bool		first;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '{');
	if (!append_class(&buf, relid, &natts))
	{
		appendStringInfo(&buf, "\"relid\":%u}", relid);
		return buf.data;
	}
	appendStringInfoChar(&buf, ',');
	append_columns(&buf, relid, natts);

	rel = relation_open(relid, AccessShareLock);

	/* extended statistics */
	appendStringInfoString(&buf, ",\"extended\":[");
	first = true;
	l = RelationGetStatExtList(rel);
	foreach(lc, l)
	{
		Oid			stxoid = lfirst_oid(lc);
		HeapTuple	stx = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(stxoid));
		Form_pg_statistic_ext sf;

		if (!HeapTupleIsValid(stx))
			continue;
		sf = (Form_pg_statistic_ext) GETSTRUCT(stx);
		for (int inh = 0; inh <= 1; inh++)
		{
			HeapTuple	tup = SearchSysCache2(STATEXTDATASTXOID,
											  ObjectIdGetDatum(stxoid),
											  BoolGetDatum(inh));
			bool		isnull;

			if (!HeapTupleIsValid(tup))
				continue;
			if (!first)
				appendStringInfoChar(&buf, ',');
			first = false;
			appendStringInfo(&buf, "{\"oid\":%u,\"name\":", stxoid);
			pgpi_append_json_string(&buf, NameStr(sf->stxname));
			appendStringInfo(&buf, ",\"inh\":%s", inh ? "true" : "false");
			append_ext_value(&buf, "ndistinct", tup,
							 Anum_pg_statistic_ext_data_stxdndistinct, PG_NDISTINCTOID);
			append_ext_value(&buf, "dependencies", tup,
							 Anum_pg_statistic_ext_data_stxddependencies, PG_DEPENDENCIESOID);
			(void) SysCacheGetAttr(STATEXTDATASTXOID, tup,
								   Anum_pg_statistic_ext_data_stxdmcv, &isnull);
			appendStringInfo(&buf, ",\"has_mcv\":%s", isnull ? "false" : "true");
			(void) SysCacheGetAttr(STATEXTDATASTXOID, tup,
								   Anum_pg_statistic_ext_data_stxdexpr, &isnull);
			appendStringInfo(&buf, ",\"has_expr_stats\":%s}", isnull ? "false" : "true");
			ReleaseSysCache(tup);
		}
		ReleaseSysCache(stx);
	}
	appendStringInfoChar(&buf, ']');

	/* indexes: sizes and expression statistics */
	appendStringInfoString(&buf, ",\"indexes\":[");
	first = true;
	l = RelationGetIndexList(rel);
	foreach(lc, l)
	{
		Oid			indexoid = lfirst_oid(lc);
		int			inatts = 0;
		StringInfoData one;

		initStringInfo(&one);
		appendStringInfoChar(&one, '{');
		if (!append_class(&one, indexoid, &inatts))
			continue;
		appendStringInfoChar(&one, ',');
		append_columns(&one, indexoid, inatts);
		appendStringInfoChar(&one, '}');
		if (!first)
			appendStringInfoChar(&buf, ',');
		first = false;
		appendBinaryStringInfo(&buf, one.data, one.len);
	}
	appendStringInfoChar(&buf, ']');
	relation_close(rel, AccessShareLock);

	appendStringInfoChar(&buf, '}');
	return buf.data;
}
