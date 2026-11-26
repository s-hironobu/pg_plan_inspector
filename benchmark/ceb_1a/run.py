#!/usr/bin/env python3
"""CEB-IMDb template 1a: estimators compared by execution time on correlated predicates.

CEB (the Cardinality Estimation Benchmark of Negi et al., "Flow-Loss: Learning
Cardinality Estimates That Matter", PVLDB 14, 2021) generates its IMDb queries from
templates. Template 1a joins 9 tables (title, kind_type, two movie_info with their
info_type, cast_info, role_type, name). It filters on the kind of title, two kinds
of movie information (genres and languages, for example), the role, the gender and a
range of production years. These predicates are correlated across tables.

The template has 3000 queries. queries.txt names the queries that are used. All of
them have different literals:

  train  500 queries of the five pairs of information kinds that make up most of 1a
  A      100 other queries of the same five pairs
  B      60 queries with other pairs of information kinds. No training query has them.

The queries are downloaded once (lib/ceb.py) from a copy of the SQL of CEB. The IMDb
data must be prepared with benchmark/lib/imdb.py (title, kind_type, movie_info,
info_type, cast_info, role_type, name). The steps and the report are those of
lib/bench.py.

    python3 benchmark/ceb_1a/run.py --dsn ...

If you use these queries, cite the Flow-Loss paper (see lib/ceb.py).
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import lib  # noqa: E402,F401  (makes client/ importable without PYTHONPATH)
from lib import ceb, imdb  # noqa: E402
from lib.bench import Bench, main  # noqa: E402

TEST = {False: (100, 60), True: (50, 30)}      # (A, B) queries; with --quick


def check(conn, a, say):
    try:
        imdb.require(conn, tables=imdb.CEB_1A)
    except imdb.NotLoaded as e:
        sys.exit(f"   {e}")
    n = conn.execute("SELECT sum(reltuples)::bigint FROM pg_class c JOIN pg_namespace s ON s.oid = c.relnamespace "
                     "WHERE s.nspname = 'imdb' AND c.relname = ANY(%s)", (imdb.CEB_1A,)).fetchone()[0]
    say(f"   schema imdb: the 7 tables of CEB 1a, about {n:,} rows")


def workload(a, say):
    n_a, n_b = TEST[a.quick]
    return ceb.workload(os.path.join(HERE, "queries.txt"), n_a, n_b, say)


BENCH = Bench(name="ceb_1a", here=HERE, prefix="ceb", data_title="IMDb data (schema imdb, from benchmark/lib/imdb.py)",
              prepare=check, search_path="imdb, public", timeout=60, train=(500, 200), workload=workload)

if __name__ == "__main__":
    main(BENCH, __doc__)
