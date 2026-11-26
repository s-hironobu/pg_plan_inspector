#!/usr/bin/env python3
"""JOB-light: estimators compared by execution time on the IMDb data.

JOB-light (Kipf et al., "Learned Cardinalities: Estimating Correlated Joins with
Deep Learning", CIDR 2019) has 70 COUNT(*) queries over 6 tables of the IMDb data
of JOB (Leis et al., PVLDB 9, 2015). Each query joins title with 1 to 4 of cast_info,
movie_companies, movie_info, movie_info_idx and movie_keyword, with predicates on
integer columns.

The training queries come from the MSCN training set (train.sql, the first --train
of them). They have 0 to 2 joins. The test queries are split in two sets:

  A  the test queries with at most 3 tables: join graphs of the training queries
  B  the test queries with 4 or 5 tables: more joins than any training query

This benchmark does not download the IMDb data (about 1.2 GB, 3 GB in the database).
Prepare it once with benchmark/lib/imdb.py (download, extract, import), into schema
imdb. The steps and the report are those of lib/bench.py.

    python3 benchmark/job_light/run.py --dsn ...
"""

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import lib  # noqa: E402,F401  (makes client/ importable without PYTHONPATH)
from lib import imdb  # noqa: E402
from lib.bench import Bench, main  # noqa: E402


def check(conn, a, say):
    try:
        imdb.require(conn, tables=imdb.JOB_LIGHT)
    except imdb.NotLoaded as e:
        sys.exit(f"   {e}")
    n = conn.execute("SELECT sum(reltuples)::bigint FROM pg_class c JOIN pg_namespace s ON s.oid = c.relnamespace "
                     "WHERE s.nspname = 'imdb' AND c.relname = ANY(%s)", (imdb.JOB_LIGHT,)).fetchone()[0]
    say(f"   schema imdb: the 6 tables of JOB-light, about {n:,} rows")


BENCH = Bench(name="job_light", here=HERE, prefix="jl", data_title="IMDb data (schema imdb, from benchmark/lib/imdb.py)",
              prepare=check, search_path="imdb, public", timeout=60, train=(630, 210))

if __name__ == "__main__":
    main(BENCH, __doc__)
