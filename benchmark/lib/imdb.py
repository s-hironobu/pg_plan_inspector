#!/usr/bin/env python3
"""The IMDb data of JOB (Leis et al., PVLDB 9, 2015) for the benchmarks job_light and ceb_1a.

The two workloads use 10 of the 21 IMDb tables, and only some of their columns.
This tool keeps exactly those. It has three steps. Each step can be run (and rerun)
alone. The files go to a directory outside pgpi (default ~/imdb-job, or $PGPI_IMDB_DIR):

    python3 benchmark/lib/imdb.py download                  # imdb.tgz, about 1.2 GB (resumes if interrupted)
    python3 benchmark/lib/imdb.py extract                   # keep the 10 tables (gzipped CSV), delete imdb.tgz
    python3 benchmark/lib/imdb.py import --dsn "$DSN"       # every table whose file is there, one at a time
    python3 benchmark/lib/imdb.py import --dsn "$DSN" --table cast_info --remove   # one table, then delete its file
    python3 benchmark/lib/imdb.py status [--dsn "$DSN"]     # what is downloaded, extracted and imported

extract reads the archive as a stream and writes only the 10 CSV files, recompressed.
The other 11 tables are never written.

import loads one table into schema imdb (by default) with only the columns that the
workloads use. PostgreSQL itself parses the CSV (COPY, as for the full data set) into
a staging table. The trigger of the staging table keeps those columns and stores
nothing else. So a full-width copy of the table is never on disk. A table is
imported, indexed and analyzed on its own. The other tables need not be there yet.
With --remove, the CSV file is deleted afterwards.

JOB-light needs title, cast_info, movie_companies, movie_info, movie_info_idx and
movie_keyword. CEB 1a needs title, kind_type, movie_info, info_type, cast_info,
role_type and name. Tables imported before CEB 1a was added lack columns that it
needs (cast_info.person_id, movie_info.info). Import them again.

The row counts give the exact true cardinalities of the full data set, because the
queries use no other column. Execution times differ from a full load, because the
rows are narrower. The IMDb data is for personal and non-commercial use
(https://www.imdb.com/interfaces/). It is not bundled with pgpi.
"""

from __future__ import annotations

import argparse
import gzip
import os
import shutil
import sys
import tarfile
import time
import urllib.request

URL = "http://event.cwi.nl/da/job/imdb.tgz"
DIR = os.environ.get("PGPI_IMDB_DIR", os.path.expanduser("~/imdb-job"))
ARCHIVE = "imdb.tgz"
SCHEMA = "imdb"

# table -> (number of CSV columns, [(kept column, its 1-based position in the CSV, type)], rows in JOB's data)
I, T = "integer", "text"
TABLES = {
    "title":           (12, [("id", 1, I), ("kind_id", 4, I), ("production_year", 5, I)], 2528312),
    "cast_info":       (7, [("movie_id", 3, I), ("person_id", 2, I), ("role_id", 7, I)], 36244344),
    "movie_companies": (5, [("movie_id", 2, I), ("company_id", 3, I), ("company_type_id", 4, I)], 2609129),
    "movie_info":      (5, [("movie_id", 2, I), ("info_type_id", 3, I), ("info", 4, T)], 14835720),
    "movie_info_idx":  (5, [("movie_id", 2, I), ("info_type_id", 3, I)], 1380035),
    "movie_keyword":   (3, [("movie_id", 2, I), ("keyword_id", 3, I)], 4523930),
    "name":            (9, [("id", 1, I), ("gender", 5, T)], 4167491),
    "kind_type":       (2, [("id", 1, I), ("kind", 2, T)], 7),
    "info_type":       (2, [("id", 1, I), ("info", 2, T)], 113),
    "role_type":       (2, [("id", 1, I), ("role", 2, T)], 12),
}
PRIMARY_KEY = {"title", "name", "kind_type", "info_type", "role_type"}       # on id
INDEXES = {"cast_info": ["movie_id", "person_id"], "movie_companies": ["movie_id"],
           "movie_info": ["movie_id"], "movie_info_idx": ["movie_id"], "movie_keyword": ["movie_id"]}
JOB_LIGHT = ["title", "cast_info", "movie_companies", "movie_info", "movie_info_idx", "movie_keyword"]
CEB_1A = ["title", "kind_type", "movie_info", "info_type", "cast_info", "role_type", "name"]
# the other tables of the archive: never extracted, deleted if found unpacked in the directory
OTHER = ["aka_name", "aka_title", "char_name", "comp_cast_type", "company_name", "company_type",
         "complete_cast", "keyword", "link_type", "movie_link", "person_info"]


class NotLoaded(RuntimeError):
    """The imdb schema lacks tables (the data is never downloaded by a benchmark)."""


def csv_path(d: str, table: str) -> str:
    return os.path.join(d, f"{table}.csv.gz")


def say(*xs):
    print(*xs, flush=True)


def check_dir(d: str) -> str:
    d = os.path.abspath(os.path.expanduser(d))
    repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if d == repo or d.startswith(repo + os.sep):
        sys.exit(f"{d} is inside pgpi ({repo}); use a directory outside it (--dir or $PGPI_IMDB_DIR)")
    os.makedirs(d, exist_ok=True)
    return d


def free_gb(d: str) -> float:
    return shutil.disk_usage(d).free / 1e9


def mb(n: float) -> str:
    return f"{n / 1e6:,.0f} MB"


# ---------------------------------------------------------------- download

def download(d: str, url: str = URL) -> None:
    path = os.path.join(d, ARCHIVE)
    if os.path.exists(path):
        say(f"{path} is already there")
        return
    part = path + ".part"
    have = os.path.getsize(part) if os.path.exists(part) else 0
    req = urllib.request.Request(url, headers={"Range": f"bytes={have}-"} if have else {})
    say(f"downloading {url} -> {path} ({free_gb(d):.1f} GB free here; the archive is about 1.2 GB)")
    with urllib.request.urlopen(req) as r:
        if have and r.status != 206:            # the server ignored the range: start over
            have = 0
        total = have + int(r.headers.get("Content-Length") or 0)
        t0, last = time.time(), 0.0
        with open(part, "ab" if have else "wb") as f:
            while chunk := r.read(1 << 20):
                f.write(chunk)
                have += len(chunk)
                if time.time() - last > 5:
                    last = time.time()
                    pct = f" ({100 * have / total:.0f}%)" if total else ""
                    say(f"   {mb(have)}{pct}, {have / 1e6 / max(time.time() - t0, 1e-9):.1f} MB/s")
    os.replace(part, path)
    say(f"   done: {mb(os.path.getsize(path))}")


# ---------------------------------------------------------------- extract

def extract(d: str, keep_archive: bool = False) -> None:
    path = os.path.join(d, ARCHIVE)
    have = {t for t in TABLES if os.path.exists(csv_path(d, t))}
    loose = [t for t in TABLES if t not in have and os.path.exists(os.path.join(d, f"{t}.csv"))]
    for t in loose:                              # already unpacked by hand: compress what is kept
        say(f"   {t}.csv -> {t}.csv.gz")
        with open(os.path.join(d, f"{t}.csv"), "rb") as src, gzip.open(csv_path(d, t) + ".part", "wb", 3) as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        os.replace(csv_path(d, t) + ".part", csv_path(d, t))
        os.remove(os.path.join(d, f"{t}.csv"))
        have.add(t)
    need = set(TABLES) - have
    if need:
        if not os.path.exists(path):
            sys.exit(f"{path} not found (run: download); missing: {', '.join(sorted(need))}")
        say(f"reading {path}: keeping {', '.join(sorted(need))}")
        t0 = time.time()
        with tarfile.open(path, "r|gz") as tar:
            for m in tar:
                name = os.path.basename(m.name)
                t = name[:-4] if name.endswith(".csv") else None
                if t not in need or not m.isfile():
                    continue
                src = tar.extractfile(m)
                with gzip.open(csv_path(d, t) + ".part", "wb", 3) as dst:
                    shutil.copyfileobj(src, dst, 1 << 20)
                os.replace(csv_path(d, t) + ".part", csv_path(d, t))
                need.discard(t)
                say(f"   {t:<16} {mb(m.size):>9} -> {mb(os.path.getsize(csv_path(d, t))):>9} "
                    f"({time.time() - t0:.0f}s)")
                if not need:
                    break
        if need:
            sys.exit(f"not in {path}: {', '.join(sorted(need))}")
    for t in OTHER:                              # unpacked tables the workloads do not use
        p = os.path.join(d, f"{t}.csv")
        if os.path.exists(p):
            os.remove(p)
            say(f"   deleted {p}")
    if os.path.exists(path) and not keep_archive:
        os.remove(path)
        say(f"   deleted {path}")
    total = sum(os.path.getsize(csv_path(d, t)) for t in TABLES)
    say(f"kept the {len(TABLES)} tables in {d}: {mb(total)}")


# ---------------------------------------------------------------- import

def has_header(path: str) -> bool:
    with gzip.open(path, "rt", errors="replace") as f:
        first = f.readline().split(",", 1)[0].strip('"')
    return not first.isdigit()


def import_table(conn, d: str, table: str, schema: str = SCHEMA, remove: bool = False) -> None:
    ncols, keep, expected = TABLES[table]
    path = csv_path(d, table)
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found (run: download, extract)")
    cols = ", ".join(f"{c} {ty}" + (" PRIMARY KEY" if c == "id" and table in PRIMARY_KEY else "")
                     for c, _, ty in keep)
    stage = [f"c{i}" for i in range(1, ncols + 1)]
    say(f"importing {table} ({expected:,} rows expected) into {schema}.{table} ({', '.join(c for c, _, _ in keep)})")
    t0 = time.time()
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    conn.execute(f"CREATE TABLE {schema}.{table} ({cols})")
    # the staging table never stores a row: its trigger moves the kept columns to the table
    conn.execute(f"DROP TABLE IF EXISTS pg_temp.stage_{table}")
    conn.execute(f"CREATE TEMP TABLE stage_{table} ({', '.join(c + ' text' for c in stage)})")
    vals = ", ".join(f"NEW.c{pos}" + ("::integer" if ty == I else "") for _, pos, ty in keep)
    conn.execute(f"""CREATE OR REPLACE FUNCTION pg_temp.keep_{table}() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO {schema}.{table} VALUES ({vals}); RETURN NULL; END $$""")
    conn.execute(f"CREATE TRIGGER keep BEFORE INSERT ON pg_temp.stage_{table} "
                 f"FOR EACH ROW EXECUTE FUNCTION pg_temp.keep_{table}()")
    header = "true" if has_header(path) else "false"
    done, last = 0, time.time()
    size = os.path.getsize(path)
    with open(path, "rb") as raw, gzip.GzipFile(fileobj=raw) as f, conn.cursor().copy(
            f"COPY pg_temp.stage_{table} FROM STDIN WITH (FORMAT csv, ESCAPE '\\', NULL '', HEADER {header})") as cp:
        while chunk := f.read(1 << 20):
            cp.write(chunk)
            done += len(chunk)
            if time.time() - last > 10:
                last = time.time()
                say(f"   {100 * raw.tell() / size:3.0f}% of the file, {time.time() - t0:.0f}s")
    conn.execute(f"DROP TABLE pg_temp.stage_{table}")
    conn.execute("SET maintenance_work_mem TO '256MB'")
    for c in INDEXES.get(table, []):
        conn.execute(f"CREATE INDEX ON {schema}.{table} ({c})")
    conn.execute(f"VACUUM ANALYZE {schema}.{table}")
    n = conn.execute(f"SELECT count(*) FROM {schema}.{table}").fetchone()[0]
    sz = conn.execute(f"SELECT pg_total_relation_size('{schema}.{table}')").fetchone()[0]
    note = "" if n == expected else f"  (JOB's data has {expected:,}: a different version of the data?)"
    say(f"   {n:,} rows, {mb(sz)} with indexes, {time.time() - t0:.0f}s{note}")
    if remove:
        os.remove(path)
        say(f"   deleted {path}")


def missing_tables(conn, schema: str = SCHEMA, tables=None) -> list[str]:
    """The tables that are not imported, or lack a column (imported by an older version)."""
    have: dict[str, set] = {}
    for t, c in conn.execute(
            "SELECT c.relname, a.attname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped "
            "WHERE n.nspname = %s AND c.relkind = 'r'", (schema,)).fetchall():
        have.setdefault(t, set()).add(c)
    return [t for t in (tables or TABLES) if not {c for c, _, _ in TABLES[t][1]} <= have.get(t, set())]


def require(conn, schema: str = SCHEMA, tables=None) -> None:
    """For the benchmarks: the tables imported, or NotLoaded with what to run."""
    miss = missing_tables(conn, schema, tables)
    if miss:
        raise NotLoaded(f"schema {schema} lacks {', '.join(miss)} (or a column of them); the IMDb data is not "
                        f"downloaded by the benchmarks. Prepare it with benchmark/lib/imdb.py (download, extract, import "
                        f"{' '.join('--table ' + t for t in miss)}; see python3 benchmark/lib/imdb.py -h)")


# ---------------------------------------------------------------- status

def status(d: str, dsn: str | None, schema: str = SCHEMA) -> None:
    arch = os.path.join(d, ARCHIVE)
    say(f"directory {d} ({free_gb(d):.1f} GB free)")
    if os.path.exists(arch):
        say(f"   {ARCHIVE}: {mb(os.path.getsize(arch))}")
    elif os.path.exists(arch + ".part"):
        say(f"   {ARCHIVE}: partial, {mb(os.path.getsize(arch + '.part'))} (download resumes)")
    imported, missing_tables_cached = {}, {}
    if dsn is not None:
        import psycopg
        with psycopg.connect(dsn, autocommit=True) as conn:
            exists = {r[0] for r in conn.execute(
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relkind = 'r'", (schema,)).fetchall()}
            stale = set(missing_tables(conn, schema))
            for t in TABLES:
                missing_tables_cached[t] = t in stale
                if t in exists:
                    n = conn.execute(f"SELECT reltuples::bigint FROM pg_class WHERE oid = '{schema}.{t}'::regclass"
                                     ).fetchone()[0]
                    sz = conn.execute(f"SELECT pg_total_relation_size('{schema}.{t}')").fetchone()[0]
                    imported[t] = f"imported: ~{n:,} rows, {mb(sz)}"
    for t in TABLES:
        p = csv_path(d, t)
        f = f"{t}.csv.gz {mb(os.path.getsize(p))}" if os.path.exists(p) else "no file"
        db = imported.get(t, "not imported" if dsn is not None else "")
        if t in imported and missing_tables_cached.get(t):
            db += " (lacks a column: import again)"
        say(f"   {t:<16} {f:<28} {db}")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("download", "extract", "import", "status"):
        p = sub.add_parser(name)
        p.add_argument("--dir", default=DIR, help=f"data directory, outside pgpi (default {DIR})")
        if name == "download":
            p.add_argument("--url", default=URL)
        if name == "extract":
            p.add_argument("--keep-archive", action="store_true", help="do not delete imdb.tgz")
        if name in ("import", "status"):
            p.add_argument("--dsn", default=None if name == "status" else
                           (os.environ.get("PGPI_BENCH_DSN") or os.environ.get("DSN") or ""))
            p.add_argument("--schema", default=SCHEMA)
        if name == "import":
            p.add_argument("--table", action="append", choices=list(TABLES),
                           help="table to import (repeatable; default: every table whose file is there)")
            p.add_argument("--remove", action="store_true", help="delete each CSV file once its table is imported")
    a = ap.parse_args(argv)
    d = check_dir(a.dir)
    if a.cmd == "download":
        download(d, a.url)
    elif a.cmd == "extract":
        extract(d, a.keep_archive)
    elif a.cmd == "status":
        status(d, a.dsn, a.schema)
    else:
        import psycopg
        tables = a.table or [t for t in TABLES if os.path.exists(csv_path(d, t))]
        if not tables:
            sys.exit(f"no table files in {d} (run: download, extract)")
        with psycopg.connect(a.dsn, autocommit=True) as conn:
            for t in tables:
                import_table(conn, d, t, a.schema, a.remove)
            miss = missing_tables(conn, a.schema)
        say(f"all {len(TABLES)} tables are imported" if not miss else f"still to import: {', '.join(miss)}")


if __name__ == "__main__":
    main()
