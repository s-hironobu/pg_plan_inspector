"""The SQL of CEB-IMDb queries, downloaded once and kept in a local cache.

CEB (Negi et al., "Flow-Loss: Learning Cardinality Estimates That Matter", PVLDB 14,
2021; https://github.com/learnedsystems/CEB) distributed its SQL through links that
no longer work. The queries are taken from a copy in imdb_pg_dataset
(https://github.com/RyanMarcus/imdb_pg_dataset, ceb-imdb-13k). The author of this
copy is one of the authors of CEB. The queries are taken at a fixed commit, file by
file. Only the queries that a benchmark uses are downloaded. They are not bundled
with pgpi. If you use them, cite:

    Parimarjan Negi, Ryan Marcus, Andreas Kipf, Hongzi Mao, Nesime Tatbul, Tim Kraska,
    and Mohammad Alizadeh. 2021. Flow-loss: learning cardinality estimates that matter.
    Proc. VLDB Endow. 14, 11 (July 2021), 2019-2032.
"""

from __future__ import annotations

import concurrent.futures
import os
import re
import time
import urllib.error
import urllib.request

from . import CACHE_BASE

COMMIT = "1f39e9aa85ee64249f60bfa59543e8707b228644"
BASE = f"https://raw.githubusercontent.com/RyanMarcus/imdb_pg_dataset/{COMMIT}/ceb-imdb-13k"
CACHE = os.path.join(CACHE_BASE, "ceb")


def fetch(names: list[str], say=print) -> dict[str, str]:
    """Return {name: SQL on one line} for query names such as '1a17'. Download them if they are not cached."""
    def one(name):
        tpl = re.match(r"\d+[a-z]", name).group(0)
        path = os.path.join(CACHE, tpl, name + ".sql")
        if not os.path.exists(path):
            sql = _get(f"{BASE}/{tpl}/{name}.sql")
            if not sql.lstrip().upper().startswith("SELECT"):
                raise RuntimeError(f"{name}: not a query: {sql[:80]!r}")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path + ".part", "w") as f:
                f.write(sql)
            os.replace(path + ".part", path)
        with open(path) as f:
            return name, " ".join(f.read().split()).rstrip(";")

    missing = [n for n in names if not os.path.exists(os.path.join(CACHE, re.match(r"\d+[a-z]", n).group(0), n + ".sql"))]
    if missing:
        say(f"   downloading {len(missing)} CEB queries into {CACHE} (once)")
    with concurrent.futures.ThreadPoolExecutor(4) as ex:
        out = dict(ex.map(one, names))
    if missing:
        say("   downloaded")
    return out


def _get(url: str, tries: int = 6) -> str:
    """Return the text at url. Retry with backoff, because GitHub resets connections when asked too often."""
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                return r.read().decode()
        except urllib.error.HTTPError as e:
            if e.code not in (429, 500, 502, 503, 504) or i == tries - 1:
                raise
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            if i == tries - 1:
                raise
        time.sleep(2 ** i)                  # 1, 2, 4, 8, 16 s
    raise AssertionError("unreachable")


def workload(listing: str, n_a: int, n_b: int, say=print) -> tuple[str, str]:
    """Write train.sql and test.sql (in the cache) from a listing of 'kind name pair' lines.

    train.sql has every training query (bench takes the first N). test.sql has the
    first n_a queries of A and the first n_b queries of B."""
    rows = [line.split() for line in open(listing) if line.strip() and not line.startswith("#")]
    train = [r for r in rows if r[0] == "train"]
    test = [r for r in rows if r[0] == "A"][:n_a] + [r for r in rows if r[0] == "B"][:n_b]
    sql = fetch([r[1] for r in train + test], say)
    out = os.path.join(CACHE, os.path.splitext(os.path.basename(listing))[0] + f"-{n_a}-{n_b}")
    os.makedirs(out, exist_ok=True)
    paths = []
    for name, rs in (("train.sql", train), ("test.sql", test)):
        p = os.path.join(out, name)
        with open(p, "w") as f:
            for kind, qname, pair in rs:
                f.write(f"-- {kind} {qname}\n{sql[qname]};\n")
        paths.append(p)
    return paths[0], paths[1]
