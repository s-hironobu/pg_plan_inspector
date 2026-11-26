"""The STATS data set (the simplified version used by STATS-CEB), loaded into schema ``stats``.

The data is not bundled with pgpi. It is downloaded once from the STATS-CEB
repository (End-to-End-CardEst-Benchmark, Han et al., PVLDB 15, 2022) at a fixed
commit, into a local cache. Then it is copied into the database with COPY FROM STDIN
(no server-side files).
"""

from __future__ import annotations

import os
import urllib.request

from . import CACHE_BASE

REPO = "https://raw.githubusercontent.com/ZhengtongYan/End-to-End-CardEst-Benchmark"
COMMIT = "a7b201f4a3a6e7ebc3e250fccb63723adb48d6be"
CACHE = os.path.join(CACHE_BASE, "stats")

# (table, csv file, columns). The columns are in the order of the CSV.
TABLES = [
    ("users", "users.csv", "id integer PRIMARY KEY, reputation integer, creationdate timestamp, views integer, "
                           "upvotes integer, downvotes integer"),
    ("posts", "posts.csv", "id integer PRIMARY KEY, posttypeid smallint, creationdate timestamp, score integer, "
                           "viewcount integer, owneruserid integer, answercount integer, commentcount integer, "
                           "favoritecount integer, lasteditoruserid integer"),
    ("postlinks", "postLinks.csv", "id integer PRIMARY KEY, creationdate timestamp, postid integer, "
                                   "relatedpostid integer, linktypeid smallint"),
    ("posthistory", "postHistory.csv", "id integer PRIMARY KEY, posthistorytypeid smallint, postid integer, "
                                       "creationdate timestamp, userid integer"),
    ("comments", "comments.csv", "id integer PRIMARY KEY, postid integer, score smallint, creationdate timestamp, "
                                 "userid integer"),
    ("votes", "votes.csv", "id integer PRIMARY KEY, postid integer, votetypeid smallint, creationdate timestamp, "
                           "userid integer, bountyamount smallint"),
    ("badges", "badges.csv", "id integer PRIMARY KEY, userid integer, date timestamp"),
    ("tags", "tags.csv", "id integer PRIMARY KEY, count integer, excerptpostid integer"),
]

# the indexes of STATS-CEB (scripts/sql/stats_index.sql)
INDEXES = [("posts", "owneruserid"), ("posts", "lasteditoruserid"), ("postlinks", "relatedpostid"),
           ("postlinks", "postid"), ("posthistory", "postid"), ("posthistory", "userid"),
           ("comments", "postid"), ("comments", "userid"), ("votes", "userid"), ("votes", "postid"),
           ("badges", "userid"), ("tags", "excerptpostid")]


def download(say=print) -> str:
    """Fetch the CSV files into the cache (once) and return the cache directory."""
    os.makedirs(CACHE, exist_ok=True)
    for _, fname, _ in TABLES:
        path = os.path.join(CACHE, fname)
        if os.path.exists(path):
            continue
        url = f"{REPO}/{COMMIT}/datasets/stats_simplified/{fname}"
        say(f"   downloading {url}")
        tmp = path + ".part"
        urllib.request.urlretrieve(url, tmp)
        os.replace(tmp, path)
    return CACHE


def load(conn, say=print) -> None:
    """(Re)create schema stats and load it. It has about 1 million rows (40 MB of CSV)."""
    cache = download(say)
    conn.execute("DROP SCHEMA IF EXISTS stats CASCADE")
    conn.execute("CREATE SCHEMA stats")
    for table, fname, cols in TABLES:
        conn.execute(f"CREATE TABLE stats.{table} ({cols})")
        with open(os.path.join(cache, fname), "rb") as f, \
                conn.cursor().copy(f"COPY stats.{table} FROM STDIN WITH (FORMAT csv, HEADER true)") as cp:
            while chunk := f.read(1 << 20):
                cp.write(chunk)
    for table, col in INDEXES:
        conn.execute(f"CREATE INDEX ON stats.{table} ({col})")
    for table, _, _ in TABLES:
        conn.execute(f"VACUUM ANALYZE stats.{table}")
