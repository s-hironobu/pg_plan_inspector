"""Anonymization of collected data, applied before anything is written.

Two levels:

``names``  Every identifier (schemas, tables, aliases, columns, indexes,
           subquery names, user and database names) becomes a salted hash
           token. Query text, bound parameters and EXPLAIN text are dropped.
           Literals and statistics values are kept.
``full``   ``names`` plus literals and statistics values (MCV, histogram
           bounds) replaced by salted hash tokens. Frequencies, null
           fractions, n_distinct and all row counts are kept.

Tokens are consistent within a salt. The same table, column or value always
maps to the same token. So equalities survive (joins of keys, MCV matches,
template grouping), and a model trained on the anonymized dataset can be
evaluated on it. Order does not survive at the ``full`` level.

The salt is never stored. The manifest records only a short fingerprint.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from typing import Any

_QUOTED = r'"(?:[^"]|"")*"'
_TOKEN = re.compile(r"""
    (?P<lit>'(?:[^']|'')*')
  | (?P<qid>"(?:[^"]|"")*")
  | (?P<word>[A-Za-z_][A-Za-z0-9_$]*)
  | (?P<num>\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)
  | (?P<cast>::)
  | (?P<dot>\.)
  | (?P<ws>\s+)
  | (?P<other>.)
""", re.VERBOSE | re.DOTALL)
_QUALIFIED_PART = re.compile(_QUOTED + r'|[^."]+')


def _unquote(s: str) -> str:
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        return s[1:-1].replace('""', '"')
    return s


def _split_qualified(name: str) -> list[str]:
    return [_unquote(p) for p in _QUALIFIED_PART.findall(name)]


class Anonymizer:
    LEVELS = ("names", "full")

    def __init__(self, salt: str, level: str = "full"):
        if level not in self.LEVELS:
            raise ValueError(f"level must be one of {self.LEVELS}")
        if not salt:
            raise ValueError("a salt is required")
        self._key = salt.encode()
        self.level = level

    def describe(self) -> dict:
        fp = hmac.new(self._key, b"pgpi-fingerprint", hashlib.sha256).hexdigest()[:12]
        return {"level": self.level, "salt_fingerprint": fp}

    # ----------------------------------------------------------- tokens

    def token(self, kind: str, value: str | None) -> str | None:
        if value is None:
            return None
        h = hmac.new(self._key, f"{kind}\x00{value}".encode(), hashlib.sha256).hexdigest()[:10]
        return f"{kind}_{h}"

    def rel(self, qualified: str | None) -> str | None:
        """Convert schema.table to s_xxx.t_xxx. The table token depends on the full name."""
        if qualified is None:
            return None
        parts = _split_qualified(qualified)
        if len(parts) == 2:
            return f"{self.token('s', parts[0])}.{self.token('t', qualified)}"
        return self.token("t", qualified)

    def column(self, qualified_rel: str | None, col: str) -> str:
        return self.token("c", f"{qualified_rel}.{col}")

    def value(self, text: str) -> str:
        return self.token("v", text)

    # ----------------------------------------------------------- SQL clauses

    def clause_sql(self, sql: str | None, alias_rel: dict[str, str | None]) -> str | None:
        """Rewrite a deparsed clause: alias.column and, at 'full', literals."""
        if sql is None:
            return None
        toks = [(m.lastgroup, m.group()) for m in _TOKEN.finditer(sql)]
        out = []
        i = 0
        while i < len(toks):
            kind, text = toks[i]
            if kind in ("word", "qid") and i + 2 < len(toks) and toks[i + 1][0] == "dot" \
                    and toks[i + 2][0] in ("word", "qid") and _unquote(text) in alias_rel \
                    and (i == 0 or toks[i - 1][0] != "cast"):
                alias = _unquote(text)
                col = _unquote(toks[i + 2][1])
                out.append(self.token("a", alias))
                out.append(".")
                out.append(self.column(alias_rel[alias], col))
                i += 3
                continue
            if kind == "lit" and self.level == "full":
                out.append("'" + self.value(text[1:-1].replace("''", "'")) + "'")
            else:
                out.append(text)
            i += 1
        return "".join(out)

    # ----------------------------------------------------------- descriptors

    def request(self, d: dict | None) -> dict | None:
        """Anonymize a request or a node descriptor (they have the same format)."""
        if d is None:
            return None
        d = dict(d)
        alias_rel = {r.get("alias"): r.get("rel") for r in d.get("rels", []) + d.get("outer", [])}

        def rel_json(r):
            r = dict(r)
            r["alias"] = self.token("a", r.get("alias"))
            r["rel"] = self.rel(r.get("rel"))
            return r

        def clause_json(c):
            c = dict(c)
            c["sql"] = self.clause_sql(c.get("sql"), alias_rel)
            c["rels"] = " ".join(self.token("a", a) for a in (c.get("rels") or "").split())
            return c

        d["rels"] = [rel_json(r) for r in d.get("rels", [])]
        d["outer"] = [rel_json(r) for r in d.get("outer", [])]
        d["clauses"] = [clause_json(c) for c in d.get("clauses", [])]
        d["outer_clauses"] = [clause_json(c) for c in d.get("outer_clauses", [])]
        if d.get("plan_name") is not None:
            d["plan_name"] = self.token("q", d["plan_name"])
        return d

    def ident(self, s: str | None) -> str | None:
        """Anonymize a pgpi relation identifier: alias#n/schema.part@subquery."""
        if s is None:
            return None
        m = re.match(r'^((?:"(?:[^"]|"")*"|[^#/@"])+)(#\d+)?(?:/((?:"(?:[^"]|"")*"|[^@"])+))?(?:@(.+))?$', s)
        if not m:
            return self.token("x", s)
        out = self.token("a", _unquote(m.group(1)))
        if m.group(2):
            out += m.group(2)
        if m.group(3):
            out += "/" + self.rel(m.group(3))
        if m.group(4):
            out += "@" + self.token("q", _unquote(m.group(4)))
        return out

    def index_name(self, qualified: str) -> str:
        parts = _split_qualified(qualified)
        if len(parts) == 2:
            return f"{self.token('s', parts[0])}.{self.token('i', qualified)}"
        return self.token("i", qualified)

    # ----------------------------------------------------------- records

    def plan_record(self, rec: dict[str, Any]) -> dict[str, Any]:
        rec = dict(rec)
        rec["query"] = None
        rec["params"] = None
        rec["explain"] = None
        rec["database"] = self.token("d", rec.get("database"))
        rec["username"] = self.token("u", rec.get("username"))
        nodes = []
        for n in rec.get("nodes") or []:
            n = dict(n)
            n["ident"] = [self.ident(x) for x in n.get("ident") or []]
            if n.get("ce") is not None:
                n["ce"] = self.request(n["ce"])
            if n.get("index") is not None:
                ix = dict(n["index"])
                ix["name"] = self.token("i", ix.get("name"))
                n["index"] = ix
            if n.get("elided") is not None:
                n["elided"] = n["elided"]
            nodes.append(n)
        rec["nodes"] = nodes
        meta = dict(rec.get("meta") or {})
        if "rtable" in meta:
            rt = []
            for r in meta["rtable"]:
                r = dict(r)
                r["ident"] = self.ident(r.get("ident"))
                r["alias"] = self.token("a", r.get("alias"))
                r["rel"] = self.rel(r.get("rel"))
                if r.get("plan_name") is not None:
                    r["plan_name"] = self.token("q", r["plan_name"])
                rt.append(r)
            meta["rtable"] = rt
        if "subplans" in meta:
            meta["subplans"] = [dict(s, name=self.token("q", s.get("name"))) for s in meta["subplans"]]
        if "stats_refs" in meta:
            meta["stats_refs"] = [dict(s, rel=self.rel(s.get("rel"))) for s in meta["stats_refs"]]
        if "requests" in meta:
            meta["requests"] = [dict(r, request=self.request(r.get("request"))) for r in meta["requests"]]
        rec["meta"] = meta
        return rec

    def stats_record(self, r: dict[str, Any]) -> dict[str, Any]:
        r = dict(r)
        rel = r.get("rel")
        r["database"] = self.token("d", r.get("database"))
        r["rel"] = self.rel(rel)
        stats = json.loads(r["stats"]) if isinstance(r.get("stats"), str) else (r.get("stats") or {})
        r["stats"] = json.dumps(self._stats(stats), separators=(",", ":"))
        return r

    def _columns(self, cols: list[dict], rel: str | None) -> list[dict]:
        out = []
        for c in cols:
            c = dict(c)
            if c.get("attname") is not None:
                c["attname"] = self.column(rel, c["attname"])
            if self.level == "full":
                slots = []
                for s in c.get("slots", []):
                    s = dict(s)
                    if s.get("values") is not None:
                        s["values"] = [None if v is None else self.value(v) for v in s["values"]]
                    slots.append(s)
                c["slots"] = slots
            out.append(c)
        return out

    def _stats(self, st: dict) -> dict:
        st = dict(st)
        rel = st.get("name")
        st["name"] = self.rel(rel)
        st["columns"] = self._columns(st.get("columns", []), rel)
        st["extended"] = [dict(e, name=self.token("e", e.get("name"))) for e in st.get("extended", [])]
        idx = []
        for i in st.get("indexes", []):
            i = dict(i)
            iname = i.get("name")
            i["name"] = self.index_name(iname) if iname else None
            i["columns"] = self._columns(i.get("columns", []), iname)
            idx.append(i)
        st["indexes"] = idx
        return st
