"""LiteCard plugin: graph, patterns, levels, fallback, regressors, bias correction (no database)."""

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest  # noqa: E402

import pgpi  # noqa: E402
from pgpi.estimator_plugins import litecard  # noqa: E402
from pgpi.request import Request  # noqa: E402

T = {"alias": "t", "rel": "public.t", "relid": 1, "rti": 1}
U = {"alias": "u", "rel": "public.u", "relid": 2, "rti": 2}


def base(col, op, v, pg=100.0):
    return Request({"point": "base", "rels": [T], "outer": [], "outer_clauses": [], "opaque": 0, "nonrel": False,
                    "jointype": 0, "pg": pg, "clauses": [{"sql": f"(t.{col} {op} '{v}'::integer)", "rels": "t"}]})


def join(v, pg=50.0):
    return Request({"point": "join", "rels": [T, U], "outer": [], "outer_clauses": [], "opaque": 0, "nonrel": False,
                    "jointype": 0, "pg": pg, "clauses": [{"sql": f"(t.a < '{v}'::integer)", "rels": "t"},
                                                         {"sql": "(t.id = u.t_id)", "rels": "t u"}]})


class DS:
    def __init__(self, pairs):
        self.pairs = pairs

    def examples(self):
        return [SimpleNamespace(request=r, true_rows=y) for r, y in self.pairs]


def test_registered():
    assert "litecard" in pgpi.registered()


def fitted(pairs):
    e = pgpi.load("litecard", {})
    e.fit(DS(pairs))
    return e


def test_levels_and_fallback(monkeypatch):
    monkeypatch.setattr(litecard, "MODEL", "rbf")
    monkeypatch.setattr(litecard, "BETA", {3: 5, 2: 12, 1: 30})
    # t.a < v returns 10 v rows: 8 observations (enough for H3). t.a > v: 4 (not enough).
    pairs = [(base("a", "<", v), 10.0 * v) for v in range(1, 9)] + [(base("a", ">", v), 100.0 - 10 * v) for v in (1, 3, 5, 7)]
    e = fitted(pairs)
    a = e.estimate(base("a", "<", 4))
    assert a["source"] == "litecard-h3" and abs(a["rows"] - 40) < 5          # near the neighbours of v = 4
    b = e.estimate(base("a", ">", 3))                                          # H3 has 4 < 5, H2 has 12: falls back
    assert b["source"] == "litecard-h2" and abs(b["rows"] - 70) < 15           # the operator is a feature there
    assert e.estimate(base("b", "<", 4)) is None                               # another column: H1 has 12 < 30
    monkeypatch.setattr(litecard, "BETA", {3: 5, 2: 12, 1: 12})
    assert e.estimate(base("b", "<", 4))["source"] == "litecard-h1"            # same table and column type


def test_models_and_target(monkeypatch):
    monkeypatch.setattr(litecard, "BETA", {3: 5, 2: 50, 1: 100})
    e = fitted([(base("a", "<", v), 10.0 * v) for v in range(1, 9)])
    monkeypatch.setattr(litecard, "MODEL", "lr")
    assert abs(e.estimate(base("a", "<", 4))["rows"] - 40) < 2                # locally linear: exact on a line
    monkeypatch.setattr(litecard, "MODEL", "rbf")
    monkeypatch.setattr(litecard, "TARGET_LOG", True)
    assert abs(e.estimate(base("a", "<", 4))["rows"] - 40) < 5
    pytest.importorskip("ydf")
    monkeypatch.setattr(litecard, "MODEL", "gbdt")
    monkeypatch.setattr(litecard, "TARGET_LOG", False)
    assert abs(e.estimate(base("a", "<", 4))["rows"] - 40) < 15                # the trees, weighted by the kernel


def test_bias_correction(monkeypatch):
    e = fitted([])
    assert e.estimate(base("a", "<", 4)) is None                               # only joins are corrected
    answers = [e.estimate(join(v)) for v in range(200)]
    hit = [a for a in answers if a]
    assert all(a == {"rows": 50.0 * 1.57, "source": "litecard-bias"} for a in hit)  # 1 join: Table 5
    assert 0.4 < len(hit) / 200 < 0.75                                         # about 0.57 of the questions
    monkeypatch.setattr(litecard, "BIAS_RANDOM", False)
    assert e.estimate(join(7)) == e.estimate(join(7))                          # derived from the question
    monkeypatch.setattr(litecard, "BIAS_CORRECTION", False)
    assert all(e.estimate(join(v)) is None for v in range(20))


V = {"alias": "v", "rel": "public.v", "relid": 3, "rti": 3}


def live(rels, clauses, pg, split=None, outer_rows=None, inner_rows=None):
    d = {"point": "join" if len(rels) > 1 else "base", "rels": rels, "outer": [], "outer_clauses": [],
         "opaque": 0, "nonrel": False, "jointype": 0, "pg": pg, "queryid": 42,
         "clauses": [{"sql": s, "rels": r} for s, r in clauses]}
    if split is not None:
        d.update(split=split, outer_rows=outer_rows, inner_rows=inner_rows)
    return Request(d)


def test_bias_correction_does_not_stack(monkeypatch):
    """A join is corrected from PostgreSQL's estimate with uncorrected inputs."""
    monkeypatch.setattr(litecard, "BIAS_TABLE", {1: (1.0, 10.0), 2: (1.0, 100.0)})   # always correct
    e = fitted([])
    rt, ru, rv = ("(t.a < '5'::integer)", "t"), ("(u.b < '5'::integer)", "u"), ("(v.c < '5'::integer)", "v")
    jtu, juv = ("(t.id = u.t_id)", "t u"), ("(u.id = v.u_id)", "u v")
    assert e.estimate(live([T], [rt], 100.0)) is None
    assert e.estimate(live([U], [ru], 200.0)) is None
    assert e.estimate(live([V], [rv], 300.0)) is None
    tu = e.estimate(live([T, U], [rt, ru, jtu], 50.0, "t", 100.0, 200.0))
    assert tu == {"rows": 500.0, "source": "litecard-bias"}                    # 50 x 10
    # PostgreSQL estimates t u v from the corrected t u (500 rows): 500 x 300 x sel = 1500.
    # From the uncorrected t u, this is 150. Corrected: 150 x 100, not 1500 x 100.
    tuv = e.estimate(live([T, U, V], [rt, ru, rv, jtu, juv], 1500.0, "t u", 500.0, 300.0))
    assert tuv == {"rows": 15000.0, "source": "litecard-bias"}
    # inputs that it never saw: no correction
    assert e.estimate(live([T, U, V], [rt, ru, ("(v.c < '9'::integer)", "v"), jtu, juv],
                           1500.0, "t u", 500.0, 300.0)) is None


def clause(sql, rels):
    return {"sql": sql, "rels": rels}


def scan(clauses):
    return Request({"point": "base", "rels": [T], "outer": [], "outer_clauses": [], "opaque": 0, "nonrel": False,
                    "jointype": 0, "pg": 10.0, "clauses": clauses})


def test_graph_patterns_and_features():
    stats = litecard.Stats(None)
    # IN lists (= ANY), LIKE, dates: the pattern ignores the literals, the features hold them
    a = scan([clause("(t.k = ANY ('{1,2}'::integer[]))", "t"), clause("((t.s)::text ~~ 'Ab%'::text)", "t"),
              clause("(t.d >= '2010-03-04'::date)", "t")])
    b = scan([clause("(t.d >= '1999-12-31'::date)", "t"), clause("(t.k = ANY ('{7,8}'::integer[]))", "t"),
              clause("((t.s)::text ~~ 'Zz%'::text)", "t")])
    ka, xa = litecard.describe(a, stats)
    kb, xb = litecard.describe(b, stats)
    assert ka == kb                                       # the same pattern at every level, in any clause order
    assert len(xa[3]) == len(xb[3]) == 2 + 3 + 3          # two numbers (f1), three characters (f4), a date (f5)
    assert sorted(xa[3]) != sorted(xb[3])
    assert 2010.0 in xa[3] and 3.0 in xa[3] and float(ord("A")) in xa[3]
    # another list length or another operator: another pattern at H3
    c = scan([clause("(t.k = ANY ('{1,2,3}'::integer[]))", "t"), clause("((t.s)::text ~~ 'Ab%'::text)", "t"),
              clause("(t.d >= '2010-03-04'::date)", "t")])
    assert litecard.describe(c, stats)[0][3] != ka[3]
    d = scan([clause("(t.k = ANY ('{1,2}'::integer[]))", "t"), clause("((t.s)::text ~~ 'Ab%'::text)", "t"),
              clause("(t.d < '2010-03-04'::date)", "t")])
    kd = litecard.describe(d, stats)[0]
    assert kd[3] != ka[3] and kd[2] == ka[2]              # the operator is hashed at H3 only


def test_features_from_statistics():
    class Cat:
        by_rel = {"public.t": {"reltuples": 1000, "columns": [
            {"attname": "a", "type": "integer", "n_distinct": -0.5, "inh": False,
             "slots": [{"kind": 2, "values": ["0", "50", "100"]}]}]}}
    stats = litecard.Stats(Cat())
    k, x = litecard.describe(base("a", "<", 25), stats)
    assert x[3] == [0.0, 0.25]                            # f3 for <: [0, f2]
    assert x[2] == [1.0, 0.0, 0.0, 0.0, 0.25]             # f8 of < first (the op node sorts first here) + f3
    assert sorted(x[1]) == sorted([500.0, 1.0, 0.0, 0.0, 0.0, 0.25])   # numUniques = 0.5 * 1000
