"""Tests of the switchable model assumptions and CE feedback."""

import json
import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pgpi  # noqa: E402
from pgpi import assume as A  # noqa: E402
from pgpi.dataset import Example  # noqa: E402
from pgpi.request import Request  # noqa: E402


def test_combine_and_hints():
    s = [0.5, 0.1, 0.2, 0.9, 0.3]
    assert abs(A.combine(s, "independence") - math.prod(s)) < 1e-15
    assert abs(A.combine(s, "full") - 0.1) < 1e-15
    # sorted ascending, only the four most selective ones count: 0.1 * 0.2^(1/2) * 0.3^(1/4) * 0.5^(1/8)
    assert abs(A.combine(s, "partial") - 0.1 * 0.2 ** 0.5 * 0.3 ** 0.25 * 0.5 ** 0.125) < 1e-15
    m, hit = A.parse_hints("ASSUME_MIN_SELECTIVITY_FOR_FILTER_ESTIMATES, ASSUME_JOIN_PREDICATE_DEPENDS_ON_FILTERS",
                           A.POSTGRES)
    assert hit and m == A.Model("full", "simple")
    assert A.parse_hints("FORCE_LEGACY_CARDINALITY_ESTIMATION", A.POSTGRES)[0] == A.Model("independence", "simple")
    assert A.parse_hints("force_default_cardinality_estimation", A.POSTGRES)[0] == A.Model("partial", "base")
    assert A.parse_hints("correlation=partial other=1", A.POSTGRES)[0] == A.Model("partial", "base")
    assert A.parse_hints("something else", A.POSTGRES) == (A.POSTGRES, False)


def test_containment_factor():
    # filters on the join keys themselves (o.id and l.order_id in the same range):
    # simple containment finds the matching rows, base containment divides by all distinct values
    key = A.containment_factor(A.JoinSide(500000, 500000, 934 / 500000, 1.0),
                               A.JoinSide(368069, 1.5e6, 2904 / 1.5e6, 1.0))
    assert 500 < key < 560          # PostgreSQL's 5.4 rows -> about 2900 (true 2998)
    # filters on other columns: simple containment overestimates (the true value is base)
    other = A.containment_factor(A.JoinSide(500000, 500000, 1.0, 0.099), A.JoinSide(368069, 1.5e6, 1.0, 0.1095))
    assert 3 < other < 4
    assert A.containment_factor(A.JoinSide(100, 1000, 1, 1), A.JoinSide(100, 1000, 1, 1)) == 1.0


STATS = {"reltuples": 1000, "columns": [
    {"attname": "a", "type": "text", "null_frac": 0, "n_distinct": 4, "inh": False,
     "slots": [{"kind": 1, "values": ["a0", "a1", "a2", "a3"], "numbers": [0.4, 0.3, 0.2, 0.1]}]},
    {"attname": "b", "type": "text", "null_frac": 0, "n_distinct": 4, "inh": False,
     "slots": [{"kind": 1, "values": ["b0", "b1", "b2", "b3"], "numbers": [0.4, 0.3, 0.2, 0.1]}]},
    {"attname": "id", "type": "integer", "null_frac": 0, "n_distinct": -1, "inh": False,
     "slots": [{"kind": 2, "values": ["1", "250", "500", "750", "1000"]}]},
]}


class Rows:
    def __init__(self, rows):
        self.rows = rows

    def to_pylist(self):
        return self.rows


class FakeDataset:
    def __init__(self, exs=(), times=None, sources=None):
        self.exs = list(exs)
        self.times = times or {}
        self.sources = sources or {}

    def stats(self, columns=None, filter=None):
        return Rows([{"rel": t, "captured_at": "2026-01-01", "stats": json.dumps(STATS)}
                     for t in ("public.t", "public.u")])

    def examples(self, points=None, usable_only=True, **kw):
        return iter(self.exs)

    def plans(self, columns=None, filter=None):
        return Rows([{"seqid": s, "queryid": 7, "execution_ms": ms} for s, ms in self.times.items()])

    def nodes(self, columns=None, filter=None):
        return Rows([{"seqid": s, "est_source": v} for s, v in self.sources.items()])


T = {"alias": "t", "rel": "public.t", "rti": 1}
U = {"alias": "u", "rel": "public.u", "rti": 2}


def scan(a="a0", b="b0", hints=None, queryid=7):
    return {"point": "base", "rels": [T], "outer": [], "outer_clauses": [], "opaque": 0, "jointype": 0,
            "clauses": [{"sql": f"(t.a = '{a}'::text)", "rels": "t"}, {"sql": f"(t.b = '{b}'::text)", "rels": "t"}],
            "tuples": 1000, "hints": hints, "queryid": queryid}


def test_fixed_assumptions_scans_hints_and_joins():
    est = pgpi.load("ce-feedback:correlation=full")     # fixed assumptions: statistics only, no store
    est.load_stats(FakeDataset())
    # full correlation: min(0.4, 0.4) instead of 0.4 * 0.4
    a = est.estimate(Request(scan()))
    assert abs(a["factor"] - 2.5) < 1e-9 and a["source"] == "default:full/base"
    # a hint overrides the default
    a = est.estimate(Request(scan(hints="ASSUME_FULL_INDEPENDENCE_FOR_FILTER_ESTIMATES")))
    assert a is None                                  # independence = PostgreSQL: abstain
    # containment: a key range on both sides of t.id = u.id
    j = {"point": "join", "rels": [T, U], "outer": [], "outer_clauses": [], "opaque": 0, "jointype": 0,
         "clauses": [{"sql": "(t.id = u.id)", "rels": "t u"},
                     {"sql": "(t.id < '100'::integer)", "rels": "t"},
                     {"sql": "(u.id < '100'::integer)", "rels": "u"}]}
    simple = pgpi.load("ce-feedback:containment=simple")
    simple.load_stats(FakeDataset())
    f_all = simple.estimate(Request(j))["factor"]
    assert 8 < f_all < 12                             # 1000 distinct values -> about 100
    # a live request: the join clause is between the inputs (split t | u)
    assert abs(simple.estimate(Request(dict(j, split="t")))["factor"] - f_all) < 1e-9
    # three relations, the t-u clause inside the outer input (t u | v): only u.a = v.a counts.
    # With 4 values that survive any filter, it changes nothing.
    V = {"alias": "v", "rel": "public.u", "rti": 3}
    j3 = dict(j, rels=[T, U, V], clauses=j["clauses"] + [{"sql": "(u.a = v.a)", "rels": "u v"}], split="t u")
    assert simple.estimate(Request(j3)) is None
    # offline (no split, the observation frame): all clauses count
    j3_obs = {k: v for k, v in j3.items() if k != "split"}
    assert abs(simple.estimate(Request(j3_obs))["factor"] - f_all) < 1e-6


def ex(seqid, est, true=400, hints=None):
    return Example(request=Request(scan(hints=hints)), true_rows=true, est_pg=None, est_plan=est,
                   seqid=seqid, node_id=0, label="t", queryid=7, planid=0, censored=False, nloops=1)


def test_ce_feedback_rounds(tmp_path):
    store = str(tmp_path / "hints.json")
    spec = f"ce-feedback:store={store}"
    fb = pgpi.load(spec)
    # round 0: PostgreSQL underestimates 20x -> one step toward correlation
    r0 = [ex(1, 20), ex(2, 20)]
    fb.fit(FakeDataset(r0, {1: 10.0, 2: 10.0}))
    assert fb.store["7"]["candidate"] == "partial/base"
    assert fb.model_for(Request(scan()))[0] == A.Model("partial", "base")
    # round 1: better and not slower -> kept
    r1 = [ex(3, 90), ex(4, 90)]
    fb.fit(FakeDataset(r0 + r1, {1: 10.0, 2: 10.0, 3: 9.0, 4: 11.0}))
    assert fb.store["7"]["model"] == "partial/base" and fb.store["7"]["candidate"] is None
    # round 2: still 4x under -> try full correlation
    r2 = [ex(5, 90), ex(6, 90)]
    fb.fit(FakeDataset(r0 + r1 + r2, {5: 10.0, 6: 10.0}))
    assert fb.store["7"]["candidate"] == "full/base"
    # round 3: estimates are right but much slower -> rejected, never tried again
    r3 = [ex(7, 400), ex(8, 400)]
    fb.fit(FakeDataset(r0 + r1 + r2 + r3, {7: 40.0, 8: 40.0}))
    e = fb.store["7"]
    assert e["model"] == "partial/base" and "full/base" in e["rejected"] and e["candidate"] is None
    r4 = [ex(9, 90), ex(10, 90)]
    fb.fit(FakeDataset(r0 + r1 + r2 + r3 + r4, {9: 10.0, 10: 10.0}))
    assert fb.store["7"]["candidate"] is None
    # the store persists, so a new estimator continues from it
    fb2 = pgpi.load(spec)
    assert fb2.model_for(Request(scan()))[0] == A.Model("partial", "base")
    # a query with hints is not touched by the feedback
    assert fb2.model_for(Request(scan(hints="correlation=full")))[1] == "hint"


def test_ce_feedback_overestimate_and_hinted(tmp_path):
    fb = pgpi.load(f"ce-feedback:store={tmp_path / 'h.json'}")
    # overestimate at independence: no step left toward independence
    fb.fit(FakeDataset([ex(1, 400, true=20)], {1: 5.0}))
    assert fb.store["7"]["candidate"] is None
    # executions that ran with hints are ignored
    fb3 = pgpi.load(f"ce-feedback:store={tmp_path / 'h3.json'}")
    fb3.fit(FakeDataset([ex(1, 20)], {1: 5.0}, sources={1: "hint:full/base"}))
    assert "7" not in fb3.store or fb3.store["7"]["candidate"] is None
