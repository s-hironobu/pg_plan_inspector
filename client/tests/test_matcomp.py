"""Tests of matcomp (join coupling as matrix completion)."""

import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pgpi  # noqa: E402
from pgpi.dataset import Example  # noqa: E402
from pgpi.request import Request  # noqa: E402

A = {"alias": "a", "rel": "public.a", "rti": 1}
B = {"alias": "b", "rel": "public.b", "rti": 2}
N, ROWS_A, ROWS_B, K = 6, 1000, 2000, 500


def sign(i):
    return 1 if i < N // 2 else -1


def true_join(i, j):
    """|a(r=i) join b(c=j)|: uniform part times 1 + a rank-1 tilt product."""
    return ROWS_A * ROWS_B / K * (1 + 0.8 * sign(i) * sign(j))


def join_request(i, j):
    return {"point": "join", "rels": [A, B], "jointype": 0,
            "clauses": [{"sql": f"(a.r = '{i}'::integer)", "rels": "a"},
                        {"sql": f"(b.c = '{j}'::integer)", "rels": "b"},
                        {"sql": "(a.k = b.k)", "rels": "a b"}]}


class Rows:
    def __init__(self, rows):
        self.rows = rows

    def to_pylist(self):
        return self.rows


class FakeDataset:
    """One Hash Join of a and b per cell (i, j) of the grid."""

    def __init__(self, grid_cells):
        self.exs, self.nodes_ = [], []
        for s, (i, j) in enumerate(grid_cells):
            seq = 100 + s

            def mk(d, rows, nid, seq=seq):
                req = Request(d)
                req.pg = rows
                return Example(request=req, true_rows=rows, est_pg=rows, est_plan=rows, seqid=seq,
                               node_id=nid, label="t", queryid=1, planid=1)
            ba = {"point": "base", "rels": [A], "clauses": [join_request(i, j)["clauses"][0]]}
            bb = {"point": "base", "rels": [B], "clauses": [join_request(i, j)["clauses"][1]]}
            self.exs += [mk(join_request(i, j), true_join(i, j), 1), mk(ba, ROWS_A, 2), mk(bb, ROWS_B, 3)]
            self.nodes_ += [{"seqid": seq, "node_id": 1, "parent": -1, "role": "top", "point": "join"},
                            {"seqid": seq, "node_id": 2, "parent": 1, "role": "outer", "point": "base"},
                            {"seqid": seq, "node_id": 3, "parent": 1, "role": "inner", "point": "base"}]

    def examples(self, points=None, labels=None, usable_only=True, **kw):
        return iter(self.exs)

    def nodes(self, columns=None, filter=None):
        return Rows([{k: n.get(k) for k in columns} for n in self.nodes_])


def test_completes_unseen_cells():
    cells = [(i, j) for i in range(N) for j in range(N)]
    random.Random(1).shuffle(cells)
    train, test = cells[:24], cells[24:]
    assert {i for i, _ in train} == set(range(N)) and {j for _, j in train} == set(range(N))
    est = pgpi.load("matcomp:rank=1")
    est.fit(FakeDataset(train))
    qs = []
    for i, j in test:
        req = Request(dict(join_request(i, j), split="a", outer_rows=ROWS_A, inner_rows=ROWS_B))
        v = est.estimate(req)["rows"]
        t = true_join(i, j)
        q, q_indep = max(v / t, t / v), max(K * t / (ROWS_A * ROWS_B), ROWS_A * ROWS_B / (K * t))
        assert q < 2.0 and q < q_indep, (i, j, v, t)       # independence is off by 1.8x or 5x
        qs.append(q)
        # the split the other way round gives the same answer
        req2 = Request(dict(join_request(i, j), split="b", outer_rows=ROWS_B, inner_rows=ROWS_A))
        assert abs(est.estimate(req2)["rows"] - v) < 1e-6 * v
    assert sorted(qs)[len(qs) // 2] < 1.25


def test_new_literal_falls_back_to_independence():
    est = pgpi.load("matcomp:rank=1")
    est.fit(FakeDataset([(i, j) for i in range(N) for j in range(N)]))
    # a region never seen: no interaction, only the average coupling of the template
    v = est.estimate(Request(dict(join_request(99, 0), split="a", outer_rows=ROWS_A, inner_rows=ROWS_B)))["rows"]
    assert 0.5 < v / (ROWS_A * ROWS_B / K) < 2.0
    # no inputs: no answer. A join edge never seen: no answer.
    assert est.estimate(Request(join_request(0, 0))) is None
    other = join_request(0, 0)
    other["clauses"][2] = {"sql": "(a.k2 = b.k2)", "rels": "a b"}
    assert est.estimate(Request(dict(other, split="a", outer_rows=1, inner_rows=1))) is None
