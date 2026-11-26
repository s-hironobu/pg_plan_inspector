"""The estimator server: the pgpi extension's callback.

It speaks JSON Lines over a Unix socket: one request per line, one answer per line.
Estimators are tried in order. The first non-empty answer wins.

With guard=K, an answer more than K times above or below PostgreSQL's estimate
is treated as an abstention. guard_under=U sets a separate bound for answers
below the estimate (U times below). Underestimates lead to nested loops over
large inputs, so this bound can be tighter than the one for overestimates.

Blocking:     EstimatorServer([est], "/tmp/pgpi.sock").serve_forever()
In a thread:  with EstimatorServer([est], path) as srv: ...  (used by the benchmarks and tests)
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time
from typing import Sequence

from .estimator import Estimator
from .request import Request, answer_rows, normalize_answer


class EstimatorServer:
    def __init__(self, estimators: Sequence[Estimator], socket_path: str, *,
                 log_path: str | None = None, verbose: bool = False,
                 guard: float | None = None, guard_under: float | None = None):
        self.estimators = list(estimators)
        self.socket_path = socket_path
        self.verbose = verbose
        for k in (guard, guard_under):
            if k is not None and not k > 1:
                raise ValueError("guard and guard_under must be greater than 1")
        self.guard = guard                  # answers more than guard times above or below: abstain
        self.guard_under = guard_under      # below: this bound instead (guard_under alone: only below)
        self.log = open(log_path, "a") if log_path else None
        self.stats = {"requests": 0, "answered": 0, "abstained": 0, "errors": 0, "guarded": 0}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._server = None
        self._ready = threading.Event()

    # ------------------------------------------------------------ deciding

    def decide(self, data: dict) -> dict:
        # Undocumented on purpose. With several --plugin, they are asked in the
        # order given, and the first that does not abstain answers.
        # It is a fallback chain, not a race.
        req = Request(data)
        self._guarded = None
        for est in self.estimators:
            try:
                ans = normalize_answer(est.estimate(req))
            except Exception as e:  # a failing estimator abstains
                self.stats["errors"] += 1
                print(f"[{est.name}] error: {e!r}", file=sys.stderr)
                ans = None
            if ans and (self.guard or self.guard_under) and not self._within_guard(req, ans):
                self.stats["guarded"] += 1   # too far from PostgreSQL's estimate: abstain
                self._guarded = (est.name, ans)
                ans = None
            if ans:
                ans.setdefault("source", est.name)
                return ans
        return {}

    def _abstain_note(self) -> str:
        if getattr(self, "_guarded", None):
            name, ans = self._guarded
            return f"abstain (guard: {name} said {ans})"
        return "abstain"

    def _within_guard(self, req: Request, ans: dict) -> bool:
        """Return True if the answer is within the guard of PostgreSQL's estimate."""
        if not req.pg or req.pg <= 0:
            return True
        rows = answer_rows(req, ans)
        if rows is None:
            return True
        pg = req.pg * (req.tuples or 0) if req.point == "clauselist" else req.pg
        if pg <= 0:
            return True
        r = rows / pg
        under = self.guard_under or self.guard
        if under and r < 1 / under:
            return False
        return not (self.guard and r > self.guard)

    async def _handle(self, reader, writer):
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                t0 = time.time()
                try:
                    data = json.loads(line)
                    ans = self.decide(data)
                except Exception as e:
                    print(f"bad request: {e}", file=sys.stderr)
                    data, ans = None, {}
                writer.write((json.dumps(ans) + "\n").encode())
                await writer.drain()
                self.stats["requests"] += 1
                self.stats["answered" if ans else "abstained"] += 1
                if data is not None:
                    if self.verbose:
                        rels = " ".join(r.get("alias", "?") for r in data.get("rels", []))
                        outer = " ".join(r.get("alias", "?") for r in data.get("outer", []))
                        print(f'{data.get("route")} {data.get("point", "?"):<10} ({rels}'
                              f'{" | " + outer if outer else ""}) pg={data.get("pg", 0):.6g}'
                              f' -> {ans or self._abstain_note()} [{(time.time() - t0) * 1000:.1f} ms]',
                              file=sys.stderr)
                    if self.log:
                        self.log.write(json.dumps({"request": data, "answer": ans}) + "\n")
                        self.log.flush()
        except (ConnectionResetError, BrokenPipeError):
            # The backend went away in the middle of a question. Its query was canceled
            # (statement_timeout, often while planning), or it gave up waiting
            # (pgpi.callback_timeout).
            self.stats["disconnected"] = self.stats.get("disconnected", 0) + 1
        finally:
            writer.close()

    async def _start(self):
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
        self._server = await asyncio.start_unix_server(self._handle, path=self.socket_path)
        os.chmod(self.socket_path, 0o777)   # the PostgreSQL server user must connect

    # ------------------------------------------------------------ running

    def serve_forever(self) -> None:
        async def main():
            await self._start()
            print(f"pgpi server listening on {self.socket_path} with "
                  f"{[e.name for e in self.estimators]}", file=sys.stderr)
            async with self._server:
                await self._server.serve_forever()
        try:
            asyncio.run(main())
        finally:
            self.close()

    def start(self) -> "EstimatorServer":
        """Serve in a background thread."""
        def run():
            loop = asyncio.new_event_loop()
            self._loop = loop
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._start())
            self._ready.set()
            loop.run_forever()
            pending = asyncio.all_tasks(loop)
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

        self._thread = threading.Thread(target=run, name="pgpi-server", daemon=True)
        self._thread.start()
        if not self._ready.wait(10):
            raise RuntimeError("estimator server did not start")
        return self

    async def _shutdown(self):
        self._server.close()
        if hasattr(self._server, "close_clients"):
            self._server.close_clients()
        for t in [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]:
            t.cancel()
        asyncio.get_running_loop().stop()

    def stop(self) -> None:
        loop = self._loop
        if loop is not None and self._thread is not None:
            asyncio.run_coroutine_threadsafe(self._shutdown(), loop)
            self._thread.join(10)
            self._loop = None
        if os.path.exists(self.socket_path):
            try:
                os.unlink(self.socket_path)
            except OSError:
                pass

    def close(self) -> None:
        if self.log:
            self.log.close()
            self.log = None

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()
        self.close()

    def reset_stats(self) -> dict:
        s = dict(self.stats)
        for k in self.stats:
            self.stats[k] = 0
        return s
