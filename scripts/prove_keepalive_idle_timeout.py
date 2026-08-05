#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One-shot demonstration that HTTP/1.1 keep-alive disables idle tymeout in hio.

Run from repo root (with hio importable):

  python scripts/prove_keepalive_idle_timeout.py

Or with the tests:

  pytest tests/core/http/test_idle_timeout_keepalive.py -v
"""
from __future__ import annotations

import sys
import time

from hio.base import tyming
from hio.core import http
from hio.core.http.serving import Requestant


def section(title: str) -> None:
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def prove_unit() -> None:
    section("1) Unit: Requestant.checkPersisted on HTTP/1.1")

    class FakeRemoter:
        def __init__(self):
            self.tymeout = 5.0

    raw = bytearray(
        b"GET /oobi HTTP/1.1\r\n"
        b"Host: example.test\r\n"
        b"\r\n"
    )
    remoter = FakeRemoter()
    req = Requestant(msg=raw, remoter=remoter)
    while not req.ended:
        req.parse()

    print(f"  version          = {req.version}")
    print(f"  persisted        = {req.persisted}")
    print(f"  remoter.tymeout  = {remoter.tymeout}  (was 5.0 before parse)")
    if req.persisted and remoter.tymeout == 0.0:
        print("  RESULT: BUG CONFIRMED — keep-alive zeroed idle tymeout")
    else:
        print("  RESULT: unexpected (bug may already be fixed)")
        sys.exit(2)


def prove_server() -> None:
    section("2) Integration: WSGI server keeps idle keep-alive after tymeout")

    def app(environ, start_response):
        body = b"ok"
        start_response(
            "200 OK",
            [("Content-type", "text/plain"), ("Content-length", str(len(body)))],
        )
        return [body]

    tymist = tyming.Tymist(tyme=0.0)
    idle = 0.5
    # fixed high port for demo; pytest suite uses ephemeral ports
    port = 16111

    with http.openServer(
        port=port, app=app, tymeout=idle, tymth=tymist.tymen()
    ) as alpha:
        path = f"http://127.0.0.1:{port}/"
        with http.openClient(
            path=path, tymth=tymist.tymen(), reconnectable=False
        ) as beta:
            beta.requests.append(
                dict(
                    method="GET",
                    path="/",
                    qargs={},
                    fragment="",
                    headers={"Accept": "text/plain", "Content-Length": 0},
                )
            )
            for _ in range(200):
                alpha.service()
                beta.service()
                # Do not require alpha.idle(): keep-alive remoters stay open, so
                # idle() may remain False after a successful response.
                if beta.responses and alpha.servant.ixes:
                    break
                time.sleep(0.01)
            else:
                print("  ERROR: request did not complete")
                sys.exit(1)

            remoter = list(alpha.servant.ixes.values())[0]
            print(f"  after request: ixes={len(alpha.servant.ixes)} "
                  f"tymeout={remoter.tymeout}")

            for _ in range(20):
                tymist.tick(tock=0.1)
                alpha.service()

            print(f"  after tyme={tymist.tyme:.1f}s "
                  f"(idle tymeout was {idle}s): "
                  f"ixes={len(alpha.servant.ixes)}")
            if len(alpha.servant.ixes) == 1:
                print("  RESULT: BUG CONFIRMED — idle keep-alive not pruned")
            else:
                print("  RESULT: unexpected close (bug may already be fixed)")
                sys.exit(2)


def main() -> None:
    print("hio keep-alive idle timeout proof")
    print(f"python: {sys.version.split()[0]}")
    try:
        import hio
        print(f"hio:    {getattr(hio, '__version__', '?')}")
    except Exception:
        pass

    prove_unit()
    prove_server()

    section("Summary")
    print(
        "Root cause: Requestant.checkPersisted() sets remoter.tymeout=0.0 when\n"
        "persisted (HTTP/1.1 keep-alive). Server.serviceConnects() only closes\n"
        "when ix.tymeout > 0.0 and ix.tymer.expired — so idle sockets never die.\n"
        "\n"
        "See tests/core/http/test_idle_timeout_keepalive.py"
    )


if __name__ == "__main__":
    main()
