#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Demonstrate keep-alive idle timeout behavior in hio.

Run from repo root (with hio importable, preferably this tree on PYTHONPATH):

  PYTHONPATH=src python scripts/prove_keepalive_idle_timeout.py

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
    if req.persisted and remoter.tymeout == 5.0:
        print("  RESULT: OK — keep-alive preserved idle tymeout")
    elif req.persisted and remoter.tymeout == 0.0:
        print("  RESULT: BUG STILL PRESENT — keep-alive zeroed idle tymeout")
        sys.exit(2)
    else:
        print("  RESULT: unexpected")
        sys.exit(2)


def prove_server() -> None:
    section("2) Integration: idle keep-alive is pruned after tymeout")

    def app(environ, start_response):
        body = b"ok"
        start_response(
            "200 OK",
            [("Content-type", "text/plain"), ("Content-length", str(len(body)))],
        )
        return [body]

    tymist = tyming.Tymist(tyme=0.0)
    idle = 0.5
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
                if beta.responses and alpha.servant.ixes:
                    break
                time.sleep(0.01)
            else:
                print("  ERROR: request did not complete")
                sys.exit(1)

            remoter = list(alpha.servant.ixes.values())[0]
            print(f"  after request: ixes={len(alpha.servant.ixes)} "
                  f"tymeout={remoter.tymeout} "
                  f"persisted={list(alpha.reqs.values())[0].persisted}")

            for _ in range(20):
                tymist.tick(tock=0.1)
                alpha.service()

            print(f"  after tyme={tymist.tyme:.1f}s "
                  f"(idle tymeout was {idle}s): "
                  f"ixes={len(alpha.servant.ixes)}")
            if len(alpha.servant.ixes) == 0:
                print("  RESULT: OK — idle keep-alive pruned after tymeout")
            else:
                print("  RESULT: BUG STILL PRESENT — idle keep-alive not pruned")
                sys.exit(2)


def prove_reuse() -> None:
    section("3) Legitimate keep-alive: second request reuses connection")

    def app(environ, start_response):
        body = b"ok"
        start_response(
            "200 OK",
            [("Content-type", "text/plain"), ("Content-length", str(len(body)))],
        )
        return [body]

    tymist = tyming.Tymist(tyme=0.0)
    idle = 2.0
    port = 16112

    with http.openServer(
        port=port, app=app, tymeout=idle, tymth=tymist.tymen()
    ) as alpha:
        path = f"http://127.0.0.1:{port}/"
        with http.openClient(
            path=path, tymth=tymist.tymen(), reconnectable=False
        ) as beta:
            req = dict(
                method="GET",
                path="/",
                qargs={},
                fragment="",
                headers={"Accept": "text/plain", "Content-Length": 0},
            )
            beta.requests.append(dict(req))
            for _ in range(200):
                alpha.service()
                beta.service()
                if beta.responses and alpha.servant.ixes:
                    break
                time.sleep(0.01)
            else:
                print("  ERROR: first request did not complete")
                sys.exit(1)

            ca = next(iter(alpha.servant.ixes))
            beta.responses.clear()
            beta.requests.append(dict(req))
            for _ in range(200):
                alpha.service()
                beta.service()
                if beta.responses:
                    break
                time.sleep(0.01)
            else:
                print("  ERROR: second request did not complete")
                sys.exit(1)

            print(f"  responses=2 path, same ca present={ca in alpha.servant.ixes}, "
                  f"ixes={len(alpha.servant.ixes)}")
            if ca in alpha.servant.ixes and len(alpha.servant.ixes) == 1:
                print("  RESULT: OK — connection reused for second request")
            else:
                print("  RESULT: unexpected connection handling")
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
    prove_reuse()

    section("Summary")
    print(
        "Keep-alive still reuses connections while active.\n"
        "Idle connections expire after server tymeout.\n"
        "See tests/core/http/test_idle_timeout_keepalive.py"
    )


if __name__ == "__main__":
    main()
