# -*- coding: utf-8 -*-
"""
HTTP/1.1 keep-alive must remain reusable without disabling idle timeouts.

Keep-alive (persisted) means the connection may handle another request.
It must not set remoter.tymeout = 0.0; idle sockets expire via
Server.serviceConnects() when ix.tymeout > 0 and ix.tymer.expired.
Active traffic still refreshes the remoter timer (refreshable).
"""
import socket
import time

import pytest

from hio.base import tyming
from hio.core import http
from hio.core.http.serving import Requestant


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeRemoter:
    """Minimal remoter stand-in for Requestant unit tests."""

    def __init__(self, tymeout=5.0):
        self.tymeout = tymeout


def parse_request(raw: bytes, tymeout: float = 5.0) -> tuple[Requestant, FakeRemoter]:
    remoter = FakeRemoter(tymeout=tymeout)
    requestant = Requestant(msg=bytearray(raw), remoter=remoter)
    while not requestant.ended:
        requestant.parse()
    return requestant, remoter


def wsgi_ok(environ, start_response):
    body = b"ok"
    start_response(
        "200 OK",
        [("Content-type", "text/plain"), ("Content-length", str(len(body)))],
    )
    return [body]


def service_until(alpha, beta, predicate, loops=200, sleep=0.01):
    """Service server and client until predicate() or loops exhausted."""
    for _ in range(loops):
        alpha.service()
        beta.service()
        if predicate():
            return True
        time.sleep(sleep)
    return False


def advance_virtual_tyme(tymist, alpha, seconds, step=0.1):
    """Advance virtual tyme while servicing the server (idle expiry path)."""
    remaining = float(seconds)
    while remaining > 0:
        tock = min(step, remaining)
        tymist.tick(tock=tock)
        alpha.service()
        remaining -= tock


def free_port() -> int:
    """Bind an ephemeral port and return the number (socket closed afterward)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ---------------------------------------------------------------------------
# Unit: Requestant.checkPersisted
# ---------------------------------------------------------------------------

def test_http11_default_keepalive_preserves_idle_tymeout():
    """HTTP/1.1 keep-alive stays persisted but does not zero remoter.tymeout."""
    raw = (
        b"GET /oobi HTTP/1.1\r\n"
        b"Host: example.test\r\n"
        b"\r\n"
    )
    requestant, remoter = parse_request(raw, tymeout=5.0)

    assert requestant.version == (1, 1)
    assert requestant.persisted is True
    assert remoter.tymeout == 5.0


def test_http11_connection_close_keeps_remoter_tymeout():
    """Control: Connection: close is non-persisted and keeps tymeout."""
    raw = (
        b"GET /oobi HTTP/1.1\r\n"
        b"Host: example.test\r\n"
        b"Connection: close\r\n"
        b"\r\n"
    )
    requestant, remoter = parse_request(raw, tymeout=5.0)

    assert requestant.version == (1, 1)
    assert requestant.persisted is False
    assert remoter.tymeout == 5.0


def test_http10_default_keeps_remoter_tymeout():
    """Control: HTTP/1.0 defaults to non-persisted; tymeout preserved."""
    raw = (
        b"GET /oobi HTTP/1.0\r\n"
        b"Host: example.test\r\n"
        b"\r\n"
    )
    requestant, remoter = parse_request(raw, tymeout=5.0)

    assert requestant.version == (1, 0)
    assert requestant.persisted is False
    assert remoter.tymeout == 5.0


def test_http10_keepalive_preserves_idle_tymeout():
    """HTTP/1.0 keep-alive is persisted but still idle-expirable."""
    raw = (
        b"GET /oobi HTTP/1.0\r\n"
        b"Host: example.test\r\n"
        b"Connection: keep-alive\r\n"
        b"\r\n"
    )
    requestant, remoter = parse_request(raw, tymeout=5.0)

    assert requestant.version == (1, 0)
    assert requestant.persisted is True
    assert remoter.tymeout == 5.0


# ---------------------------------------------------------------------------
# Integration: WSGI Server + keep-alive + virtual tyme
# ---------------------------------------------------------------------------

def test_wsgi_keepalive_connection_idle_closes_after_tymeout():
    """After one HTTP/1.1 request, idle sockets drop once tymeout elapses."""
    tymist = tyming.Tymist(tyme=0.0)
    port = free_port()
    idle_tymeout = 0.5

    with http.openServer(
        port=port,
        app=wsgi_ok,
        tymeout=idle_tymeout,
        tymth=tymist.tymen(),
    ) as alpha:
        path = f"http://127.0.0.1:{port}/"
        with http.openClient(
            path=path,
            tymth=tymist.tymen(),
            reconnectable=False,
        ) as beta:
            beta.requests.append(
                dict(
                    method="GET",
                    path="/",
                    qargs=dict(),
                    fragment="",
                    headers=dict([("Accept", "text/plain"), ("Content-Length", 0)]),
                )
            )

            ok = service_until(
                alpha,
                beta,
                lambda: bool(beta.responses) and len(alpha.servant.ixes) == 1,
            )
            assert ok, "request/response did not complete"
            assert len(beta.responses) == 1
            assert beta.responses[0]["status"] == 200

            remoter = list(alpha.servant.ixes.values())[0]
            assert remoter.tymeout == idle_tymeout
            # Keep-alive reuse flag stays set; only idle tymeout is enforced
            requestant = list(alpha.reqs.values())[0]
            assert requestant.persisted is True

            advance_virtual_tyme(tymist, alpha, seconds=idle_tymeout * 4)

            assert len(alpha.servant.ixes) == 0


def test_wsgi_keepalive_reuses_connection_for_second_request():
    """Legitimate keep-alive: second request on same client before idle expiry."""
    tymist = tyming.Tymist(tyme=0.0)
    port = free_port()
    idle_tymeout = 2.0

    with http.openServer(
        port=port,
        app=wsgi_ok,
        tymeout=idle_tymeout,
        tymth=tymist.tymen(),
    ) as alpha:
        path = f"http://127.0.0.1:{port}/"
        with http.openClient(
            path=path,
            tymth=tymist.tymen(),
            reconnectable=False,
        ) as beta:
            req = dict(
                method="GET",
                path="/",
                qargs=dict(),
                fragment="",
                headers=dict([("Accept", "text/plain"), ("Content-Length", 0)]),
            )
            beta.requests.append(dict(req))

            ok = service_until(
                alpha,
                beta,
                lambda: len(beta.responses) >= 1 and len(alpha.servant.ixes) == 1,
            )
            assert ok, "first request did not complete"
            assert beta.responses.popleft()["status"] == 200

            ca = next(iter(alpha.servant.ixes))
            # Second request while still within idle window
            beta.requests.append(dict(req))
            ok = service_until(
                alpha,
                beta,
                lambda: len(beta.responses) >= 1,
            )
            assert ok, "second request did not complete"
            assert beta.responses.popleft()["status"] == 200

            # Same remoter still present (connection reused, not re-accepted only)
            assert len(alpha.servant.ixes) == 1
            assert ca in alpha.servant.ixes
            assert list(alpha.reqs.values())[0].persisted is True


def test_many_idle_keepalive_clients_are_pruned():
    """Many idle keep-alive clients are all closed after tymeout (no pile-up)."""
    tymist = tyming.Tymist(tyme=0.0)
    port = free_port()
    idle_tymeout = 0.5
    n_clients = 20

    with http.openServer(
        port=port,
        app=wsgi_ok,
        tymeout=idle_tymeout,
        tymth=tymist.tymen(),
    ) as alpha:
        clients = []
        try:
            for _ in range(n_clients):
                path = f"http://127.0.0.1:{port}/"
                beta = http.Client(
                    path=path,
                    tymth=tymist.tymen(),
                    reconnectable=False,
                )
                beta.reopen()
                clients.append(beta)
                beta.requests.append(
                    dict(
                        method="GET",
                        path="/",
                        qargs=dict(),
                        fragment="",
                        headers=dict(
                            [("Accept", "text/plain"), ("Content-Length", 0)]
                        ),
                    )
                )

            for _ in range(500):
                alpha.service()
                for beta in clients:
                    beta.service()
                if (
                    all(len(c.responses) >= 1 for c in clients)
                    and len(alpha.servant.ixes) >= n_clients
                ):
                    break
                time.sleep(0.01)
            else:
                pytest.fail(
                    f"not all clients completed: "
                    f"responses={[len(c.responses) for c in clients]} "
                    f"ixes={len(alpha.servant.ixes)}"
                )

            assert len(alpha.servant.ixes) == n_clients
            assert all(ix.tymeout == idle_tymeout for ix in alpha.servant.ixes.values())

            advance_virtual_tyme(tymist, alpha, seconds=idle_tymeout * 4)

            assert len(alpha.servant.ixes) == 0
        finally:
            for beta in clients:
                try:
                    beta.close()
                except Exception:
                    pass


if __name__ == "__main__":
    test_http11_default_keepalive_preserves_idle_tymeout()
    test_http11_connection_close_keeps_remoter_tymeout()
    test_http10_default_keeps_remoter_tymeout()
    test_http10_keepalive_preserves_idle_tymeout()
    test_wsgi_keepalive_connection_idle_closes_after_tymeout()
    test_wsgi_keepalive_reuses_connection_for_second_request()
    test_many_idle_keepalive_clients_are_pruned()
    print("all keep-alive idle timeout tests OK")
