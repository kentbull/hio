# -*- coding: utf-8 -*-
"""
Prove that HTTP/1.1 keep-alive disables idle connection timeouts.

Production symptom (KERI witnesses, public :5623):
  Idle ESTABLISHED TCP connections accumulate forever because after the first
  HTTP/1.1 request, Requestant.checkPersisted() sets remoter.tymeout = 0.0
  ("never timesout"), and Server.serviceConnects() only drops connections when
  ``ix.tymeout > 0.0 and ix.tymer.expired``.

These tests are deterministic and do not need keripy or a witness.

Two layers:
  1. Unit tests on Requestant.checkPersisted (no sockets).
  2. WSGI Server integration: keep-alive clients stay in servant.ixes after the
     configured tymeout elapses in virtual tyme.

Documenting tests assert *current* defective behavior and pass on unfixed hio.
Target-behavior tests are xfail(strict=True) until keep-alive idle timeout is fixed.
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
    # Drive the generator-based parser to completion
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

def test_http11_default_keepalive_zeros_remoter_tymeout():
    """
    BUG (documenting): HTTP/1.1 defaults to keep-alive and disables idle tymeout.

    Desired after fix: remoter.tymeout stays at the server idle timeout (e.g. 5.0)
    so serviceConnects can still drop idle keep-alive sockets.
    """
    raw = (
        b"GET /oobi HTTP/1.1\r\n"
        b"Host: example.test\r\n"
        b"\r\n"
    )
    requestant, remoter = parse_request(raw, tymeout=5.0)

    assert requestant.version == (1, 1)
    assert requestant.persisted is True
    # Current defective behavior:
    assert remoter.tymeout == 0.0


def test_http11_connection_close_keeps_remoter_tymeout():
    """Control: Connection: close is non-persisted and must not zero tymeout."""
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


def test_http10_keepalive_zeros_remoter_tymeout():
    """
    BUG (documenting): explicit HTTP/1.0 keep-alive also zeros tymeout.

    Same root cause as HTTP/1.1 default (persisted branch).
    """
    raw = (
        b"GET /oobi HTTP/1.0\r\n"
        b"Host: example.test\r\n"
        b"Connection: keep-alive\r\n"
        b"\r\n"
    )
    requestant, remoter = parse_request(raw, tymeout=5.0)

    assert requestant.version == (1, 0)
    assert requestant.persisted is True
    assert remoter.tymeout == 0.0


# ---------------------------------------------------------------------------
# Target behavior after fix (xfail until keep-alive idle timeout works)
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    strict=True,
    reason=(
        "HTTP/1.1 keep-alive currently sets remoter.tymeout=0.0 in "
        "Requestant.checkPersisted; idle expiry should remain enabled."
    ),
)
def test_http11_keepalive_should_preserve_idle_tymeout():
    """Target: keep-alive must not disable idle connection expiry."""
    raw = (
        b"GET /oobi HTTP/1.1\r\n"
        b"Host: example.test\r\n"
        b"\r\n"
    )
    requestant, remoter = parse_request(raw, tymeout=5.0)

    assert requestant.persisted is True
    assert remoter.tymeout == 5.0  # desired: still idle-expirable


# ---------------------------------------------------------------------------
# Integration: WSGI Server + keep-alive + virtual tyme
# ---------------------------------------------------------------------------

def test_wsgi_keepalive_connection_survives_past_idle_tymeout():
    """
    BUG (documenting): after one HTTP/1.1 request, idle sockets are never dropped.

    Server is configured with tymeout=0.5s. Client does one GET and stays connected.
    Virtual tyme advances well past 0.5s while the server is serviced.

    Current behavior: connection remains in servant.ixes with tymeout == 0.0.
    Desired after fix: connection is closed/removed after idle tymeout.
    """
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

            # Keep-alive remoters stay open, so do not require alpha.idle().
            ok = service_until(
                alpha,
                beta,
                lambda: bool(beta.responses) and len(alpha.servant.ixes) == 1,
            )
            assert ok, "request/response did not complete"
            assert len(beta.responses) == 1
            assert beta.responses[0]["status"] == 200

            # Immediately after keep-alive request, remoter idle timeout is disabled.
            remoter = list(alpha.servant.ixes.values())[0]
            assert remoter.tymeout == 0.0

            # Advance virtual tyme far past configured idle tymeout.
            advance_virtual_tyme(tymist, alpha, seconds=idle_tymeout * 4)

            # BUG: connection still held
            assert len(alpha.servant.ixes) == 1
            remoter = list(alpha.servant.ixes.values())[0]
            assert remoter.tymeout == 0.0


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Keep-alive currently disables remoter.tymeout; server should idle-close "
        "after tymeout even for HTTP/1.1 persisted connections."
    ),
)
def test_wsgi_keepalive_connection_should_idle_close_after_tymeout():
    """Target: idle keep-alive connections are pruned after server tymeout."""
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

            advance_virtual_tyme(tymist, alpha, seconds=idle_tymeout * 4)

            # Desired: idle keep-alive socket removed
            assert len(alpha.servant.ixes) == 0


def test_many_idle_keepalive_clients_accumulate():
    """
    BUG (documenting): many idle keep-alive clients all remain accepted forever.

    Mirrors the production shape (hundreds/thousands of ESTABLISHED sockets from
    clients that completed a request and never closed).
    """
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

            # Drive until all clients have a response (keep-alive may leave
            # server non-idle while remoters stay open).
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
            assert all(ix.tymeout == 0.0 for ix in alpha.servant.ixes.values())

            advance_virtual_tyme(tymist, alpha, seconds=idle_tymeout * 4)

            # BUG: all N idle keep-alive connections still present
            assert len(alpha.servant.ixes) == n_clients
        finally:
            for beta in clients:
                try:
                    beta.close()
                except Exception:
                    pass


if __name__ == "__main__":
    # Allow: python tests/core/http/test_idle_timeout_keepalive.py
    test_http11_default_keepalive_zeros_remoter_tymeout()
    test_http11_connection_close_keeps_remoter_tymeout()
    test_http10_default_keeps_remoter_tymeout()
    test_http10_keepalive_zeros_remoter_tymeout()
    test_wsgi_keepalive_connection_survives_past_idle_tymeout()
    test_many_idle_keepalive_clients_accumulate()
    print("documenting tests OK (bug present as expected)")
