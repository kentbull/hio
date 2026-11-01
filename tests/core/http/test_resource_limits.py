# -*- encoding: utf-8 -*-
"""Focused tests for opt-in HTTP ingress and response limits."""

import socket
import time
from types import SimpleNamespace

import pytest

from hio.base import tyming
from hio.core import http, tcp
from hio.core.http import clienting, serving


def unused_port():
    """Return an ephemeral loopback TCP port for a server fixture."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]
    finally:
        probe.close()


def parse_response(message, limit, *, method="GET", closed=False):
    """Parse one response with a decoded-body limit."""
    respondent = clienting.Respondent(msg=bytearray(message),
                                      method=method,
                                      bodyLimit=limit)
    respondent.parse()
    if closed and not respondent.ended:
        respondent.close()
        respondent.parse()
    return respondent


def test_requestant_rejects_headers_body_and_expired_deadline():
    tymist = tyming.Tymist(tyme=0.0)
    remoter = SimpleNamespace(tymth=tymist.tymen())

    message = bytearray(b"GET / HTTP/1.1\r\nX-Test: 1234\r\n\r\n")
    requestant = serving.Requestant(msg=message,
                                    remoter=remoter,
                                    headerLimit=len(message) - 1)
    requestant.checkLimits()
    assert requestant.rejection == (431, "Request Header Fields Too Large")
    assert requestant.body == b""

    message = bytearray(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
    requestant = serving.Requestant(msg=message,
                                    remoter=remoter,
                                    headerLimit=len(message))
    requestant.checkLimits()
    requestant.parse()
    assert requestant.ended
    assert requestant.rejection is None

    message = bytearray(
        b"POST / HTTP/1.1\r\nContent-Length: 5\r\n\r\nabcde")
    requestant = serving.Requestant(msg=message,
                                    remoter=remoter,
                                    bodyLimit=4)
    requestant.parse()
    assert requestant.ended
    assert requestant.rejection == (413, "Request Entity Too Large")
    assert requestant.body == b""

    # A declared chunk is rejected before its data has arrived.
    message = bytearray(
        b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n")
    requestant = serving.Requestant(msg=message,
                                    remoter=remoter,
                                    bodyLimit=4)
    requestant.parse()
    assert requestant.ended
    assert requestant.rejection == (413, "Request Entity Too Large")
    assert requestant.body == b""

    message = bytearray(b"G")
    requestant = serving.Requestant(msg=message,
                                    remoter=remoter,
                                    requestTimeout=1.0)
    requestant.checkLimits()
    assert requestant.requestTymer is not None
    tymist.tick(tock=1.1)
    message.extend(b"E")
    requestant.checkLimits()
    assert requestant.rejection == (408, "Request Timeout")


def test_wsgi_body_rejection_returns_413_without_calling_app():
    calls = []

    def app(environ, start_response):
        calls.append(environ)
        start_response("200 OK", [("Content-Length", "2")])
        return [b"ok"]

    server = http.Server(ha=("127.0.0.1", unused_port()),
                         app=app,
                         requestBodyLimit=4)
    client = None
    received = bytearray()
    try:
        assert server.reopen()
        client = socket.create_connection(server.servant.eha)
        client.setblocking(False)
        client.sendall(
            b"POST / HTTP/1.1\r\nHost: localhost\r\n"
            b"Content-Length: 5\r\n\r\nabcde")

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            server.service()
            try:
                part = client.recv(4096)
            except BlockingIOError:
                part = None
            if part:
                received.extend(part)
            elif part == b"":
                break
            time.sleep(0.001)

        assert received.startswith(b"HTTP/1.1 413 Request Entity Too Large\r\n")
        assert b"Connection: close\r\n" in received
        assert not calls
        assert not server.servant.ixes
        assert not server.reqs
    finally:
        if client is not None:
            client.close()
        server.close()


def test_wsgi_request_deadline_does_not_interrupt_streamed_response():
    tymist = tyming.Tymist(tyme=0.0)

    def app(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/plain")])
        yield b"first"
        yield b"second"

    server = http.Server(ha=("127.0.0.1", unused_port()),
                         app=app,
                         tymth=tymist.tymen(),
                         requestTimeout=0.1)
    client = None
    received = bytearray()
    try:
        assert server.reopen()
        client = socket.create_connection(server.servant.eha)
        client.setblocking(False)
        client.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            server.service()
            if server.reps and next(iter(server.reps.values())).headed:
                break
            time.sleep(0.001)

        requestant = next(iter(server.reqs.values()))
        responder = next(iter(server.reps.values()))
        assert requestant.parser is None
        assert requestant.requestTymer is not None
        assert responder.headed
        assert not responder.ended

        tymist.tick(tock=0.2)
        assert requestant.requestTymer.expired

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            server.service()
            try:
                part = client.recv(4096)
            except BlockingIOError:
                part = None
            if part:
                received.extend(part)
            if responder.ended and not requestant.remoter.txbs:
                break
            time.sleep(0.001)

        assert received.startswith(b"HTTP/1.1 200 OK\r\n")
        assert received.count(b"HTTP/1.1") == 1
        assert b"408 Request Timeout" not in received
        assert b"5\r\nfirst\r\n" in received
        assert b"6\r\nsecond\r\n" in received
        assert responder.ended
        assert requestant.rejection is None
    finally:
        if client is not None:
            client.close()
        server.close()


@pytest.mark.parametrize(
    "partial_request",
    (b"G",
     b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\na"))
def test_wsgi_request_deadline_still_rejects_incomplete_request(partial_request):
    tymist = tyming.Tymist(tyme=0.0)
    calls = []

    def app(environ, start_response):
        calls.append(environ)
        start_response("200 OK", [("Content-Length", "2")])
        return [b"ok"]

    server = http.Server(ha=("127.0.0.1", unused_port()),
                         app=app,
                         tymth=tymist.tymen(),
                         requestTimeout=0.1)
    client = None
    received = bytearray()
    try:
        assert server.reopen()
        client = socket.create_connection(server.servant.eha)
        client.setblocking(False)
        client.sendall(partial_request)

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            server.service()
            if (server.reqs and
                    next(iter(server.reqs.values())).requestTymer is not None):
                break
            time.sleep(0.001)

        requestant = next(iter(server.reqs.values()))
        assert requestant.parser is not None
        assert not requestant.ended

        tymist.tick(tock=0.2)
        assert requestant.requestTymer.expired

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            server.service()
            try:
                part = client.recv(4096)
            except BlockingIOError:
                part = None
            if part:
                received.extend(part)
            elif part == b"":
                break
            time.sleep(0.001)

        assert received.startswith(b"HTTP/1.1 408 Request Timeout\r\n")
        assert not calls
        assert not server.servant.ixes
        assert not server.reqs
    finally:
        if client is not None:
            client.close()
        server.close()


def test_tcp_overflow_discards_http_state_before_app_dispatch():
    calls = []

    def app(environ, start_response):
        calls.append(environ)
        start_response("200 OK", [("Content-Length", "2")])
        return [b"ok"]

    server = http.Server(ha=("127.0.0.1", unused_port()),
                         app=app,
                         rxbsLimit=8,
                         rxbsPerTurn=64)
    client = None
    try:
        assert server.reopen()
        client = socket.create_connection(server.servant.eha)
        client.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            server.service()
            if not server.servant.ixes and not server.reqs:
                break
            time.sleep(0.001)

        assert not calls
        assert not server.servant.ixes
        assert not server.reqs
    finally:
        if client is not None:
            client.close()
        server.close()


@pytest.mark.parametrize(
    "message,closed",
    ((b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nabcd", False),
     (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
      b"4\r\nabcd\r\n0\r\n\r\n", False),
     (b"HTTP/1.0 200 OK\r\n\r\nabcd", True)))
def test_response_body_limit_accepts_exact_declared_chunked_and_eof(message,
                                                                    closed):
    respondent = parse_response(message, 4, closed=closed)
    assert respondent.ended
    assert not respondent.errored
    assert not respondent.limitExceeded
    assert respondent.body == b"abcd"


@pytest.mark.parametrize(
    "message,closed",
    ((b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nabcde", False),
     # Chunk size alone is enough to reject; the chunk data is not buffered.
     (b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n", False),
     (b"HTTP/1.0 200 OK\r\n\r\nabcde", True)))
def test_response_body_limit_rejects_declared_chunked_and_eof(message, closed):
    respondent = parse_response(message, 4, closed=closed)
    assert respondent.ended
    assert respondent.errored
    assert respondent.limitExceeded
    assert respondent.error == "response body limit exceeded"
    assert respondent.body == b""


def test_response_body_limit_ignores_head_content_length_metadata():
    respondent = parse_response(
        b"HTTP/1.1 200 OK\r\nContent-Length: 999\r\n\r\n", 4, method="HEAD")
    assert respondent.ended
    assert not respondent.errored
    assert respondent.body == b""


def test_client_response_limit_closes_and_discards_unparsed_bytes():
    connector = tcp.Client(ha=("127.0.0.1", 6101))
    client = clienting.Client(connector=connector,
                              path="http://127.0.0.1:6101/",
                              responseBodyLimit=4)
    closed = []
    connector.close = lambda: closed.append(True)
    connector.rxbs.extend(
        b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nabcde"
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
    client.waited = True

    client.serviceResponse()

    assert closed == [True]
    assert connector.rxbs == b""
    assert len(client.responses) == 1
    response = client.responses.popleft()
    assert response["errored"]
    assert response["error"] == "response body limit exceeded"
    assert response["body"] == b""


def test_default_http_limits_preserve_legacy_unbounded_configuration():
    requestant = serving.Requestant(
        remoter=SimpleNamespace(tymth=tyming.Tymist().tymen()))
    assert requestant.headerLimit is None
    assert requestant.bodyLimit is None
    assert requestant.requestTimeout is None

    server = http.Server()
    assert server.requestHeaderLimit is None
    assert server.requestBodyLimit is None
    assert server.requestTimeout is None

    respondent = clienting.Respondent()
    assert respondent.bodyLimit is None
    client = clienting.Client()
    assert client.responseBodyLimit is None
    assert client.connector.rxbsLimit is None

    client = clienting.Client(responseBodyLimit=4, rxbsLimit=8)
    assert client.responseBodyLimit == 4
    assert client.connector.rxbsLimit == 8
