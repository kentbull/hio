# -*- encoding: utf-8 -*-
"""Tests for opt-in TCP server ingress resource limits."""

import os
import socket
import ssl
import time

import pytest

from hio.base import tyming
from hio.core import tcp


def unused_port():
    """Return an ephemeral loopback TCP port for a server fixture."""
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]
    finally:
        probe.close()


@pytest.mark.parametrize("server_cls", (tcp.Server, tcp.ServerTls))
def test_accepts_per_turn_applies_to_plain_and_tls_servers(server_cls):
    kwa = dict(certify=ssl.CERT_NONE) if server_cls is tcp.ServerTls else {}
    server = server_cls(ha=("127.0.0.1", unused_port()),
                        acceptsPerTurn=1,
                        **kwa)
    clients = []

    try:
        assert server.reopen()
        for _ in range(2):
            client = socket.create_connection(server.eha)
            clients.append(client)

        server.serviceAccepts()
        assert len(server.axes) == 1
        assert server.connectionCount() == 1

        server.serviceAccepts()
        assert len(server.axes) == 2
        assert server.connectionCount() == 2
    finally:
        while server.axes:
            cs, _ = server.axes.popleft()
            cs.close()
        server.close()
        for client in clients:
            client.close()


@pytest.mark.parametrize("server_cls", (tcp.Server, tcp.ServerTls))
def test_connection_admission_counts_plain_and_tls_handshakes(server_cls):
    kwa = dict(certify=ssl.CERT_NONE) if server_cls is tcp.ServerTls else {}
    server = server_cls(ha=("127.0.0.1", unused_port()),
                        maxConnections=1,
                        tymeout=2.5,
                        **kwa)
    clients = []

    try:
        assert server.reopen()
        for _ in range(2):
            client = socket.create_connection(server.eha)
            clients.append(client)

        server.serviceAxes()
        assert server.connectionCount() == 1
        assert not server.axes
        if server_cls is tcp.ServerTls:
            assert len(server.cxes) == 1
            assert not server.ixes
            remoter = next(iter(server.cxes.values()))
        else:
            assert len(server.ixes) == 1
            remoter = next(iter(server.ixes.values()))
        assert remoter.tymeout == 2.5
        assert remoter.tymer.duration == 2.5

        server.serviceAccepts()
        assert not server.axes
        assert server.connectionCount() == 1

        if server_cls is tcp.ServerTls:
            _, remoter = server.cxes.popitem()
            remoter.close()
        else:
            ca = next(iter(server.ixes))
            server.removeIx(ca)

        server.serviceAccepts()
        assert len(server.axes) == 1
        assert server.connectionCount() == 1
    finally:
        while server.axes:
            cs, _ = server.axes.popleft()
            cs.close()
        if isinstance(server, tcp.ServerTls):
            for remoter in server.cxes.values():
                remoter.close()
            server.cxes.clear()
        server.close()
        for client in clients:
            client.close()


def test_remoter_bounds_receive_work_and_closes_before_overflow_append():
    cs, peer = socket.socketpair()
    remoter = tcp.Remoter(tymth=tyming.Tymist().tymen(),
                          ha=("local", 1),
                          ca=("peer", 2),
                          cs=cs,
                          bs=8,
                          rxbsLimit=5,
                          rxbsPerTurn=3)

    try:
        peer.sendall(b"abcdef")

        remoter.serviceReceives()
        assert remoter.rxbs == b"abc"
        assert not remoter.rxbsOverflowed
        assert not remoter.cutoff

        remoter.serviceReceives()
        assert remoter.rxbs == b"abcde"
        assert remoter.rxbsOverflowed
        assert remoter.cutoff
        assert remoter.cs is None
    finally:
        remoter.close()
        peer.close()


def test_default_receive_limits_preserve_legacy_drain_behavior():
    server = tcp.Server()
    assert server.maxConnections is None
    assert server.acceptsPerTurn is None
    assert server.rxbsLimit is None
    assert server.rxbsPerTurn is None

    cs, peer = socket.socketpair()
    remoter = tcp.Remoter(tymth=tyming.Tymist().tymen(),
                          ha=("local", 1),
                          ca=("peer", 2),
                          cs=cs,
                          bs=4)

    try:
        assert remoter.rxbsLimit is None
        assert remoter.rxbsPerTurn is None
        peer.sendall(b"legacy-drain")
        remoter.serviceReceives()
        assert remoter.rxbs == b"legacy-drain"
        assert not remoter.rxbsOverflowed
    finally:
        remoter.close()
        peer.close()


def test_client_closes_before_receive_buffer_overflow_append():
    cs, peer = socket.socketpair()
    client = tcp.Client(bs=8, rxbsLimit=5)
    client.cs = cs
    client.accepted = True
    client.opened = True

    try:
        peer.sendall(b"abcdef")
        client.serviceReceives()

        assert client.rxbs == b"abcde"
        assert client.rxbsOverflowed
        assert client.cutoff
        assert client.cs is None
    finally:
        client.close()
        peer.close()


def test_tls_server_propagates_receive_limits_and_removes_overflow():
    cert_dir = os.path.join(os.path.dirname(__file__), "certs")
    server = tcp.ServerTls(
        tymth=tyming.Tymist().tymen(),
        ha=("127.0.0.1", unused_port()),
        bs=8,
        rxbsLimit=4,
        rxbsPerTurn=2,
        keypath=os.path.join(cert_dir, "server_key.pem"),
        certpath=os.path.join(cert_dir, "server_cert.pem"),
        cafilepath=os.path.join(cert_dir, "client.pem"),
        certify=ssl.CERT_REQUIRED)
    client = None

    try:
        assert server.reopen()
        client = tcp.ClientTls(
            tymth=tyming.Tymist().tymen(),
            ha=server.eha,
            bs=8,
            certedhost="localhost",
            keypath=os.path.join(cert_dir, "client_key.pem"),
            certpath=os.path.join(cert_dir, "client_cert.pem"),
            cafilepath=os.path.join(cert_dir, "server.pem"),
            certify=ssl.CERT_REQUIRED,
            hostify=True)
        assert client.reopen()

        deadline = time.monotonic() + 5.0
        while not (client.connected and server.ixes):
            client.serviceConnect()
            server.serviceConnects()
            assert time.monotonic() < deadline
            time.sleep(0.001)

        ca, remoter = next(iter(server.ixes.items()))
        assert isinstance(remoter, tcp.serving.RemoterTls)
        assert remoter.rxbsLimit == 4
        assert remoter.rxbsPerTurn == 2

        client.tx(b"abcde")
        while client.txbs:
            client.serviceSends()
            assert time.monotonic() < deadline
            time.sleep(0.001)

        server.serviceReceivesAllIx()
        assert remoter.rxbs == b"ab"
        assert ca in server.ixes

        server.serviceReceivesAllIx()
        assert remoter.rxbs == b"abcd"
        assert ca in server.ixes

        server.serviceReceivesAllIx()
        assert remoter.rxbs == b"abcd"
        assert remoter.rxbsOverflowed
        assert remoter.cs is None
        assert ca not in server.ixes
    finally:
        if client is not None:
            client.close()
        for remoter in server.cxes.values():
            remoter.close()
        server.cxes.clear()
        server.close()
