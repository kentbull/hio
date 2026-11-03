"""Recover peer-address availability before publishing TCP/TLS acceptance."""
import errno
import socket
import time
from unittest.mock import Mock

import pytest

from hio.base import tyming
from hio.core import tcp


def peer_socket():
    """Supply only the socket operations exercised by Client.open/accept/close."""
    sock = Mock(spec=socket.socket)
    sock.connect_ex.return_value = 0
    sock.getsockname.return_value = ('127.0.0.1', 57000)
    sock.getpeername.return_value = ('127.0.0.1', 56000)
    sock.getsockopt.return_value = 8192
    return sock


@pytest.mark.parametrize('klass', [tcp.Client, tcp.ClientTls])
@pytest.mark.parametrize('result', [0, errno.EISCONN])
@pytest.mark.parametrize('error', [errno.ENOTCONN, errno.EINVAL])
def test_peer_address_retry_preserves_buffers_and_deadline(monkeypatch, klass, result, error):
    """Retain an unfinished socket, replace a shut-down one, and defer TLS wrapping."""
    clock = tyming.Tymist()
    txbs, rxbs = bytearray(b'queued send'), bytearray(b'held receive')
    client = klass(tymth=clock.tymen(), tymeout=1.0, txbs=txbs, rxbs=rxbs)
    first, replacement = peer_socket(), peer_socket()
    first.connect_ex.return_value = result
    first.getpeername.side_effect = [OSError(error, 'peer unavailable'), client.ha]
    monkeypatch.setattr(tcp.clienting.socket, 'socket', Mock(return_value=replacement))
    client.cs, client.opened = first, True
    addresses = client.ca, client.ha
    timer = client.tymer
    deadline = timer.tyme + timer.remaining
    if klass is tcp.ClientTls:
        client.wrap = Mock()

        def handshake():
            client.connected = True
            return True

        client.handshake = Mock(side_effect=handshake)
    try:
        clock.tick(tock=0.125)
        assert client.connect() is False
        assert not client.accepted and not client.connected
        assert (client.ca, client.ha) == addresses
        if error == errno.ENOTCONN:
            assert client.cs is first
            first.close.assert_not_called()
        else:
            first.close.assert_called_once()
            assert client.cs is replacement
        assert client.txbs is txbs and txbs == b'queued send'
        assert client.rxbs is rxbs and rxbs == b'held receive'
        assert client.tymer is timer and timer.tyme + timer.remaining == deadline
        if klass is tcp.ClientTls:
            client.wrap.assert_not_called()
            client.handshake.assert_not_called()

        clock.tick(tock=0.125)
        assert client.connect() is True
        assert client.cs is (first if error == errno.ENOTCONN else replacement)
        assert client.accepted and client.connected
        assert client.ca == ('127.0.0.1', 57000)
        assert client.ha == addresses[1]
        assert client.txbs is txbs and txbs == b'queued send'
        assert client.rxbs is rxbs and rxbs == b'held receive'
        assert client.tymer is timer and timer.tyme + timer.remaining == deadline
        if klass is tcp.ClientTls:
            client.wrap.assert_called_once()
            client.handshake.assert_called_once()
    finally:
        client.close()


@pytest.mark.parametrize('error', [errno.EBADF, errno.ENOTSOCK])
def test_unexpected_peer_address_error_propagates(error):
    """Descriptor/programming errors must not become an endless connection retry."""
    client = tcp.Client()
    sock = peer_socket()
    failure = OSError(error, 'invalid socket')
    sock.getpeername.side_effect = failure
    client.cs = sock
    addresses = client.ca, client.ha
    try:
        with pytest.raises(OSError) as caught:
            client.accept()
        assert caught.value is failure
        assert (client.ca, client.ha) == addresses
        assert not client.accepted
        sock.close.assert_not_called()
    finally:
        client.close()


@pytest.mark.parametrize('error', [errno.ENOTCONN, errno.EINVAL])
def test_local_address_errors_are_not_peer_retries(error):
    client = tcp.Client()
    sock = peer_socket()
    sock.getsockname.side_effect = OSError(error, 'local address unavailable')
    client.cs = sock
    try:
        with pytest.raises(OSError) as caught:
            client.accept()
        assert caught.value.errno == error
        sock.getpeername.assert_not_called()
        sock.close.assert_not_called()
    finally:
        client.close()


def test_queued_bytes_reach_listener_after_initial_refusal():
    """A bound, initially unlistening endpoint can start without losing queued bytes."""
    payload = b'queued before listener starts'
    clock = tyming.Tymist()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(('127.0.0.1', 0))
        listener.setblocking(False)
        with tcp.openClient(ha=listener.getsockname(), tymth=clock.tymen(), tymeout=1.0) as client:
            client.tx(payload)
            timer = client.tymer
            deadline = timer.tyme + timer.remaining
            for _ in range(3):
                assert client.serviceConnect() is False
                assert client.txbs == payload
                clock.tick(tock=0.005)
                time.sleep(0.005)
            listener.listen(1)
            peer, received = None, bytearray()
            limit = time.monotonic() + 1.0
            try:
                while received != payload and time.monotonic() < limit:
                    client.service()
                    if peer is None:
                        try:
                            peer, _ = listener.accept()
                            peer.setblocking(False)
                        except BlockingIOError:
                            pass
                    if peer is not None:
                        try:
                            received.extend(peer.recv(4096))
                        except BlockingIOError:
                            pass
                    clock.tick(tock=0.005)
                    time.sleep(0.005)
                assert received == payload
                assert client.connected and not client.txbs
                assert client.tymer is timer and timer.tyme + timer.remaining == deadline
            finally:
                if peer is not None:
                    peer.close()
