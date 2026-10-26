from __future__ import annotations

import errno
import os
import select
import socket
import struct
import tempfile
import unittest

from rp_ylx.native import native_session_store_or_none


class NativeSendfileErrorsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = native_session_store_or_none()
        self.assertIsNotNone(self.store, "this regression requires the real native module")

    def test_closed_peer_raises_broken_pipe_without_closing_owned_descriptors(self) -> None:
        with tempfile.TemporaryFile() as source:
            source.write(b"abcdef")
            source.flush()
            source.seek(5)
            sender, peer = socket.socketpair()
            try:
                peer.close()
                with self.assertRaises(BrokenPipeError) as raised:
                    self.store.sendfile(sender.fileno(), source.fileno(), 1, 3)
                self.assertEqual(raised.exception.errno, errno.EPIPE)
                self.assertEqual(source.tell(), 5)
                os.fstat(sender.fileno())
                os.fstat(source.fileno())
            finally:
                sender.close()
                peer.close()

    def test_tcp_reset_is_a_connection_error(self) -> None:
        with socket.socket() as listener, tempfile.TemporaryFile() as source:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            listener.settimeout(2)
            with socket.create_connection(listener.getsockname(), timeout=2) as peer:
                sender, _ = listener.accept()
                peer.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            with sender:
                self.assertTrue(select.select([sender], [], [], 2)[0])
                source.write(b"abcdef")
                source.flush()
                with self.assertRaises((BrokenPipeError, ConnectionResetError)) as raised:
                    self.store.sendfile(sender.fileno(), source.fileno(), 0, 6)
                self.assertIn(raised.exception.errno, (errno.EPIPE, errno.ECONNRESET))

    def test_invalid_descriptor_is_not_mistaken_for_client_disconnect(self) -> None:
        with (
            tempfile.TemporaryFile() as source,
            self.assertRaisesRegex(RuntimeError, "send_failed"),
        ):
            self.store.sendfile(-1, source.fileno(), 0, 1)

    def test_early_eof_is_not_mistaken_for_client_disconnect(self) -> None:
        sender, peer = socket.socketpair()
        with sender, peer, tempfile.TemporaryFile() as source:
            source.write(b"x")
            source.flush()
            with self.assertRaisesRegex(RuntimeError, "send_failed"):
                self.store.sendfile(sender.fileno(), source.fileno(), 0, 2)
            self.assertEqual(peer.recv(1), b"x")
