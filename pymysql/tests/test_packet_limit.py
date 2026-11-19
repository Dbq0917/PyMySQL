from unittest import mock

import pytest

from pymysql import err
from pymysql.connections import (
    _PHASE_IDLE,
    _PHASE_READING,
    _PHASE_SENDING,
    PACKET_SAFETY_MARGIN,
    Connection,
)
from pymysql.constants import CR, SERVER_STATUS
from pymysql.cursors import Cursor


def _connection(**kwargs):
    with mock.patch.object(Connection, "connect"):
        return Connection(
            host="localhost",
            user="user",
            password="pw",
            defer_connect=True,
            **kwargs,
        )


def test_effective_limit_uses_min_of_client_and_server():
    conn = _connection(max_allowed_packet=8 * 1024 * 1024)
    conn._server_max_allowed_packet = 16 * 1024 * 1024
    assert conn.effective_max_allowed_packet == 8 * 1024 * 1024 - PACKET_SAFETY_MARGIN

    conn = _connection(max_allowed_packet=64 * 1024 * 1024)
    conn._server_max_allowed_packet = 16 * 1024 * 1024
    assert conn.effective_max_allowed_packet == 16 * 1024 * 1024 - PACKET_SAFETY_MARGIN


def test_server_limit_lookup_falls_back_to_client_value():
    conn = _connection(max_allowed_packet=4 * 1024 * 1024)
    assert conn._server_max_allowed_packet is None
    with mock.patch.object(
        Connection, "cursor", side_effect=err.OperationalError(2006, "gone")
    ):
        assert conn.get_server_max_allowed_packet() == 4 * 1024 * 1024
    # The fallback is cached rather than retried on every statement.
    assert conn._server_max_allowed_packet == 4 * 1024 * 1024


def test_check_packet_size_raises_before_sending():
    conn = _connection()
    conn._server_max_allowed_packet = 16 * 1024 * 1024
    limit = conn.effective_max_allowed_packet

    conn._check_packet_size(limit)  # exactly at the limit is allowed
    with pytest.raises(err.PacketTooLargeError) as cm:
        conn._check_packet_size(limit + 1)
    assert cm.value.args[0] == CR.CR_NET_PACKET_TOO_LARGE
    assert isinstance(cm.value, err.OperationalError)
    # Nothing was sent, so no phase change and no connection teardown.
    assert conn._command_phase == _PHASE_IDLE
    assert conn.last_transaction_state is None


def test_infer_state_open_transaction_is_rolled_back():
    conn = _connection()
    conn.server_status = SERVER_STATUS.SERVER_STATUS_IN_TRANS
    conn._command_phase = _PHASE_READING
    conn._last_command = None
    assert conn._infer_transaction_state() == err.TRANSACTION_ROLLED_BACK


def test_infer_state_commit_reply_is_unknown():
    conn = _connection()
    conn.server_status = SERVER_STATUS.SERVER_STATUS_IN_TRANS
    conn._command_phase = _PHASE_READING
    conn._last_command = "COMMIT"
    assert conn._infer_transaction_state() == err.TRANSACTION_UNKNOWN


def test_infer_state_autocommit_sending_is_unknown():
    conn = _connection()
    conn.server_status = 0
    conn._command_phase = _PHASE_SENDING
    conn._last_command = None
    assert conn._infer_transaction_state() == err.TRANSACTION_UNKNOWN


def test_infer_state_autocommit_reading_is_committed():
    conn = _connection()
    conn.server_status = 0
    conn._command_phase = _PHASE_READING
    conn._last_command = None
    assert conn._infer_transaction_state() == err.TRANSACTION_COMMITTED


def test_force_close_records_transaction_state():
    conn = _connection()
    conn.server_status = SERVER_STATUS.SERVER_STATUS_IN_TRANS
    conn._command_phase = _PHASE_READING
    conn._sock = mock.Mock()
    conn._rfile = mock.Mock()
    conn._force_close()
    assert conn.last_transaction_state == err.TRANSACTION_ROLLED_BACK
    assert conn.open is False


def test_classify_query_detects_transaction_control():
    assert Connection._classify_query(b"COMMIT") == "COMMIT"
    assert Connection._classify_query(b"  commit /* x */") == "COMMIT"
    assert Connection._classify_query(b"/* hint */ ROLLBACK") == "ROLLBACK"
    assert Connection._classify_query(b"INSERT INTO t VALUES (1)") is None
    assert Connection._classify_query(b"SELECT 1") is None


def test_cursor_chunk_limit_falls_back_when_connection_cannot_report():
    cursor = Cursor.__new__(Cursor)
    cursor.connection = None
    assert cursor._get_max_stmt_length() == Cursor.max_stmt_length

    broken = mock.Mock()
    type(broken).effective_max_allowed_packet = mock.PropertyMock(
        side_effect=err.OperationalError(2006, "gone")
    )
    cursor.connection = broken
    assert cursor._get_max_stmt_length() == Cursor.max_stmt_length
