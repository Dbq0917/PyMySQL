from unittest import mock

import pytest

from pymysql import err
from pymysql.retry import is_retryable_connection_error, run_in_transaction


def _op_error(state):
    error = err.OperationalError(2013, "lost")
    error.transaction_state = state
    return error


def test_is_retryable_only_for_rolled_back():
    assert is_retryable_connection_error(_op_error(err.TRANSACTION_ROLLED_BACK))
    assert not is_retryable_connection_error(_op_error(err.TRANSACTION_UNKNOWN))
    assert not is_retryable_connection_error(_op_error(err.TRANSACTION_COMMITTED))
    assert not is_retryable_connection_error(err.ProgrammingError(1064, "syntax"))
    assert not is_retryable_connection_error(err.OperationalError(2013, "no state"))


def test_run_in_transaction_commits_on_success():
    conn = mock.Mock()
    cursor = conn.cursor.return_value
    work = mock.Mock(return_value="done")

    result = run_in_transaction(lambda: conn, work, attempts=2)

    assert result == "done"
    conn.begin.assert_called_once_with()
    work.assert_called_once_with(cursor, conn)
    conn.commit.assert_called_once_with()
    conn.close.assert_called_once_with()


def test_run_in_transaction_replays_rolled_back_work():
    connections = [mock.Mock(name=f"conn{i}") for i in range(2)]
    connections[0].doomed = True
    connections[1].doomed = False

    calls = []
    index = {"i": 0}

    def factory():
        conn = connections[index["i"]]
        index["i"] += 1
        return conn

    def work(cursor, conn):
        calls.append(conn)
        if conn.doomed:
            raise _op_error(err.TRANSACTION_ROLLED_BACK)
        cursor.execute("INSERT ...")
        return len(calls)

    result = run_in_transaction(factory, work, attempts=3)

    assert result == 2
    assert len(calls) == 2
    connections[1].begin.assert_called_once_with()
    connections[1].commit.assert_called_once_with()


def test_run_in_transaction_does_not_retry_unknown_state():
    conn = mock.Mock()
    conn.cursor.side_effect = _op_error(err.TRANSACTION_UNKNOWN)
    work = mock.Mock()

    with pytest.raises(err.OperationalError):
        run_in_transaction(lambda: conn, work, attempts=3)

    work.assert_not_called()
    conn.commit.assert_not_called()


def test_run_in_transaction_gives_up_after_attempts():
    def factory():
        c = mock.Mock()
        c.cursor.side_effect = _op_error(err.TRANSACTION_ROLLED_BACK)
        return c

    with pytest.raises(err.OperationalError):
        run_in_transaction(factory, mock.Mock(), attempts=2)


def test_run_in_transaction_validates_attempts():
    with pytest.raises(ValueError):
        run_in_transaction(mock.Mock(), mock.Mock(), attempts=0)
