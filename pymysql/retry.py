"""Helpers for retrying database work without creating duplicate writes.

The driver can detect *when* a connection is lost and infer whether the
in-flight transaction was rolled back, but it cannot know whether a
single autocommit statement reached the server. These helpers therefore
retry at transaction granularity: the whole unit of work is replayed on a
fresh connection, which is safe when the work itself is idempotent (for
example bulk inserts keyed by a primary key, or ``INSERT ... ON DUPLICATE
KEY UPDATE``).
"""

from __future__ import annotations

import contextlib

from . import err


def is_retryable_connection_error(error):
    """Return True if *error* is a disconnect whose transaction is safe to
    replay as a whole.

    ``rolled_back`` means an explicit transaction was open and the server
    discarded it on disconnect, so replaying every statement cannot create
    duplicates. ``unknown`` (typically an autocommit statement or a
    ``COMMIT`` reply that never arrived) is not retried automatically.
    """
    if not isinstance(error, err.OperationalError):
        return False
    state = getattr(error, "transaction_state", None)
    return state == err.TRANSACTION_ROLLED_BACK


def run_in_transaction(connection_factory, work, attempts=3):
    """Run *work* inside one transaction, replaying it on connection loss.

    :param connection_factory: zero-argument callable returning a new
        :class:`~pymysql.connections.Connection`. A fresh connection is used
        for every attempt because a lost connection cannot be reused.
    :param work: callable ``work(cursor, connection) -> result``. It must be
        idempotent: every statement it issues is re-executed when the
        transaction is replayed. It must not call ``commit()`` itself; the
        commit is issued here so its outcome stays under control.
    :param attempts: maximum number of attempts (must be >= 1).

    The transaction is committed only after *work* returns. If the connection
    is lost while the transaction is open, the server has rolled it back and
    the whole *work* call is replayed on a new connection. Losses while
    sending an autocommit statement or waiting for the ``COMMIT`` reply raise
    :class:`~pymysql.err.OperationalError` with
    ``transaction_state == "unknown"`` instead, because a blind replay could
    double-write.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    last_error = None
    for _ in range(attempts):
        conn = connection_factory()
        try:
            conn.begin()
            cursor = conn.cursor()
            result = work(cursor, conn)
            conn.commit()
            return result
        except err.OperationalError as error:
            last_error = error
            if not is_retryable_connection_error(error):
                raise
        finally:
            with contextlib.suppress(Exception):
                conn.close()
    raise last_error
