"""A small thread-safe connection pool for PyMySQL.

Firewalls and load balancers silently drop idle TCP connections after a
fixed idle timeout (10 minutes is common).  A pooled connection handed out
without any liveness check then fails on its first query, forcing the
application to retry.

Pinging every connection on every checkout would add one network round trip
per request, which is unacceptable at high QPS.  This pool therefore takes
the middle ground:

* Every idle entry remembers when the connection was last returned.
* On checkout, connections idle for less than ``idle_timeout`` are handed
  out without any network I/O ("hot" path, no extra round trip).
* Connections idle for at least ``idle_timeout`` are probed with
  ``Connection.ping()``; dead ones are closed and replaced transparently.

Set ``idle_timeout`` below the firewall's idle timeout (e.g. 540 seconds for
a 600 second firewall timeout) so a silently dropped connection is detected
before it is ever handed to application code.
"""

import contextlib
import threading
import time
from collections import deque

from . import err
from .connections import Connection

__all__ = ["ConnectionPool", "PoolClosedError", "PoolTimeoutError"]


class PoolError(err.Error):
    """Base class for connection pool errors."""


class PoolClosedError(PoolError):
    """Raised when the pool has been closed."""


class PoolTimeoutError(PoolError):
    """Raised when no connection becomes available in time."""


class _PooledEntry:
    __slots__ = ("connection", "deadline", "last_used")

    def __init__(self, connection, last_used, deadline=None):
        self.connection = connection
        self.last_used = last_used
        self.deadline = deadline


class ConnectionPool:
    """A bounded, thread-safe pool of :class:`~pymysql.connections.Connection`.

    :param min_size: Minimum number of connections kept open by the
        background eviction thread.
    :param max_size: Maximum number of connections the pool may create.
    :param idle_timeout: Seconds a connection may stay idle before it is
        pinged on checkout instead of being trusted blindly.  ``None``
        disables on-checkout probing entirely.
    :param max_lifetime: Seconds after which a connection is recycled even
        if it is still healthy.  ``None`` disables lifetime recycling.
    :param acquire_timeout: Seconds to wait for a free connection when the
        pool is saturated.  ``None`` waits forever.
    :param eviction_interval: If not ``None``, run a background thread that
        every ``eviction_interval`` seconds closes connections idle for more
        than ``idle_timeout``, down to ``min_size`` connections.
    :param reset_on_return: Roll back any open transaction when a connection
        is returned, so the next user never inherits an unfinished
        transaction.
    :param connection_class: Class instantiated for new connections.
    :param connection_kwargs: Keyword arguments passed to
        ``connection_class`` for every new connection.
    """

    def __init__(
        self,
        *,
        max_size=10,
        min_size=0,
        idle_timeout=600,
        max_lifetime=None,
        acquire_timeout=10,
        eviction_interval=None,
        reset_on_return=True,
        connection_class=Connection,
        **connection_kwargs,
    ):
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        if min_size < 0 or min_size > max_size:
            raise ValueError("min_size must be between 0 and max_size")
        if idle_timeout is not None and idle_timeout <= 0:
            raise ValueError("idle_timeout must be positive or None")
        if max_lifetime is not None and max_lifetime <= 0:
            raise ValueError("max_lifetime must be positive or None")
        if eviction_interval is not None and eviction_interval <= 0:
            raise ValueError("eviction_interval must be positive or None")

        self._max_size = max_size
        self._min_size = min_size
        self._idle_timeout = idle_timeout
        self._max_lifetime = max_lifetime
        self._acquire_timeout = acquire_timeout
        self._eviction_interval = eviction_interval
        self._reset_on_return = reset_on_return
        self._connection_class = connection_class
        self._connection_kwargs = connection_kwargs

        self._idle = deque()
        self._active = 0
        self._created = 0
        self._closed = False
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._evictor = None
        self._stop_event = None
        self._stale_closed = 0
        self._ping_failures = 0

        if eviction_interval is not None:
            self._stop_event = threading.Event()
            self._evictor = threading.Thread(
                target=self._eviction_loop,
                name="pymysql-pool-evictor",
                daemon=True,
            )
            self._evictor.start()

    @property
    def max_size(self):
        return self._max_size

    @property
    def idle_count(self):
        """Number of currently idle connections."""
        with self._lock:
            return len(self._idle)

    @property
    def active_count(self):
        """Number of connections currently checked out."""
        with self._lock:
            return self._active

    @property
    def size(self):
        """Total number of open connections (idle + checked out)."""
        with self._lock:
            return self._active + len(self._idle)

    @property
    def stats(self):
        """Counters useful for monitoring."""
        with self._lock:
            return {
                "size": self._active + len(self._idle),
                "idle": len(self._idle),
                "active": self._active,
                "created": self._created,
                "stale_closed": self._stale_closed,
                "ping_failures": self._ping_failures,
            }

    def _now(self):
        return time.monotonic()

    def _new_connection(self):
        connection = self._connection_class(**self._connection_kwargs)
        self._created += 1
        return connection

    def _close_quietly(self, connection):
        with contextlib.suppress(Exception):
            connection.close()

    def _take_idle_locked(self, now):
        """Remove one idle entry from the queue.

        Returns ``(entry, needs_ping)``; ``entry`` is ``None`` when the
        queue is empty.  Connections past ``max_lifetime`` are discarded in
        place.  The caller holds the lock and this method does no I/O.
        """
        while self._idle:
            entry = self._idle.popleft()
            if self._max_lifetime is not None and now >= entry.deadline:
                self._close_quietly(entry.connection)
                self._stale_closed += 1
                continue
            needs_ping = (
                self._idle_timeout is not None
                and now - entry.last_used >= self._idle_timeout
            )
            return entry, needs_ping
        return None, False

    def acquire(self, timeout=None):
        """Borrow a connection from the pool.

        Hot (recently used) connections are returned without any network
        round trip; cold connections are pinged and replaced transparently
        if the firewall already dropped them.
        """
        if timeout is None:
            timeout = self._acquire_timeout
        # Blocking deadlines use the real clock: wait() must always time out
        # even though _now() is injectable for idle-age testing.
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            with self._condition:
                if self._closed:
                    raise PoolClosedError("Connection pool is closed")
                now = self._now()
                entry, needs_ping = self._take_idle_locked(now)
                if entry is not None:
                    self._active += 1
                    connection = entry.connection
                elif self._active + len(self._idle) < self._max_size:
                    self._active += 1
                    connection = None
                else:
                    if deadline is None:
                        self._condition.wait()
                    else:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise PoolTimeoutError(
                                "Timed out waiting for a pooled connection"
                            )
                        self._condition.wait(remaining)
                    continue

            # All network I/O happens outside the lock so ping/connect
            # latency never blocks unrelated threads.
            if connection is None:
                try:
                    return self._new_connection()
                except Exception:
                    with self._condition:
                        self._active -= 1
                        self._condition.notify()
                    raise

            if not needs_ping:
                return connection

            try:
                connection.ping()
            except Exception:
                self._abandon(connection)
                continue
            return connection

    def _abandon(self, connection):
        """Discard a connection that failed its checkout probe."""
        with self._condition:
            self._active -= 1
            self._ping_failures += 1
            self._close_quietly(connection)
            self._stale_closed += 1
            self._condition.notify()

    def release(self, connection):
        """Return a connection to the pool.

        The connection is discarded instead of pooled if it is closed or if
        rolling back its unfinished transaction fails.
        """
        if connection is None:
            return
        with self._condition:
            if self._active <= 0:
                return
            self._active -= 1

            healthy = connection.open
            if healthy and self._reset_on_return:
                try:
                    connection.rollback()
                except Exception:
                    healthy = False

            if not healthy or self._closed:
                self._close_quietly(connection)
                self._stale_closed += 1
            else:
                now = self._now()
                deadline = (
                    None if self._max_lifetime is None else now + self._max_lifetime
                )
                self._idle.append(_PooledEntry(connection, now, deadline))
            self._condition.notify()

    def connection(self):
        """Context manager yielding a pooled connection."""
        return _PooledConnectionContext(self)

    def close(self):
        """Close every idle connection and stop the eviction thread."""
        with self._condition:
            if self._closed:
                return
            self._closed = True
            while self._idle:
                entry = self._idle.popleft()
                self._close_quietly(entry.connection)
                self._stale_closed += 1
            self._condition.notify_all()
        if self._evictor is not None:
            self._stop_event.set()
            self._evictor.join(timeout=self._eviction_interval)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def _eviction_loop(self):
        while not self._stop_event.wait(self._eviction_interval):
            with self._condition:
                if self._closed:
                    return
                self._evict_idle_locked(self._now())

    def _evict_idle_locked(self, now):
        """Close long-idle connections, keeping at least min_size open."""
        if self._idle_timeout is None:
            return
        survivors = deque()
        while self._idle:
            entry = self._idle.popleft()
            expired_lifetime = self._max_lifetime is not None and now >= entry.deadline
            if len(survivors) < self._min_size or (
                now - entry.last_used < self._idle_timeout and not expired_lifetime
            ):
                survivors.append(entry)
            else:
                self._close_quietly(entry.connection)
                self._stale_closed += 1
        self._idle = survivors


class _PooledConnectionContext:
    __slots__ = ("_connection", "_pool")

    def __init__(self, pool):
        self._pool = pool
        self._connection = None

    def __enter__(self):
        self._connection = self._pool.acquire()
        return self._connection

    def __exit__(self, exc_type, exc, tb):
        self._pool.release(self._connection)
        self._connection = None
