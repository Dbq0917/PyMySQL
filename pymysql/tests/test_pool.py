"""Unit tests for pymysql.pool using fake connections (no MySQL needed)."""

import threading

import pytest

from pymysql.pool import (
    ConnectionPool,
    PoolClosedError,
    PoolTimeoutError,
)


class FakeConnection:
    instances = []

    def __init__(self, **kwargs):
        self.open = True
        self.pings = 0
        self.rollbacks = 0
        self.closed = False
        self.fail_next_ping = False
        self.kwargs = kwargs
        FakeConnection.instances.append(self)

    def ping(self):
        self.pings += 1
        if self.fail_next_ping or not self.open:
            self.open = False
            raise RuntimeError("connection lost")

    def rollback(self):
        if not self.open:
            raise RuntimeError("connection lost")
        self.rollbacks += 1

    def close(self):
        self.open = False
        self.closed = True


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


@pytest.fixture
def pool_factory(monkeypatch):
    pools = []
    clock = FakeClock()

    def factory(**kwargs):
        kwargs.setdefault("connection_class", FakeConnection)
        pool = ConnectionPool(**kwargs)
        monkeypatch.setattr(pool, "_now", clock)
        pools.append(pool)
        return pool

    yield factory, clock

    for pool in pools:
        pool.close()
    FakeConnection.instances.clear()


def test_creates_connection_when_empty(pool_factory):
    factory, _clock = pool_factory
    pool = factory(max_size=5)

    conn = pool.acquire()

    assert isinstance(conn, FakeConnection)
    assert pool.active_count == 1
    assert pool.idle_count == 0
    assert pool.stats["created"] == 1
    assert conn.pings == 0


def test_hot_connection_reused_without_ping(pool_factory):
    factory, clock = pool_factory
    pool = factory(max_size=5, idle_timeout=600)

    conn = pool.acquire()
    pool.release(conn)
    clock.now += 100

    again = pool.acquire()

    assert again is conn
    assert conn.pings == 0
    assert pool.stats["ping_failures"] == 0


def test_cold_connection_is_pinged_and_reused_when_alive(pool_factory):
    factory, clock = pool_factory
    pool = factory(max_size=5, idle_timeout=600)

    conn = pool.acquire()
    pool.release(conn)
    clock.now += 601

    again = pool.acquire()

    assert again is conn
    assert conn.pings == 1


def test_dead_cold_connection_is_replaced_transparently(pool_factory):
    factory, clock = pool_factory
    pool = factory(max_size=5, idle_timeout=600)

    conn = pool.acquire()
    pool.release(conn)
    clock.now += 601
    conn.fail_next_ping = True

    replacement = pool.acquire()

    assert replacement is not conn
    assert replacement.open
    assert conn.closed
    assert pool.stats["ping_failures"] == 1
    assert pool.stats["stale_closed"] == 1
    assert pool.stats["created"] == 2


def test_replacement_retry_loop_scans_queue(pool_factory):
    factory, clock = pool_factory
    pool = factory(max_size=5, idle_timeout=600)

    first = pool.acquire()
    pool.release(first)
    # Force a second distinct connection by keeping the first one checked
    # out while creating it.
    with pool.connection() as held:
        second = pool.acquire()
        pool.release(second)
    assert held is first
    clock.now += 700
    # LIFO pops second first; it is dead, so the loop must keep scanning
    # the idle queue instead of giving up and creating a new connection.
    second.fail_next_ping = True

    conn = pool.acquire()

    assert conn is first
    assert first.pings == 1
    assert second.closed
    assert pool.stats["ping_failures"] == 1


def test_release_closed_connection_discards_it(pool_factory):
    factory, _clock = pool_factory
    pool = factory(max_size=5)

    conn = pool.acquire()
    conn.open = False
    pool.release(conn)

    assert pool.idle_count == 0
    assert pool.stats["stale_closed"] == 1


def test_release_rolls_back_open_transaction(pool_factory):
    factory, _clock = pool_factory
    pool = factory(max_size=5)

    conn = pool.acquire()
    pool.release(conn)

    assert conn.rollbacks == 1


def test_release_with_failed_rollback_discards_connection(pool_factory):
    factory, _clock = pool_factory
    pool = factory(max_size=5)

    conn = pool.acquire()
    conn.open = False
    pool.release(conn)

    assert pool.idle_count == 0


def test_max_size_blocks_then_times_out(pool_factory):
    factory, _clock = pool_factory
    pool = factory(max_size=2, acquire_timeout=0.05)

    pool.acquire()
    pool.acquire()

    with pytest.raises(PoolTimeoutError):
        pool.acquire()


def test_blocked_acquire_wakes_on_release(pool_factory):
    factory, _clock = pool_factory
    pool = factory(max_size=1, acquire_timeout=5)

    conn = pool.acquire()
    result = {}

    def worker():
        result["conn"] = pool.acquire()

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(0.2)
    assert thread.is_alive()

    pool.release(conn)
    thread.join(2)

    assert not thread.is_alive()
    assert result["conn"] is conn


def test_acquire_after_close_raises(pool_factory):
    factory, _clock = pool_factory
    pool = factory(max_size=5)
    pool.close()

    with pytest.raises(PoolClosedError):
        pool.acquire()


def test_close_closes_idle_connections(pool_factory):
    factory, _clock = pool_factory
    pool = factory(max_size=5)

    conn = pool.acquire()
    pool.release(conn)
    pool.close()

    assert conn.closed
    assert pool.idle_count == 0


def test_eviction_removes_idle_connections_above_min_size(pool_factory):
    factory, clock = pool_factory
    pool = factory(
        max_size=5,
        min_size=1,
        idle_timeout=600,
        eviction_interval=60,
    )

    conns = [pool.acquire() for _ in range(3)]
    for conn in conns:
        pool.release(conn)

    clock.now += 601
    pool._evict_idle_locked(clock.now)

    assert pool.idle_count == 1
    assert sum(1 for conn in conns if conn.closed) == 2


def test_eviction_keeps_fresh_connections(pool_factory):
    factory, clock = pool_factory
    pool = factory(max_size=5, min_size=0, idle_timeout=600)

    conn = pool.acquire()
    pool.release(conn)

    clock.now += 100
    pool._evict_idle_locked(clock.now)

    assert pool.idle_count == 1
    assert not conn.closed


def test_max_lifetime_recycles_connection_on_checkout(pool_factory):
    factory, clock = pool_factory
    pool = factory(max_size=5, idle_timeout=None, max_lifetime=300)

    conn = pool.acquire()
    pool.release(conn)
    clock.now += 301

    replacement = pool.acquire()

    assert replacement is not conn
    assert conn.closed
    assert pool.stats["created"] == 2


def test_context_manager_returns_connection(pool_factory):
    factory, _clock = pool_factory
    pool = factory(max_size=5)

    with pool.connection() as conn:
        assert conn.open
        assert pool.active_count == 1

    assert pool.idle_count == 1


def test_connection_kwargs_passed_through(pool_factory):
    factory, _clock = pool_factory
    pool = factory(max_size=5, host="db.example", user="app")

    conn = pool.acquire()

    assert conn.kwargs == {"host": "db.example", "user": "app"}


def test_invalid_constructor_arguments():
    with pytest.raises(ValueError):
        ConnectionPool(max_size=0, connection_class=FakeConnection)
    with pytest.raises(ValueError):
        ConnectionPool(max_size=2, min_size=3, connection_class=FakeConnection)
    with pytest.raises(ValueError):
        ConnectionPool(idle_timeout=0, connection_class=FakeConnection)


def test_concurrent_checkouts_never_exceed_max_size(pool_factory):
    factory, _clock = pool_factory
    max_size = 4
    pool = factory(max_size=max_size, acquire_timeout=10)
    seen = []
    seen_lock = threading.Lock()

    def worker():
        for _ in range(50):
            conn = pool.acquire()
            with seen_lock:
                seen.append(pool.size)
            pool.release(conn)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)

    assert pool.size <= max_size
    assert max(seen) <= max_size
    assert pool.active_count == 0
