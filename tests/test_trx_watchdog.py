"""Integration tests for tools/trx_watchdog.py.

Reproduces the production scenario: a transaction is opened and left
uncommitted (the "10 minute" case, scaled down to a couple of seconds
via --max-age). The watchdog must kill the leaked transaction but must
not touch the protected settlement batch account.
"""

import json
import os
import sys
import time

import pytest

import pymysql

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "tools"))

import trx_watchdog

APP_USER = "wd_app"  # stands in for the application that leaks transactions
BATCH_USER = "wd_batch"  # stands in for the settlement batch
PASSWORD = "wd pass"


def _root_kwargs():
    path = os.path.join(REPO_ROOT, "pymysql", "tests", "databases.json")
    with open(path) as f:
        cfg = json.load(f)[0]
    return {
        "host": cfg.get("host", "127.0.0.1"),
        "port": cfg.get("port", 3306),
        "user": cfg["user"],
        "password": cfg.get("password", ""),
    }


@pytest.fixture()
def admin():
    conn = pymysql.connect(autocommit=True, **_root_kwargs())
    with conn.cursor() as cur:
        for user in (APP_USER, BATCH_USER):
            cur.execute(f"DROP USER IF EXISTS {user}")
            cur.execute(f"CREATE USER {user} IDENTIFIED BY '{PASSWORD}'")
            cur.execute(f"GRANT ALL PRIVILEGES ON test1.* TO {user}")
        cur.execute("DROP TABLE IF EXISTS test1.wd_t")
        cur.execute("CREATE TABLE test1.wd_t (id INT PRIMARY KEY) ENGINE=InnoDB")
    yield conn
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test1.wd_t")
        for user in (APP_USER, BATCH_USER):
            cur.execute(f"DROP USER IF EXISTS {user}")
    conn.close()


def _open_uncommitted(admin, user, row_id):
    """Open a transaction as ``user``, write a row, and never commit."""
    kwargs = _root_kwargs()
    conn = pymysql.connect(
        host=kwargs["host"],
        port=kwargs["port"],
        user=user,
        password=PASSWORD,
        database="test1",
        autocommit=False,
    )
    with conn.cursor() as cur:
        cur.execute("INSERT INTO test1.wd_t (id) VALUES (%s)", (row_id,))
        cur.execute("SELECT CONNECTION_ID()")
        thread_id = cur.fetchone()[0]
    return conn, thread_id


def _alive_thread_ids(admin):
    with admin.cursor() as cur:
        cur.execute("SELECT ID FROM information_schema.processlist")
        return {row[0] for row in cur.fetchall()}


def test_kills_leaked_transaction_but_not_settlement(admin):
    app_conn, app_thread = _open_uncommitted(admin, APP_USER, 1)
    batch_conn, batch_thread = _open_uncommitted(admin, BATCH_USER, 2)
    try:
        time.sleep(2)  # both transactions are now older than max_age=1

        killed = trx_watchdog.run_once(admin, max_age=1, protect_users={BATCH_USER})

        assert app_thread in killed
        assert batch_thread not in killed
        assert app_thread not in _alive_thread_ids(admin)
        assert batch_thread in _alive_thread_ids(admin)

        # The killed connection is dead and its work was rolled back.
        with pytest.raises(pymysql.OperationalError):
            app_conn.ping(reconnect=False)

        # The settlement batch is untouched and can finish normally.
        batch_conn.commit()
        with admin.cursor() as cur:
            cur.execute("SELECT id FROM test1.wd_t ORDER BY id")
            assert [row[0] for row in cur.fetchall()] == [2]
    finally:
        app_conn.close()
        batch_conn.close()


def test_dry_run_kills_nothing(admin):
    app_conn, app_thread = _open_uncommitted(admin, APP_USER, 1)
    try:
        time.sleep(2)

        killed = trx_watchdog.run_once(admin, max_age=1, dry_run=True)

        assert app_thread in killed  # reported as a candidate
        assert app_thread in _alive_thread_ids(admin)  # but not killed
        app_conn.rollback()
    finally:
        app_conn.close()


def test_young_transactions_are_left_alone(admin):
    app_conn, app_thread = _open_uncommitted(admin, APP_USER, 1)
    try:
        killed = trx_watchdog.run_once(admin, max_age=600)
        assert app_thread not in killed
        assert app_thread in _alive_thread_ids(admin)
        app_conn.rollback()
    finally:
        app_conn.close()
