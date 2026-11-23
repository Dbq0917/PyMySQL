"""Integration tests for pymysql.pool against a real MySQL server."""

import time
import unittest

from pymysql.pool import ConnectionPool
from pymysql.tests.base import PyMySQLTestCase


class ConnectionPoolTestCase(PyMySQLTestCase):
    def setUp(self):
        params = self.databases[0]
        self.pool = ConnectionPool(
            max_size=3,
            idle_timeout=1,
            acquire_timeout=5,
            **params,
        )

    def tearDown(self):
        self.pool.close()

    def test_hot_checkout_needs_no_ping(self):
        conn = self.pool.acquire()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            self.assertEqual(cursor.fetchone(), (1,))
        self.pool.release(conn)

        original_ping = conn.ping
        calls = []

        def counting_ping():
            calls.append(1)
            return original_ping()

        conn.ping = counting_ping
        reused = self.pool.acquire()
        self.assertIs(reused, conn)
        self.assertEqual(calls, [])
        with reused.cursor() as cursor:
            cursor.execute("SELECT 1")
            self.assertEqual(cursor.fetchone(), (1,))
        self.pool.release(reused)

    def test_idle_connection_is_pinged_before_first_query(self):
        conn = self.pool.acquire()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        self.pool.release(conn)

        # idle_timeout is 1 second; after this the connection is "cold".
        time.sleep(1.2)

        original_ping = conn.ping
        calls = []

        def counting_ping():
            calls.append(1)
            return original_ping()

        conn.ping = counting_ping
        reused = self.pool.acquire()
        self.assertIs(reused, conn)
        self.assertEqual(len(calls), 1)

        # First query after the idle period succeeds without app-level retry.
        with reused.cursor() as cursor:
            cursor.execute("SELECT 1")
            self.assertEqual(cursor.fetchone(), (1,))
        self.pool.release(reused)

    def test_silently_dropped_connection_is_replaced(self):
        conn = self.pool.acquire()
        conn_id = conn.thread_id()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        self.pool.release(conn)

        # Simulate a firewall silently dropping the idle connection: the
        # server closes it while the client still holds the socket.
        killer = self.connect()
        with killer.cursor() as cursor:
            cursor.execute("KILL CONNECTION %s", (conn_id,))
        killer.close()

        time.sleep(1.2)

        replacement = self.pool.acquire()
        self.assertIsNot(replacement, conn)
        self.assertTrue(replacement.open)
        self.assertEqual(self.pool.stats["ping_failures"], 1)

        # The first request using the replacement succeeds immediately.
        with replacement.cursor() as cursor:
            cursor.execute("SELECT 1")
            self.assertEqual(cursor.fetchone(), (1,))
        self.pool.release(replacement)

    def test_context_manager(self):
        with self.pool.connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            self.assertEqual(cursor.fetchone(), (1,))
        self.assertEqual(self.pool.idle_count, 1)


if __name__ == "__main__":
    unittest.main()
