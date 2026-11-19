import time

import pytest

import pymysql
from pymysql import err
from pymysql.retry import run_in_transaction
from pymysql.tests import base


class PacketLimitTest(base.PyMySQLTestCase):
    def setUp(self):
        super().setUp()
        self.conn = self.connect()
        self.safe_create_table(
            self.conn,
            "t_packet_limit",
            "create table t_packet_limit (id int primary key, data longtext)",
        )
        self.conn.commit()

    def _kill_connection(self, victim):
        killer = self.connect()
        killer.cursor().execute(f"KILL {victim.thread_id()}")
        killer.close()
        time.sleep(0.2)

    def test_server_limit_is_detected(self):
        cur = self.conn.cursor()
        cur.execute("SELECT @@max_allowed_packet")
        server_limit = cur.fetchone()[0]
        self.assertEqual(
            self.conn.get_server_max_allowed_packet(),
            server_limit,
        )
        self.assertLess(
            self.conn.effective_max_allowed_packet,
            server_limit,
        )

    def test_oversize_statement_raises_before_send_and_stays_connected(self):
        cur = self.conn.cursor()
        limit = self.conn.effective_max_allowed_packet
        oversize = "x" * limit
        sql = "INSERT INTO t_packet_limit VALUES (1, '" + oversize + "')"

        with pytest.raises(err.PacketTooLargeError) as cm:
            cur.execute(sql)
        self.assertEqual(cm.value.args[0], 2020)

        # The guard runs before any byte is written: connection still works
        # and nothing was inserted.
        self.assertTrue(self.conn.open)
        cur.execute("SELECT 1")
        self.assertEqual(cur.fetchone(), (1,))
        cur.execute("SELECT COUNT(*) FROM t_packet_limit")
        self.assertEqual(cur.fetchone(), (0,))

    def test_executemany_chunks_at_server_limit(self):
        # Total payload is well above the 16MB server limit. Old code either
        # capped batches at 1MB (many round trips) or, for hand-built SQL,
        # crashed the connection; here executemany must succeed in one logical
        # call with chunks bounded by the real server limit.
        cur = self.conn.cursor()
        row_size = 50_000
        rows = [(i, "y" * row_size) for i in range(400)]
        affected = cur.executemany(
            "INSERT INTO t_packet_limit (id, data) VALUES (%s, %s)", rows
        )
        self.conn.commit()

        self.assertEqual(affected, 400)
        cur.execute("SELECT COUNT(*), LENGTH(MIN(data)) FROM t_packet_limit")
        self.assertEqual(cur.fetchone(), (400, row_size))

    def test_executemany_oversize_single_row_raises_without_sending(self):
        cur = self.conn.cursor()
        limit = self.conn.effective_max_allowed_packet
        rows = [(i, "q" * limit) for i in range(3)]

        with pytest.raises(err.PacketTooLargeError):
            cur.executemany(
                "INSERT INTO t_packet_limit (id, data) VALUES (%s, %s)", rows
            )
        self.assertTrue(self.conn.open)
        cur.execute("SELECT COUNT(*) FROM t_packet_limit")
        self.assertEqual(cur.fetchone(), (0,))

    def test_disconnect_in_open_transaction_is_marked_rolled_back(self):
        victim = self.connect(autocommit=False)
        victim.begin()
        vcur = victim.cursor()
        vcur.execute("INSERT INTO t_packet_limit VALUES (1, 'a')")

        self._kill_connection(victim)
        with pytest.raises(pymysql.OperationalError) as cm:
            vcur.execute("INSERT INTO t_packet_limit VALUES (2, 'b')")

        self.assertEqual(cm.value.transaction_state, err.TRANSACTION_ROLLED_BACK)
        self.assertFalse(victim.open)

        check = self.connect().cursor()
        check.execute("SELECT COUNT(*) FROM t_packet_limit")
        self.assertEqual(check.fetchone(), (0,))

    def test_disconnect_while_sending_autocommit_is_unknown(self):
        victim = self.connect(autocommit=True)
        self._kill_connection(victim)
        # A large statement makes the write itself fail rather than the read.
        big = "z" * (4 * 1024 * 1024)
        with pytest.raises(pymysql.OperationalError) as cm:
            victim.cursor().execute(
                "INSERT INTO t_packet_limit VALUES (1, '" + big + "')"
            )
        self.assertEqual(cm.value.transaction_state, err.TRANSACTION_UNKNOWN)

    def test_run_in_transaction_replays_after_rollback_disconnect(self):
        self.safe_create_table(
            self.conn, "t_idempotent", "create table t_idempotent (id int primary key)"
        )
        params = self.databases[0].copy()
        params["autocommit"] = False
        state = {"attempts": 0}

        def work(cursor, conn):
            state["attempts"] += 1
            cursor.execute(
                "INSERT INTO t_idempotent VALUES (1) ON DUPLICATE KEY UPDATE id = id"
            )
            if state["attempts"] == 1:
                self._kill_connection(conn)
                cursor.execute("SELECT 1")

        run_in_transaction(lambda: pymysql.connect(**params), work, attempts=3)

        check = self.connect().cursor()
        check.execute("SELECT COUNT(*) FROM t_idempotent")
        self.assertEqual(check.fetchone(), (1,))
        self.assertEqual(state["attempts"], 2)
