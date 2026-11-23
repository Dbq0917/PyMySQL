#!/usr/bin/env python3
"""Watchdog that kills long-running InnoDB transactions.

Safety net for transactions that stay open for too long and inflate the
undo/rollback segments (which slows down the whole instance). Batch jobs
such as settlement are protected by an explicit allowlist on the
connecting user and/or client host, so they are never killed.

Typical usage (run from cron every minute, or with --interval):

    python tools/trx_watchdog.py --max-age 600 \
        --protect-user settlement --protect-host '^batch-'

Use --dry-run first to see what would be killed.
"""

import argparse
import logging
import re
import sys
import time

import pymysql
from pymysql.cursors import DictCursor

log = logging.getLogger("trx_watchdog")

# Threads that must never be killed regardless of the allowlist.
ALWAYS_PROTECTED_USERS = frozenset(
    {
        "system user",  # replication / internal threads
        "event_scheduler",
    }
)

CANDIDATE_QUERY = """
SELECT t.trx_id,
       t.trx_mysql_thread_id AS thread_id,
       t.trx_state,
       TIMESTAMPDIFF(SECOND, t.trx_started, NOW()) AS age_seconds,
       t.trx_rows_modified,
       t.trx_rows_locked,
       p.USER AS user,
       p.HOST AS host,
       p.DB AS db,
       p.COMMAND AS command,
       p.INFO AS info
FROM information_schema.innodb_trx t
JOIN information_schema.processlist p
  ON p.ID = t.trx_mysql_thread_id
WHERE TIMESTAMPDIFF(SECOND, t.trx_started, NOW()) >= %s
ORDER BY t.trx_started
"""


def find_long_transactions(conn, max_age):
    """Return transactions open for at least ``max_age`` seconds."""
    with conn.cursor(DictCursor) as cur:
        cur.execute(CANDIDATE_QUERY, (max_age,))
        return cur.fetchall()


def is_protected(row, protect_users, protect_host_patterns, own_thread_id):
    """Decide whether a transaction must never be killed."""
    if row["thread_id"] == own_thread_id:
        return True
    if row["user"] in ALWAYS_PROTECTED_USERS:
        return True
    if row["user"] in protect_users:
        return True
    return any(p.search(row["host"] or "") for p in protect_host_patterns)


def history_list_length(conn):
    """Best-effort read of the InnoDB history list length, or None."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT FROM information_schema.innodb_metrics "
                "WHERE NAME = 'trx_rseg_history_len'"
            )
            row = cur.fetchone()
            if row:
                return int(row[0])
    except pymysql.MySQLError:
        pass
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW ENGINE INNODB STATUS")
            match = re.search(r"History list length (\d+)", cur.fetchone()[2] or "")
            if match:
                return int(match.group(1))
    except pymysql.MySQLError:
        pass
    return None


def run_once(
    conn,
    max_age,
    protect_users=(),
    protect_hosts=(),
    idle_only=False,
    dry_run=False,
):
    """Kill unprotected transactions older than ``max_age`` seconds.

    Returns the list of killed (or, with ``dry_run``, would-be-killed)
    thread ids.
    """
    protect_host_patterns = [re.compile(p) for p in protect_hosts]
    with conn.cursor() as cur:
        cur.execute("SELECT CONNECTION_ID()")
        own_thread_id = cur.fetchone()[0]

    killed = []
    for row in find_long_transactions(conn, max_age):
        if is_protected(row, set(protect_users), protect_host_patterns, own_thread_id):
            log.info(
                "skip protected trx %s (thread %s, user %s@%s, age %ss)",
                row["trx_id"],
                row["thread_id"],
                row["user"],
                row["host"],
                row["age_seconds"],
            )
            continue
        if idle_only and row["command"] != "Sleep":
            log.info(
                "skip active trx %s (thread %s, command %s)",
                row["trx_id"],
                row["thread_id"],
                row["command"],
            )
            continue
        log.warning(
            "%s trx %s: thread %s, %s@%s db=%s age=%ss rows_modified=%s "
            "state=%s last_sql=%.200r",
            "would kill" if dry_run else "killing",
            row["trx_id"],
            row["thread_id"],
            row["user"],
            row["host"],
            row["db"],
            row["age_seconds"],
            row["trx_rows_modified"],
            row["trx_state"],
            row["info"],
        )
        if not dry_run:
            with conn.cursor() as cur:
                cur.execute(f"KILL {int(row['thread_id'])}")
        killed.append(row["thread_id"])

    hll = history_list_length(conn)
    if hll is not None:
        log.info("history list length: %d", hll)
    return killed


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--unix-socket", default=None)
    parser.add_argument(
        "--max-age",
        type=int,
        default=600,
        help="kill transactions open longer than this many seconds (default: 600)",
    )
    parser.add_argument(
        "--protect-user",
        action="append",
        default=[],
        help="user that must never be killed (repeatable), "
        "e.g. the settlement batch account",
    )
    parser.add_argument(
        "--protect-host",
        action="append",
        default=[],
        help="regex for client hosts that must never be killed (repeatable)",
    )
    parser.add_argument(
        "--idle-only",
        action="store_true",
        help="only kill transactions whose connection is idle (Sleep)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="log what would be killed without killing",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0,
        help="re-check every N seconds; 0 runs once and exits (default: 0)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    if args.unix_socket:
        connect_kwargs = {
            "unix_socket": args.unix_socket,
            "user": args.user,
            "password": args.password,
            "autocommit": True,
        }
    else:
        connect_kwargs = {
            "host": args.host,
            "port": args.port,
            "user": args.user,
            "password": args.password,
            "autocommit": True,
        }
    conn = pymysql.connect(**connect_kwargs)
    try:
        while True:
            run_once(
                conn,
                max_age=args.max_age,
                protect_users=args.protect_user,
                protect_hosts=args.protect_host,
                idle_only=args.idle_only,
                dry_run=args.dry_run,
            )
            if args.interval <= 0:
                return 0
            time.sleep(args.interval)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
