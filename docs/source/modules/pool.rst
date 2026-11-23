Connection Pool
===============

.. module:: pymysql.pool

Firewalls and load balancers often silently drop TCP connections that have
been idle for a fixed amount of time (ten minutes is a common default).
Checking out such a connection from a pool makes the first query fail with
``OperationalError`` (``Lost connection to MySQL server``), forcing the
application to retry.

Pinging every connection on every checkout would add one network round trip
per request.  ``ConnectionPool`` avoids that by remembering when each idle
connection was last returned:

* Connections idle for less than ``idle_timeout`` are considered *hot* and
  are handed out with no network I/O.
* Connections idle for at least ``idle_timeout`` are probed with
  ``Connection.ping()`` before checkout.  Dead connections are closed and
  replaced transparently, so application code never sees them.

Choose ``idle_timeout`` below the firewall's idle timeout.  For example,
with a ten-minute firewall timeout::

    import pymysql

    pool = pymysql.ConnectionPool(
        max_size=20,
        idle_timeout=540,  # 9 minutes: below the 10 minute firewall cutoff
        host="127.0.0.1",
        user="app",
        password="secret",
        database="app",
    )

    def fetch_one(sql):
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchone()

.. autoclass:: ConnectionPool
   :members:

.. autoclass:: PoolClosedError
.. autoclass:: PoolTimeoutError
