.. _examples:

========
Examples
========

.. _CRUD:

CRUD
----

The following examples make use of a simple table

.. code:: sql

   CREATE TABLE `users` (
       `id` int(11) NOT NULL AUTO_INCREMENT,
       `email` varchar(255) COLLATE utf8_bin NOT NULL,
       `password` varchar(255) COLLATE utf8_bin NOT NULL,
       PRIMARY KEY (`id`)
   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
   AUTO_INCREMENT=1 ;


.. code:: python

    import pymysql.cursors

    # Connect to the database
    connection = pymysql.connect(host='localhost',
                                 user='user',
                                 password='passwd',
                                 database='db',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `users` (`email`, `password`) VALUES (%s, %s)"
            cursor.execute(sql, ('webmaster@python.org', 'very-secret'))

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()

        with connection.cursor() as cursor:
            # Read a single record
            sql = "SELECT `id`, `password` FROM `users` WHERE `email`=%s"
            cursor.execute(sql, ('webmaster@python.org',))
            result = cursor.fetchone()
            print(result)


This example will print:

.. code:: python

    {'id': 1, 'password': 'very-secret'}


Large bulk inserts and safe retries
------------------------------------

Sending a statement larger than the server's ``max_allowed_packet`` makes the
server reject the command or drop the connection outright. PyMySQL therefore
learns the server limit when the connection is first used and:

* raises :class:`pymysql.err.PacketTooLargeError` *before* writing an oversize
  statement, so the connection stays usable and nothing is partially sent;
* makes :meth:`~pymysql.cursors.Cursor.executemany` split bulk inserts into
  statements bounded by the real server limit instead of a fixed one-megabyte
  cap.

.. code:: python

    # Rows totaling far more than max_allowed_packet are loaded in chunks
    # automatically; no manual batching is required.
    rows = [(i, payload) for i, payload in dataset]
    cursor.executemany(
        "INSERT INTO events (id, payload) VALUES (%s, %s)", rows
    )
    connection.commit()

When a connection is lost while a command is in flight, the resulting
:class:`~pymysql.err.OperationalError` carries a ``transaction_state``
attribute:

* ``pymysql.err.TRANSACTION_ROLLED_BACK`` -- an explicit transaction was open
  and the server discarded it on disconnect, so replaying the whole unit of
  work is safe;
* ``pymysql.err.TRANSACTION_COMMITTED`` -- an autocommit statement finished on
  the server before the socket died;
* ``pymysql.err.TRANSACTION_UNKNOWN`` -- the outcome cannot be determined
  (the statement may not have reached the server), so an idempotency key or a
  manual check is required before retrying.

:func:`pymysql.retry.run_in_transaction` implements the safe replay pattern.
The work callback must be idempotent because every statement is re-executed on
each attempt:

.. code:: python

    from pymysql.retry import run_in_transaction

    def import_batch(cursor, connection):
        cursor.executemany(
            "INSERT INTO events (id, payload) VALUES (%s, %s) "
            "ON DUPLICATE KEY UPDATE payload = VALUES(payload)",
            rows,
        )

    run_in_transaction(
        lambda: pymysql.connect(**connection_args),
        import_batch,
        attempts=3,
    )
