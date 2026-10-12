import asyncio
import time

import psycopg
import pytest

from .utils import PG_MAJOR_VERSION, WINDOWS, wait_until


async def test_pause_resume(bouncer):
    task = bouncer.asleep(0.1, times=50, sequentially=True, connect_timeout=30)

    for _ in range(5):
        await bouncer.aadmin("pause")
        await asyncio.sleep(1)
        await bouncer.aadmin("resume")
        await asyncio.sleep(1)

    await task


async def test_pause_login_does_not_wait(bouncer):
    """
    PAUSE closes all server connections, but clients connecting during a
    pause still log in right away and only their queries wait, even when the
    pause takes longer than client_login_timeout.
    """
    bouncer.admin("set client_login_timeout=2")
    bouncer.test()
    await bouncer.aadmin("pause")
    paused = True
    try:
        async with await bouncer.aconn(connect_timeout=1) as conn:
            query = asyncio.create_task(conn.execute("select 1"))
            await asyncio.sleep(3)
            await bouncer.aadmin("resume")
            paused = False
            await query
    finally:
        if paused:
            await bouncer.aadmin("resume")


def test_enable_disable(bouncer):
    bouncer.test()
    bouncer.admin("disable p0")

    with pytest.raises(
        psycopg.OperationalError,
        match=r'database "p0" is disabled',
    ):
        bouncer.test()
    bouncer.admin("enable p0")
    bouncer.test()


async def test_database_restart(pg, bouncer):
    bouncer.admin("set server_login_retry=1")
    bouncer.test()
    pg.restart()
    bouncer.test()

    tasks = []
    for i in range(1, 6):
        tasks.append(bouncer.asleep(i, dbname="p0"))
        tasks.append(bouncer.asleep(i, dbname="p1"))

    await asyncio.sleep(0.5)
    if WINDOWS:
        # The SelectorEventLoop does not support async subprocesses, so we fall
        # back to regular subprocesses here.
        pg.restart()
    else:
        await pg.arestart()
    for task in tasks:
        try:
            await task
        except psycopg.OperationalError:
            pass
    bouncer.test(dbname="p0")
    bouncer.test(dbname="p1")


@pytest.mark.skipif(PG_MAJOR_VERSION < 14, reason="in_hot_standby was added in PG14")
def test_in_hot_standby_after_promote(pg, bouncer):
    """
    New clients must not keep getting a cached in_hot_standby=on after the
    server got promoted. Clients that look for a primary (libpq
    target_session_attrs, pgjdbc targetServerType, Npgsql
    TargetSessionAttributes) refuse such a connection without running any
    query, so PgBouncer has to refresh the value itself.
    """
    bouncer.admin("set server_login_retry=1")

    def in_hot_standby():
        # Only look at the value from the login, a query would update it
        with bouncer.conn(dbname="p3") as conn:
            return conn.info.parameter_status("in_hot_standby")

    def wait_for_in_hot_standby(value):
        for _ in wait_until(f"in_hot_standby={value} not reported"):
            try:
                if in_hot_standby() == value:
                    return
            except psycopg.OperationalError:
                pass

    def restart_as_standby():
        pg.stop()
        (pg.pgdata / "standby.signal").touch()
        pg.start()
        # A query replaces the server connections that the restart killed,
        # the new server login reports in_hot_standby=on
        for _ in wait_until("PgBouncer did not reconnect"):
            try:
                bouncer.test(dbname="p3")
                break
            except psycopg.OperationalError:
                pass

    try:
        # Promotion while the pooled server connection stays open and idle
        restart_as_standby()
        wait_for_in_hot_standby("on")
        pg.sql("SELECT pg_promote()")
        wait_for_in_hot_standby("off")

        # Promotion where all the server connections are closed
        restart_as_standby()
        wait_for_in_hot_standby("on")
        pg.sql("SELECT pg_promote()")
        pg.sql(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = 'bouncer'"
        )
        wait_for_in_hot_standby("off")

        # The updated welcome message still has the PgBouncer params
        with bouncer.conn(dbname="p3") as conn:
            assert conn.pgconn.parameter_status(b"pgbouncer.version") is not None
    finally:
        if pg.sql_value("SELECT pg_is_in_recovery()"):
            pg.sql("SELECT pg_promote()")


def test_database_change(bouncer):
    bouncer.admin("set server_lifetime=2")
    bouncer.default_db = "p1"
    assert bouncer.sql_value("select current_database()") == "p1"

    with bouncer.ini_path.open() as f:
        original = f.read()
    with bouncer.ini_path.open("w") as f:
        f.write(original.replace("dbname=p1", "dbname=p0"))
    bouncer.admin("reload")
    time.sleep(3)

    assert bouncer.sql_value("select current_database()") == "p0"


def test_reconnect(bouncer):
    pid1 = bouncer.sql_value("select pg_backend_pid()")

    bouncer.admin("reconnect")
    time.sleep(1)

    pid2 = bouncer.sql_value("select pg_backend_pid()")
    assert pid1 != pid2
