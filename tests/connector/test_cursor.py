import uuid

import pytest
import snowflake.connector


def test_fetchone(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("select 'hello world'")
        res = cur.fetchone()
        assert res[0] == "hello world"


def test_sqlstate(cursor: snowflake.connector.cursor.SnowflakeCursor):
    cursor.execute("select 'hello world'")

    # sqlstate is None on success
    assert cursor.sqlstate is None

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
        cursor.execute("select * from this_table_does_not_exist")

    assert cursor.sqlstate == "42S02"


def test_sfqid(cursor: snowflake.connector.cursor.SnowflakeCursor):
    assert not cursor.sfqid
    cursor.execute("select 1")
    assert uuid.UUID(cursor.sfqid)


def test_create_database(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create database foo")
        assert cur.fetchone()[0] == "Database FOO successfully created."


def test_create_schema(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create schema foo")
        assert cur.fetchone()[0] == "Schema FOO successfully created."


def test_create_schema_in_database(conn: snowflake.connector.SnowflakeConnection):
    """Test that CREATE SCHEMA creates the schema in the correct database."""
    with conn.cursor() as cur:
        cur.execute("CREATE DATABASE test_db")
        cur.execute("USE DATABASE test_db")
        cur.execute("CREATE SCHEMA staging")
        cur.execute("SHOW SCHEMAS")
        schemas = cur.fetchall()
        schema_names = [s[1] for s in schemas]
        assert "STAGING" in schema_names, f"Expected STAGING in {schema_names}"
        assert "PUBLIC" in schema_names, f"Expected PUBLIC in {schema_names}"


def test_create_table(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table foo (ID int)")
        assert cur.fetchone()[0] == "Table FOO successfully created."


def test_create_view(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create view foo as select 1 as id")
        assert cur.fetchone()[0] == "View FOO successfully created."


def test_truncate_table(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table foo (ID int)")
        cur.execute("insert into foo values (1), (2), (3)")
        cur.execute("truncate table foo")
        assert cur.fetchone()[0] == "Statement executed successfully."


def test_use_database(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create database foo")
        cur.execute("use database foo")
        assert cur.fetchone()[0] == "Statement executed successfully."

        cur.execute("select current_schema()")
        assert cur.fetchone()[0] == "PUBLIC"


def test_use_schema(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create database foo")
        cur.execute("use database foo")
        cur.execute("create schema bar")
        cur.execute("use schema foo.bar")
        assert cur.fetchone()[0] == "Statement executed successfully."


def test_use_schema_current_db(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create database foo")
        cur.execute("use database foo")
        cur.execute("create schema bar")
        cur.execute("use schema bar")
        assert cur.fetchone()[0] == "Statement executed successfully."


def test_use_role_and_warehouse(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("use role analyst")
        assert cur.fetchone()[0] == "Statement executed successfully."
        cur.execute("use warehouse wh1")
        assert cur.fetchone()[0] == "Statement executed successfully."

        cur.execute("select current_role() as role, current_warehouse() as warehouse")
        assert cur.fetchone() == ("ANALYST", "WH1")


def test_binding_pyformat(conn: snowflake.connector.SnowflakeConnection):
    # check pyformat is the default paramstyle
    assert snowflake.connector.paramstyle == "pyformat"
    with conn.cursor() as cur:
        cur.execute(
            "create table customers (ID int, FIRST_NAME varchar, ACTIVE boolean)"
        )
        cur.execute("insert into customers values (%s, %s, %s)", (1, "Jenny", True))
        cur.execute(
            "insert into customers values (%(id)s, %(name)s, %(active)s)",
            {"id": 2, "name": "Jasper", "active": False},
        )
        # cur.execute("select * from customers")
        cur.execute("select * from identifier('customers')")
        assert cur.fetchall() == [(1, "Jenny", True), (2, "Jasper", False)]
