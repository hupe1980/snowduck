"""Snowflake identifier case folding.

Unquoted identifiers fold to upper case, quoted ones keep their case. This is
what makes a name read back out of the catalog match the name a client
constructs for it - dbt-snowflake looks relations up under the upper-cased
name its own Relation renders to.
"""

import pytest

from snowduck.connector import Connector


@pytest.fixture
def cur():
    connector = Connector(db_file=":memory:")
    connection = connector.connect("TEST_DB", "TEST_SCHEMA")
    cursor = connection.cursor()
    yield cursor
    connection.close()


def column_names(cursor):
    return [column.name for column in cursor.description]


def test_unquoted_object_names_are_folded(cur):
    cur.execute("CREATE TABLE my_model (id INT)")
    cur.execute("SHOW OBJECTS IN TEST_DB.TEST_SCHEMA")
    names = column_names(cur)
    assert [row[names.index("name")] for row in cur.fetchall()] == ["MY_MODEL"]


def test_quoted_object_names_keep_their_case(cur):
    cur.execute('CREATE TABLE "MixedCase" (id INT)')
    cur.execute("SHOW OBJECTS IN TEST_DB.TEST_SCHEMA")
    names = column_names(cur)
    assert [row[names.index("name")] for row in cur.fetchall()] == ["MixedCase"]


def test_unquoted_column_names_are_folded(cur):
    cur.execute("CREATE TABLE col_case (id INT, MixedName VARCHAR)")
    cur.execute("DESCRIBE TABLE col_case")
    assert [row[0] for row in cur.fetchall()] == ["ID", "MIXEDNAME"]


def test_quoted_column_names_keep_their_case(cur):
    cur.execute('CREATE TABLE quoted_cols ("MixedName" VARCHAR)')
    cur.execute("DESCRIBE TABLE quoted_cols")
    assert [row[0] for row in cur.fetchall()] == ["MixedName"]


def test_result_column_names_are_folded(cur):
    cur.execute('SELECT 1 AS foo, 2 AS "bar"')
    assert column_names(cur) == ["FOO", "bar"]


def test_references_resolve_whatever_case_they_are_written_in(cur):
    cur.execute("CREATE TABLE ref_case (id INT)")
    cur.execute("INSERT INTO REF_CASE VALUES (1)")
    cur.execute("SELECT Id FROM Ref_Case")
    assert cur.fetchall() == [(1,)]


def test_string_literals_are_untouched(cur):
    cur.execute("SELECT 'lower Case' AS v")
    assert cur.fetchall() == [("lower Case",)]


def test_schemas_and_databases_are_folded(cur):
    cur.execute("CREATE DATABASE folded_db")
    cur.execute("CREATE SCHEMA folded_db.folded_schema")
    cur.execute("SHOW TERSE SCHEMAS IN DATABASE FOLDED_DB")
    names = column_names(cur)
    schemas = {row[names.index("name")] for row in cur.fetchall()}
    assert "FOLDED_SCHEMA" in schemas
