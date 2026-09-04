"""DDL that carries metadata rather than shape.

Comments, declared column sizes and constraints are all things Snowflake records
alongside a relation and reports back through the catalog. DuckDB's generator
has no syntax for most of them and used to drop them with a warning, so the
DDL appeared to succeed while the metadata silently went missing - which is
exactly what dbt's `persist_docs` writes.
"""

import pytest
from snowflake.connector.errors import ProgrammingError


class TestComments:
    def test_create_table_comments_reach_the_catalog(self, conn):
        with conn.cursor() as cur:
            cur.execute(
                "CREATE OR REPLACE TABLE cm ("
                "  id INT COMMENT 'the id',"
                "  name VARCHAR"
                ") COMMENT = 'a table'"
            )

            cur.execute(
                "SELECT comment FROM db.INFORMATION_SCHEMA.TABLES "
                "WHERE table_name = 'CM'"
            )
            assert cur.fetchone()[0] == "a table"

            cur.execute(
                "SELECT column_name, comment FROM db.INFORMATION_SCHEMA.COLUMNS "
                "WHERE table_name = 'CM' ORDER BY ordinal_position"
            )
            assert cur.fetchall() == [("ID", "the id"), ("NAME", None)]

    def test_create_view_comment(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE VIEW cv COMMENT = 'a view' AS SELECT 1 x")
            cur.execute(
                "SELECT comment FROM db.INFORMATION_SCHEMA.VIEWS "
                "WHERE table_name = 'CV'"
            )
            assert cur.fetchone()[0] == "a view"

    @pytest.mark.parametrize("keyword", ["MODIFY", "ALTER"])
    def test_alter_column_comment(self, conn, keyword):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE ac (name VARCHAR)")
            cur.execute(f"ALTER TABLE ac {keyword} COLUMN name COMMENT 'the name'")
            cur.execute(
                "SELECT comment FROM db.INFORMATION_SCHEMA.COLUMNS "
                "WHERE table_name = 'AC' AND column_name = 'NAME'"
            )
            assert cur.fetchone()[0] == "the name"

    def test_multi_column_alter(self, conn):
        """Snowflake names every column in one ALTER; sqlglot cannot parse that
        form, so it is re-read from the statement's own SQL."""
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE mc (id INT, name VARCHAR)")
            cur.execute(
                'ALTER TABLE mc ALTER "ID" COMMENT $$the id$$ , '
                '"NAME" COMMENT $$the name$$'
            )
            cur.execute(
                "SELECT column_name, comment FROM db.INFORMATION_SCHEMA.COLUMNS "
                "WHERE table_name = 'MC' ORDER BY ordinal_position"
            )
            assert cur.fetchall() == [("ID", "the id"), ("NAME", "the name")]

    def test_multi_column_alter_keeps_a_comma_in_the_text(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE mc2 (id INT)")
            cur.execute('ALTER TABLE mc2 ALTER COLUMN "ID" COMMENT $$has, a comma$$')
            cur.execute(
                "SELECT comment FROM db.INFORMATION_SCHEMA.COLUMNS "
                "WHERE table_name = 'MC2' AND column_name = 'ID'"
            )
            assert cur.fetchone()[0] == "has, a comma"

    def test_qualified_multi_column_alter(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE mc3 (id INT, name VARCHAR)")
            cur.execute(
                'ALTER TABLE db.schema.mc3 ALTER "ID" COMMENT $$a$$ , '
                '"NAME" COMMENT $$b$$'
            )
            cur.execute(
                "SELECT comment FROM db.INFORMATION_SCHEMA.COLUMNS "
                "WHERE table_name = 'MC3' ORDER BY ordinal_position"
            )
            assert [row[0] for row in cur.fetchall()] == ["a", "b"]

    def test_if_not_exists_leaves_an_existing_comment_alone(self, conn):
        """`IF NOT EXISTS` on a relation that is there does nothing in
        Snowflake - and `COMMENT ON` has no such guard of its own."""
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS ine (id INT) COMMENT = 'first table'"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS ine (id INT) COMMENT = 'second table'"
            )
            cur.execute(
                "SELECT comment FROM db.INFORMATION_SCHEMA.TABLES "
                "WHERE table_name = 'INE'"
            )
            assert cur.fetchone()[0] == "first table"

    def test_comment_with_an_embedded_quote(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE cq (id INT) COMMENT = 'it''s a table'")
            cur.execute(
                "SELECT comment FROM db.INFORMATION_SCHEMA.TABLES "
                "WHERE table_name = 'CQ'"
            )
            assert cur.fetchone()[0] == "it's a table"


class TestDeclaredLengths:
    """DuckDB drops a VARCHAR's size, so ALTER has to keep the record in step."""

    def _length(self, cur, column):
        cur.execute(
            "SELECT character_maximum_length FROM db.INFORMATION_SCHEMA.COLUMNS "
            f"WHERE table_name = 'LC' AND column_name = '{column}'"
        )
        return cur.fetchone()[0]

    def test_alter_column_type_updates_the_length(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE lc (id INT, name VARCHAR(20))")
            assert self._length(cur, "NAME") == 20

            cur.execute("ALTER TABLE lc ALTER COLUMN name SET DATA TYPE VARCHAR(30)")
            assert self._length(cur, "NAME") == 30

    def test_added_column_records_its_length(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE lc (id INT)")
            cur.execute("ALTER TABLE lc ADD COLUMN extra VARCHAR(7)")
            assert self._length(cur, "EXTRA") == 7

    def test_retyping_away_from_varchar_forgets_the_length(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE lc (id INT, extra VARCHAR(7))")
            cur.execute("ALTER TABLE lc ALTER COLUMN extra SET DATA TYPE INT")
            assert self._length(cur, "EXTRA") is None

    def test_dropping_a_column_forgets_the_length(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE lc (id INT, name VARCHAR(20))")
            cur.execute("ALTER TABLE lc DROP COLUMN name")
            cur.execute("ALTER TABLE lc ADD COLUMN name VARCHAR")
            assert self._length(cur, "NAME") is None


class TestAlterTable:
    def test_alter_column_type(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE at1 (c1 INT)")
            cur.execute("ALTER TABLE at1 ALTER COLUMN c1 SET DATA TYPE BIGINT")
            cur.execute(
                "SELECT data_type FROM db.INFORMATION_SCHEMA.COLUMNS "
                "WHERE table_name = 'AT1' AND column_name = 'C1'"
            )
            assert cur.fetchone()[0] == "NUMBER"

    def test_alter_column_nullability(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE at2 (name VARCHAR)")

            cur.execute("ALTER TABLE at2 ALTER COLUMN name SET NOT NULL")
            cur.execute(
                "SELECT is_nullable FROM db.INFORMATION_SCHEMA.COLUMNS "
                "WHERE table_name = 'AT2' AND column_name = 'NAME'"
            )
            assert cur.fetchone()[0] == "NO"

            cur.execute("ALTER TABLE at2 ALTER COLUMN name DROP NOT NULL")
            cur.execute(
                "SELECT is_nullable FROM db.INFORMATION_SCHEMA.COLUMNS "
                "WHERE table_name = 'AT2' AND column_name = 'NAME'"
            )
            assert cur.fetchone()[0] == "YES"

    def test_alter_column_default(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE at3 (id INT)")
            cur.execute("ALTER TABLE at3 ALTER COLUMN id SET DEFAULT 7")
            cur.execute("ALTER TABLE at3 ALTER COLUMN id DROP DEFAULT")

    def test_constraints_are_accepted(self, conn):
        """Snowflake records constraints as metadata; DROP CONSTRAINT has no
        DuckDB equivalent at all, so both forms are accepted as no-ops."""
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE at4 (id INT)")
            cur.execute("ALTER TABLE at4 ADD CONSTRAINT pk PRIMARY KEY (id)")
            cur.execute("ALTER TABLE at4 DROP CONSTRAINT pk")


class TestTruncate:
    def test_truncate_if_exists_on_a_real_table(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE tr (id INT)")
            cur.execute("INSERT INTO tr VALUES (1)")
            cur.execute("TRUNCATE TABLE IF EXISTS tr")
            cur.execute("SELECT COUNT(*) FROM tr")
            assert cur.fetchone()[0] == 0

    def test_truncate_if_exists_on_a_missing_table(self, conn):
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE IF EXISTS no_such_table")

    def test_truncate_without_the_guard_still_raises(self, conn):
        with conn.cursor() as cur:
            with pytest.raises(Exception, match="NO_SUCH_TABLE"):
                cur.execute("TRUNCATE TABLE no_such_table")


class TestSnowflakeOnlyRelationProperties:
    """SECURE, MATERIALIZED and TRANSIENT have no local meaning, but a relation
    declared with any of them still has to be created."""

    @pytest.mark.parametrize(
        "ddl,name",
        [
            ("CREATE OR REPLACE SECURE VIEW sv AS SELECT 1 x", "SV"),
            ("CREATE OR REPLACE MATERIALIZED VIEW mv AS SELECT 1 x", "MV"),
            ("CREATE OR REPLACE TRANSIENT TABLE tt (x INT)", "TT"),
        ],
    )
    def test_relation_is_created(self, conn, ddl, name):
        with conn.cursor() as cur:
            cur.execute(ddl)
            cur.execute(
                "SELECT COUNT(*) FROM db.INFORMATION_SCHEMA.TABLES "
                f"WHERE table_name = '{name}'"
            )
            assert cur.fetchone()[0] == 1


class TestInformationSchemaViews:
    def test_constraints_are_reported(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE ic (id INT PRIMARY KEY, b INT UNIQUE)")

            cur.execute(
                "SELECT table_name, constraint_type "
                "FROM db.INFORMATION_SCHEMA.TABLE_CONSTRAINTS "
                "WHERE table_name = 'IC' ORDER BY constraint_type"
            )
            assert cur.fetchall() == [("IC", "PRIMARY KEY"), ("IC", "UNIQUE")]

            cur.execute(
                "SELECT column_name, ordinal_position "
                "FROM db.INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                "WHERE table_name = 'IC' ORDER BY column_name"
            )
            assert cur.fetchall() == [("B", 1), ("ID", 1)]

    def test_catalog_name(self, conn):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM db.INFORMATION_SCHEMA.INFORMATION_SCHEMA_CATALOG_NAME"
            )
            assert cur.fetchone()[0] == "DB"

    def test_roles(self, conn):
        with conn.cursor() as cur:
            cur.execute("SELECT role_name FROM db.INFORMATION_SCHEMA.ENABLED_ROLES")
            assert cur.fetchone()[0] == "SYSADMIN"

    @pytest.mark.parametrize(
        "view",
        [
            "REFERENTIAL_CONSTRAINTS",
            "TABLE_PRIVILEGES",
            "USAGE_PRIVILEGES",
            "OBJECT_PRIVILEGES",
            "VIEW_TABLE_USAGE",
            "EXTERNAL_TABLES",
            "FILE_FORMATS",
            "PROCEDURES",
            "LOAD_HISTORY",
        ],
    )
    def test_views_with_no_local_equivalent_are_empty_not_missing(self, conn, view):
        """An object type SnowDuck cannot host reads back as no rows in the
        right column shape, which is what its absence actually looks like."""
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM db.INFORMATION_SCHEMA.{view}")
            assert cur.fetchall() == []
            assert cur.description

    def test_an_unknown_view_still_raises(self, conn):
        with conn.cursor() as cur:
            with pytest.raises(Exception, match="NOPE"):
                cur.execute("SELECT * FROM db.INFORMATION_SCHEMA.NOPE")


class TestDescribeTable:
    def _describe(self, cur):
        cur.execute("DESCRIBE TABLE dt")
        names = [column.name for column in cur.description]
        return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]

    def test_describe_reports_comments_and_keys(self, conn):
        with conn.cursor() as cur:
            cur.execute(
                "CREATE OR REPLACE TABLE dt ("
                "  id INT PRIMARY KEY COMMENT 'the id',"
                "  code VARCHAR(5) UNIQUE,"
                "  plain INT"
                ")"
            )
            rows = {row["name"]: row for row in self._describe(cur)}

            assert rows["ID"]["comment"] == "the id"
            assert rows["ID"]["primary key"] == "Y"
            assert rows["ID"]["unique key"] == "N"
            assert rows["CODE"]["unique key"] == "Y"
            assert rows["CODE"]["primary key"] == "N"
            assert rows["PLAIN"]["comment"] is None
            assert rows["PLAIN"]["primary key"] == "N"


class TestGetDdl:
    """GET_DDL reports DuckDB's rendering of the object, read at query time."""

    def test_table(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE gd (id INT, n VARCHAR)")
            cur.execute("SELECT GET_DDL('TABLE', 'gd')")
            ddl = cur.fetchone()[0]
            assert "CREATE TABLE" in ddl
            assert "GD" in ddl

    def test_qualified_name(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE gd (id INT)")
            cur.execute("SELECT GET_DDL('TABLE', 'db.schema.gd')")
            assert "GD" in cur.fetchone()[0]

    def test_view(self, conn):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE VIEW gdv AS SELECT 1 x")
            cur.execute("SELECT GET_DDL('VIEW', 'gdv')")
            assert "CREATE VIEW" in cur.fetchone()[0]

    def test_tracks_the_catalog_rather_than_being_frozen(self, conn):
        """The answer is read through a subquery, so a redefinition shows up
        even though the translated SQL is cached."""
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE VIEW gdv AS SELECT 1 x")
            cur.execute("SELECT GET_DDL('VIEW', 'gdv')")
            assert "X" in cur.fetchone()[0]

            cur.execute("CREATE OR REPLACE VIEW gdv AS SELECT 2 renamed")
            cur.execute("SELECT GET_DDL('VIEW', 'gdv')")
            assert "RENAMED" in cur.fetchone()[0]

    def test_a_missing_object_reads_back_as_null(self, conn):
        with conn.cursor() as cur:
            cur.execute("SELECT GET_DDL('TABLE', 'no_such_table')")
            assert cur.fetchone()[0] is None


class TestParserWarnings:
    """sqlglot warns whenever it degrades a statement to an opaque Command, and
    whenever its DuckDB generator meets an ALTER COLUMN clause it does not
    model. SnowDuck handles both of those cases, so the warning is noise for
    exactly the statements that work - and noise trains the reader to ignore
    the warnings that do matter."""

    QUIET = [
        'ALTER TABLE w ALTER "ID" COMMENT $$a$$ , "NAME" COMMENT $$b$$',
        "ALTER TABLE w ALTER COLUMN name SET NOT NULL",
        "ALTER TABLE w ALTER COLUMN name DROP NOT NULL",
        "ALTER TABLE w ALTER COLUMN name SET DATA TYPE VARCHAR(9)",
        "SHOW USER FUNCTIONS",
    ]

    @pytest.mark.parametrize("sql", QUIET)
    def test_handled_statements_are_quiet(self, conn, caplog, sql):
        with conn.cursor() as cur:
            cur.execute("CREATE OR REPLACE TABLE w (id INT, name VARCHAR)")
            caplog.clear()
            with caplog.at_level("WARNING", logger="sqlglot"):
                cur.execute(sql)
        assert caplog.records == []

    def test_an_unhandled_fallback_still_warns(self, conn, caplog):
        """Nothing is hidden: a Command SnowDuck does not claim is replayed."""
        with conn.cursor() as cur:
            with caplog.at_level("WARNING", logger="sqlglot"):
                with pytest.raises(ProgrammingError):
                    cur.execute("CREATE TASK t SCHEDULE = '1 MINUTE' AS SELECT 1")
        assert any("unsupported syntax" in r.getMessage() for r in caplog.records)
