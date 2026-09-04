import logging
import os
import re
import shutil
import uuid
from collections.abc import Iterator
from string import Template
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self, Sequence, cast

import duckdb
import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from snowflake.connector.cursor import ResultMetadata
from sqlglot import exp

from ..dialect import Dialect, DialectContext
from ..dialect.transforms import claims_command
from ..info_schema import InfoSchemaManager
from .rowtype import convert_dbapi_description_to_describe, describe_as_result_metadata

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa

    from .connection import Connection

SQL_SUCCESS = "SELECT 'Statement executed successfully.' as 'status'"
SQL_CREATED_DATABASE = Template(
    "SELECT 'Database ${name} successfully created.' as 'status'"
)
SQL_CREATED_SCHEMA = Template(
    "SELECT 'Schema ${name} successfully created.' as 'status'"
)
SQL_CREATED_TABLE = Template("SELECT 'Table ${name} successfully created.' as 'status'")
SQL_CREATED_VIEW = Template("SELECT 'View ${name} successfully created.' as 'status'")
SQL_CREATED_FUNCTION = Template(
    "SELECT 'Function ${name} successfully created.' as 'status'"
)
SQL_DROPPED = Template("SELECT '${name} successfully dropped.' as 'status'")
SQL_INSERTED_ROWS = Template("SELECT ${count} as 'number of rows inserted'")
SQL_UPDATED_ROWS = Template(
    "SELECT ${count} as 'number of rows updated', 0 as 'number of multi-joined rows updated'"
)
SQL_DELETED_ROWS = Template("SELECT ${count} as 'number of rows deleted'")


def _single_source_table(select: exp.Select) -> exp.Table | None:
    """The table a simple `SELECT ... FROM t` reads from, if there is exactly one.

    sqlglot names this argument ``from_``; older releases used ``from``. Both
    are accepted so the lookup does not silently return nothing after an
    upgrade - which is how nullability inference quietly stopped working.
    """
    source = select.args.get("from_") or select.args.get("from")
    if source is None or not isinstance(source.this, exp.Table):
        return None
    return source.this


def _identifier_name(node: exp.Expression | None) -> str | None:
    """The bare name of a parameter reference in an ALTER SESSION clause."""
    if isinstance(node, exp.Column):
        return node.name
    if isinstance(node, exp.Identifier):
        return str(node.this)
    if isinstance(node, exp.Var):
        return str(node.this)
    return None


_SQLGLOT_LOGGER = logging.getLogger("sqlglot")
_FALLBACK_WARNING = "contains unsupported syntax"


class _DeferredFallbackWarnings(logging.Filter):
    """Hold sqlglot's "falling back to Command" warnings during a parse.

    sqlglot logs a warning whenever it cannot model a statement and degrades it
    to an opaque Command. SnowDuck handles several of those deliberately - the
    whole SHOW family is re-read by :mod:`snowduck.show` - so the warning is
    noise for exactly the statements that work. dbt emits about nine of them
    per build, which reads like a defect.

    The records are captured rather than suppressed: a fallback SnowDuck does
    *not* handle is still worth telling the user about, so it is replayed.
    """

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def filter(self, record: logging.LogRecord) -> bool:
        if _FALLBACK_WARNING in record.getMessage():
            self.records.append(record)
            return False
        return True

    def replay(self) -> None:
        for record in self.records:
            _SQLGLOT_LOGGER.handle(record)


# `LIST @stage` is not modelled by sqlglot's Snowflake parser, which reads the
# leading word as a column and `@stage` as a session variable. Recognising it
# here keeps the stage commands together in the dialect layer.
_STAGE_LISTING = re.compile(r"^\s*(LIST|LS)\s+(@\S+)\s*;?\s*$", re.IGNORECASE)

# `UNSET <var>` is likewise read as a column with an alias rather than a
# statement, so it is recognised here.
_UNSET_VARIABLE = re.compile(r"^\s*UNSET\s+([A-Za-z_][\w$]*)\s*;?\s*$", re.IGNORECASE)


def _as_unset(command: str) -> exp.Command | None:
    match = _UNSET_VARIABLE.match(command)
    if match is None:
        return None
    return exp.Command(this="UNSET", expression=exp.Var(this=match.group(1)))


def _as_stage_listing(command: str) -> exp.Command | None:
    match = _STAGE_LISTING.match(command)
    if match is None:
        return None
    return exp.Command(
        this=match.group(1).upper(), expression=exp.Var(this=match.group(2))
    )


def _parse_snowflake(command: str) -> list[Any]:
    """Parse Snowflake SQL, quietly for statements SnowDuck knowingly handles."""
    for claimed in (_as_stage_listing(command), _as_unset(command)):
        if claimed is not None:
            return [claimed]

    deferred = _DeferredFallbackWarnings()
    _SQLGLOT_LOGGER.addFilter(deferred)
    try:
        expressions = sqlglot.parse(command, read="snowflake")
    finally:
        _SQLGLOT_LOGGER.removeFilter(deferred)

    unhandled = [
        expression
        for expression in expressions
        if isinstance(expression, exp.Command) and not claims_command(expression)
    ]
    if unhandled:
        deferred.replay()

    return expressions


def _is_quoted(identifier: Any) -> bool:
    return bool(getattr(identifier, "quoted", False))


def extract_sql_command(expression: exp.Expression) -> str:
    kind = expression.args.get("kind")

    if isinstance(kind, str):
        return f"{expression.key.upper()} {kind.upper()}"

    if isinstance(kind, exp.Var):
        return f"{expression.key.upper()} {kind.name.upper()}"

    if isinstance(expression, exp.Command):
        key = expression.this
        key_str = key.name if isinstance(key, exp.Identifier) else str(key)
        expr = expression.expression
        if key_str.upper() == "PUT":
            return "PUT"
        if expr is not None:
            expr_sql = expr if isinstance(expr, str) else expr.sql(dialect="snowflake")
            first = expr_sql.split()[0].upper() if expr_sql else ""
            return f"{key_str.upper()} {first}".strip()
        return key_str.upper()

    return expression.key.upper()


_SIZED_STRING_TYPES = (
    exp.DataType.Type.VARCHAR,
    exp.DataType.Type.CHAR,
    exp.DataType.Type.NCHAR,
    exp.DataType.Type.NVARCHAR,
)


def _declared_length(kind: exp.Expression | None) -> int | None:
    """The `n` in `VARCHAR(n)`, for the types that carry one."""
    if not isinstance(kind, exp.DataType) or kind.this not in _SIZED_STRING_TYPES:
        return None
    for parameter in kind.expressions or []:
        value = parameter.this if isinstance(parameter, exp.DataTypeParam) else None
        if isinstance(value, exp.Literal) and not value.is_string:
            try:
                return int(value.this)
            except (TypeError, ValueError):
                return None
        break
    return None


def _dropped_column_name(action: exp.Drop) -> str | None:
    """The column an `ALTER TABLE ... DROP COLUMN` names, if it is one."""
    kind = action.args.get("kind")
    if not isinstance(kind, str) or kind.upper() != "COLUMN":
        return None
    target = action.this
    if isinstance(target, (exp.Column, exp.Identifier)):
        return target.name
    return None


class Cursor:
    def __init__(
        self,
        sf_conn: "Connection",
        duck_conn: DuckDBPyConnection,
        info_schema_manager: InfoSchemaManager,
        use_dict_result: bool = False,
    ) -> None:
        self._sf_conn = sf_conn
        # The session's connection, not a fork of it. Snowflake scopes a
        # transaction to the session, so BEGIN on one cursor has to cover work
        # done through any other cursor of the same connection - a per-cursor
        # DuckDB connection gave each its own transaction and silently dropped
        # the rollback. Results are materialised eagerly (see _execute), so
        # sharing does not let one cursor invalidate another's rows.
        self._duck_cur = duck_conn
        self._info_schema_manager = info_schema_manager
        self._use_dict_result = use_dict_result
        self._is_closed = False
        self._last_table_name: str | None = None

        self._last_sql: str | None = None
        self._last_params: Sequence[Any] | dict[Any, Any] | None = None
        self._sqlstate: str | None = None
        self._arrow_table: pa.Table | None = None
        self._duck_description: list[Any] | None = None
        self._arrow_table_fetch_index: int = 0
        self._rowcount: int | None = None
        self._sfqid: str | None = None
        self._converter = snowflake.connector.converter.SnowflakeConverter()
        self.arraysize: int = 1

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def describe(self, command: str, *args: Any, **kwargs: Any) -> list[ResultMetadata]:
        """Return the schema of the result without executing the query.

        Takes the same arguments as execute

        Returns:
            list[ResultMetadata]: _description_
        """
        self.execute(f"DESCRIBE {command}", *args, **kwargs)
        return describe_as_result_metadata(cast(Any, self.fetchall()))

    @property
    def description(self) -> list[ResultMetadata]:
        with self._sf_conn.lock:
            return self._description_locked()

    def _description_locked(self) -> list[ResultMetadata]:
        table_name = self._infer_table_name()
        overrides = None
        if table_name:
            # Declared VARCHAR lengths live in the catalog, not in DuckDB's
            # type, so they have to be read back for `internal_size` to be the
            # size the column was created with rather than Snowflake's maximum.
            columns = self._sf_conn.get_column_metadata(table_name)
            if columns:
                overrides = {column["name"]: column for column in columns}
        return describe_as_result_metadata(
            self.describe_last_sql(),
            database=self._sf_conn.database,
            schema=self._sf_conn.schema,
            table=table_name,
            overrides=overrides,
        )

    def describe_last_sql(self) -> list[Any]:
        description = self._duck_description
        if not description:
            raise TypeError("No result set available to describe")
        nullability = self._infer_nullability()
        if nullability:
            patched: list[tuple[Any, ...]] = []
            for column in description:
                name, type_name, *rest = column
                null_ok = nullability.get(name)
                patched.append((name, type_name, None, None, None, null_ok))
            return cast(list[Any], convert_dbapi_description_to_describe(patched))

        return cast(list[Any], convert_dbapi_description_to_describe(description))

    def _infer_table_name(self) -> str | None:
        if not self._last_sql:
            return None
        try:
            expr = sqlglot.parse_one(self._last_sql, read="duckdb")
        except Exception:
            return None

        if not isinstance(expr, exp.Select):
            return None

        table = _single_source_table(expr)
        if table is None:
            return None
        return table.name if table.name else None

    def _infer_nullability(self) -> dict[str, bool]:
        if not self._last_sql:
            return {}
        try:
            expr = sqlglot.parse_one(self._last_sql, read="duckdb")
        except Exception:
            return {}

        if not isinstance(expr, exp.Select):
            return {}

        table = _single_source_table(expr)
        if table is None:
            return {}
        table_name = table.name
        if not table_name:
            return {}

        # An unqualified table lives in the session's schema, not DuckDB's
        # "main" - looking in the wrong place made every column report as
        # nullable.
        catalog = table.catalog or self._sf_conn.database
        schema = table.db or self._sf_conn.schema
        candidates = [
            ".".join(part for part in (catalog, schema, table_name) if part),
            ".".join(part for part in (schema, table_name) if part),
            table_name,
        ]

        rows: list[tuple[Any, ...]] = []
        for target in dict.fromkeys(candidates):
            try:
                pragma_cur = self._duck_cur.cursor()
                rows = pragma_cur.execute(f"PRAGMA table_info('{target}')").fetchall()
                pragma_cur.close()
            except Exception:
                continue
            if rows:
                break

        mapping: dict[str, bool] = {}
        for _cid, name, _type, notnull, _default, _pk in rows:
            mapping[name] = not notnull

        return mapping

    def _preprocess_json_extract_path_text(self, sql: str) -> str:
        """
        Pre-process JSON_EXTRACT_PATH_TEXT to convert multiple keys into a single JSONPath.

        Snowflake parser has a bug where it only includes the first key, so we need to
        transform the SQL string before parsing.

        Example:
            JSON_EXTRACT_PATH_TEXT(json, 'a', 'b', 'c')
            -> GET_PATH(json, 'a.b.c')
        """
        import re

        # Match JSON_EXTRACT_PATH_TEXT(arg1, 'key1', 'key2', ...)
        pattern = r"JSON_EXTRACT_PATH_TEXT\s*\(\s*([^,]+)\s*,\s*(.+?)\s*\)"

        def replace_func(match: re.Match[str]) -> str:
            json_arg = match.group(1)
            keys_str = match.group(2)

            # Extract individual keys (quoted strings)
            keys = re.findall(r"'([^']+)'", keys_str)

            if not keys:
                return match.group(0)  # No change if no keys found

            # Build the path
            path = ".".join(keys)

            # Use GET_PATH which will be properly handled by preprocessor
            return f"GET_PATH({json_arg}, '{path}')"

        return re.sub(pattern, replace_func, sql, flags=re.IGNORECASE | re.DOTALL)

    def execute(
        self,
        command: str,
        params: Sequence[Any] | dict[Any, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> Self:
        with self._sf_conn.lock:
            return self._execute_locked(command, params)

    def _execute_locked(
        self,
        command: str,
        params: Sequence[Any] | dict[Any, Any] | None = None,
    ) -> Self:
        try:
            self._sqlstate = None

            command, params = self._rewrite_with_params(command, params)

            # Pre-process JSON_EXTRACT_PATH_TEXT to build full JSONPath
            # Snowflake parser incorrectly only includes first key
            command = self._preprocess_json_extract_path_text(command)

            expressions = _parse_snowflake(command)

            if not expressions:
                return self

            if params is not None and len(expressions) > 1:
                self._sqlstate = "42601"
                raise snowflake.connector.errors.ProgrammingError(
                    msg="Multiple statements with parameters are not supported.",
                    errno=1003,
                    sqlstate=self._sqlstate,
                )

            for expression in expressions:
                if not isinstance(expression, exp.Expression):
                    # sqlglot.parse can yield None for an empty statement.
                    continue
                self._execute(expression, params)

            return self
        except snowflake.connector.errors.ProgrammingError as e:
            self._sqlstate = e.sqlstate
            raise e
        except sqlglot.errors.ParseError as e:
            self._sqlstate = "42000"
            msg = (
                str(e).replace("\x1b[4m", "").replace("\x1b[0m", "")
            )  # Remove ANSI formatting
            raise snowflake.connector.errors.ProgrammingError(
                msg=msg, errno=1003, sqlstate=self._sqlstate
            ) from None
        except (ValueError, NotImplementedError) as e:
            # Unsupported-syntax signals raised by the transforms. Surface them as
            # Snowflake errors so callers (and the server) see a clean failure.
            self._sqlstate = "0A000"
            raise snowflake.connector.errors.ProgrammingError(
                msg=str(e) or type(e).__name__, errno=2, sqlstate=self._sqlstate
            ) from None

    def _record_declared_lengths(self, create: exp.Expression, table: str) -> None:
        """Persist VARCHAR/CHAR sizes, which DuckDB discards at DDL time."""
        lengths: dict[str, int | None] = {}
        for column in create.find_all(exp.ColumnDef):
            length = _declared_length(column.kind)
            if length is not None:
                lengths[column.name.upper()] = length

        if not lengths:
            return

        database, schema = self._relation_target(create.this)
        if not database or not schema:
            return

        self._info_schema_manager.record_column_lengths(
            database=database, schema=schema, table=table, lengths=lengths
        )

    def _record_altered_lengths(self, alter: exp.Alter, table: str) -> None:
        """Keep declared VARCHAR/CHAR sizes in step with ALTER TABLE.

        A column added or retyped by ALTER carries a declared size exactly as
        one in a CREATE does, and one dropped or retyped away from VARCHAR has
        to forget the size it used to have - otherwise a stale length keeps
        being reported for a column that no longer has one.
        """
        lengths: dict[str, int | None] = {}
        for action in alter.args.get("actions") or []:
            if isinstance(action, exp.ColumnDef):
                lengths[action.name.upper()] = _declared_length(action.kind)
            elif isinstance(action, exp.AlterColumn):
                dtype = action.args.get("dtype")
                if dtype is not None:
                    lengths[action.name.upper()] = _declared_length(dtype)
            elif isinstance(action, exp.RenameColumn):
                # The old name's entry no longer matches any column; the new
                # name keeps DuckDB's (unsized) type either way.
                lengths[action.this.name.upper()] = None
            elif isinstance(action, exp.Drop) and _dropped_column_name(action):
                lengths[str(_dropped_column_name(action)).upper()] = None

        if not lengths:
            return

        database, schema = self._relation_target(alter.this)
        if not database or not schema:
            return

        self._info_schema_manager.record_column_lengths(
            database=database,
            schema=schema,
            table=table,
            lengths=lengths,
            replace=False,
        )

    def _relation_target(self, target: exp.Expression | None) -> tuple[str, str]:
        """The (database, schema) a DDL target names, defaulted to the session."""
        if isinstance(target, exp.Schema):
            target = target.this
        database = self._sf_conn.database or ""
        schema = self._sf_conn.schema or ""
        if isinstance(target, exp.Table):
            catalog = target.args.get("catalog")
            if isinstance(catalog, exp.Identifier):
                database = catalog.name
            qualifier = target.args.get("db")
            if isinstance(qualifier, exp.Identifier):
                schema = qualifier.name
        return database, schema

    def _schema_target(
        self, expression: sqlglot.exp.Expression, fallback: str
    ) -> tuple[str | None, str]:
        """The (database, schema) a CREATE/DROP SCHEMA statement names.

        For a schema target sqlglot puts the schema in `db` and the database in
        `catalog`, leaving `this` empty - the opposite nesting to a table.

        Snowflake folds unquoted identifiers to upper case, so the schema is
        recorded the way a later reference will spell it.
        """

        def name_of(node: object) -> str | None:
            if isinstance(node, sqlglot.exp.Identifier) and node.this:
                return str(node.this) if node.quoted else str(node.this).upper()
            return None

        table = expression.find(sqlglot.exp.Table)
        database = self._sf_conn.database
        schema = fallback

        if table is not None:
            schema = name_of(table.args.get("db")) or schema
            database = name_of(table.args.get("catalog")) or database

        return database, schema

    def _generate_result(self, template: Template | str, **kwargs: Any) -> None:
        """
        Generates and executes a fake result set based on the provided template and parameters.

        Args:
            template (Template): The SQL template to use for the result.
            **kwargs: Parameters to substitute into the template.

        Returns:
            None
        """
        if isinstance(template, Template):
            result_sql = template.substitute(**kwargs)
        else:
            result_sql = template

        self._duck_cur.execute(result_sql)
        self._last_sql = result_sql
        self._last_params = None

    def _execute(
        self,
        transformed: sqlglot.exp.Expression,
        params: Sequence[Any] | dict[Any, Any] | None = None,
    ) -> None:
        self._arrow_table = None
        self._arrow_table_fetch_index = 0
        self._rowcount = None
        self._sfqid = None
        self._last_table_name = None

        # Ensure DuckDB cursor uses the correct database/schema context
        # DuckDB cursors don't inherit connection-level SET schema settings
        if self._sf_conn.database and self._sf_conn.schema:
            try:
                self._duck_cur.execute(
                    f"SET schema='{self._sf_conn.database}.{self._sf_conn.schema}'"
                )
            except Exception:
                pass  # Schema may not exist yet

        cmd = extract_sql_command(transformed)

        dialect = Dialect(
            context=DialectContext(
                current_database=self._sf_conn.database,
                current_schema=self._sf_conn.schema,
                current_role=self._sf_conn.role,
                current_warehouse=self._sf_conn.warehouse,
                info_schema_manager=self._info_schema_manager,
                session_variables=self._sf_conn._session_variables,
                last_query_id=self._sf_conn.last_query_id,
                session_parameters=self._sf_conn.session_parameters,
            )
        )

        replace_drop_sql: str | None = None
        if isinstance(transformed, exp.Insert) and transformed.args.get("overwrite"):
            # Snowflake's INSERT OVERWRITE INTO empties the table and inserts in
            # one statement; dbt-snowflake's seed materialization relies on it.
            # sqlglot's DuckDB generator turns it into Hive's
            # `INSERT OVERWRITE TABLE`, which DuckDB cannot parse, so the two
            # halves are issued separately instead.
            target = transformed.this
            if isinstance(target, exp.Schema):
                target = target.this
            if isinstance(target, exp.Table):
                replace_drop_sql = f"DELETE FROM {target.sql(dialect=dialect)}"
                insert_expr = transformed.copy()
                insert_expr.set("overwrite", False)
                sql = insert_expr.sql(dialect=dialect)
            else:
                sql = Dialect.sql_with_cache(transformed, dialect)
        elif isinstance(transformed, exp.Create) and transformed.args.get("replace"):
            kind = transformed.args.get("kind")
            if isinstance(kind, str) and kind.upper() == "TABLE":
                table_expr = transformed.this
                if isinstance(table_expr, exp.Table):
                    replace_drop_sql = (
                        f"DROP TABLE IF EXISTS {table_expr.sql(dialect=dialect)}"
                    )
                    create_expr = transformed.copy()
                    create_expr.set("replace", False)
                    sql = create_expr.sql(dialect=dialect)
                else:
                    sql = Dialect.sql_with_cache(transformed, dialect)
            else:
                sql = Dialect.sql_with_cache(transformed, dialect)
        else:
            sql = Dialect.sql_with_cache(transformed, dialect)

        if not sql:
            raise NotImplementedError(transformed.sql(dialect="snowflake"))

        if cmd in {
            "ALTER SESSION",
            "PUT",
            "SET",
            "USE ROLE",
            "USE WAREHOUSE",
            "USE DATABASE",
            "USE SCHEMA",
        }:
            if cmd == "SET":
                # SET is handled by transform_set which stores in context
                # sql is already a SELECT statement, execute it
                pass
            elif cmd == "ALTER SESSION":
                self._apply_alter_session(transformed)
            elif cmd == "PUT":
                put_sql = transformed.sql(dialect="snowflake")
                match = re.search(r"PUT\s+(\S+)\s+@(\S+)", put_sql, re.IGNORECASE)
                if match:
                    source = match.group(1)
                    stage = match.group(2)
                    if source.startswith("file://"):
                        source = source[len("file://") :]
                    stage_root = os.getenv("SNOWDUCK_STAGE_DIR", "/tmp/snowduck_stage")
                    dest_dir = os.path.join(stage_root, stage)
                    os.makedirs(dest_dir, exist_ok=True)
                    if os.path.exists(source):
                        dest = os.path.join(dest_dir, os.path.basename(source))
                        shutil.copyfile(source, dest)
            elif cmd in {"USE ROLE", "USE WAREHOUSE", "USE DATABASE", "USE SCHEMA"}:
                ident = None
                db_name = None
                schema_name = None
                quoted = False
                if isinstance(transformed, exp.Use):
                    target = transformed.this
                    if isinstance(target, exp.Table):
                        schema_name = target.name
                        if isinstance(target.db, exp.Identifier):
                            db_name = target.db.name
                    elif isinstance(target, exp.Identifier):
                        ident = target.this
                        quoted = target.quoted
                if (
                    ident is None
                    and (eid := transformed.find(sqlglot.exp.Identifier, bfs=False))
                    and isinstance(eid.this, str)
                ):
                    ident = eid.this
                    quoted = eid.quoted
                if ident is not None and not quoted:
                    ident = ident.upper()
                if db_name:
                    db_name = db_name.upper()
                if schema_name:
                    schema_name = schema_name.upper()
                if cmd == "USE ROLE" and ident:
                    self._sf_conn.use_role(ident)
                elif cmd == "USE WAREHOUSE" and ident:
                    self._sf_conn.use_warehouse(ident)
                elif cmd == "USE DATABASE" and ident:
                    self._sf_conn.use_database(ident)
                elif cmd == "USE SCHEMA":
                    if db_name:
                        self._sf_conn.use_database(db_name)
                    if schema_name:
                        self._sf_conn.use_schema(schema_name)

            self._generate_result(SQL_SUCCESS)
            self._duck_description = self._duck_cur.description
            self._arrow_table = self._duck_cur.fetch_arrow_table()
            self._rowcount = self._arrow_table.num_rows
            self._sfqid = str(uuid.uuid4())
            self._sf_conn.last_query_id = self._sfqid
            return

        if isinstance(transformed, exp.Select):
            table_expr = transformed.find(exp.Table)
            if table_expr is not None:
                self._last_table_name = table_expr.name

        try:
            if replace_drop_sql:
                self._duck_cur.execute(replace_drop_sql)
            if cmd != "PUT":
                self._duck_cur.execute(sql, params)
            self._last_sql = sql
            self._last_params = params
        except duckdb.BinderException as e:
            msg = e.args[0]
            raise snowflake.connector.errors.ProgrammingError(
                msg=msg, errno=2043, sqlstate="02000"
            ) from None
        except duckdb.CatalogException as e:
            msg = cast(str, e.args[0]).split("\n")[0]
            raise snowflake.connector.errors.ProgrammingError(
                msg=msg, errno=2003, sqlstate="42S02"
            ) from None
        except duckdb.TransactionException as e:
            if "cannot rollback - no transaction is active" in str(
                e
            ) or "cannot commit - no transaction is active" in str(e):
                # Snowflake allows rollback or commit even when no transaction is active
                self._generate_result(SQL_SUCCESS)
            else:
                raise snowflake.connector.errors.ProgrammingError(
                    msg=cast(str, e.args[0]).split("\n")[0],
                    errno=1003,
                    sqlstate="25000",
                ) from None
        except duckdb.ConnectionException as e:
            raise snowflake.connector.errors.DatabaseError(
                msg=e.args[0], errno=250002, sqlstate="08003"
            ) from None
        except duckdb.ParserException as e:
            raise snowflake.connector.errors.ProgrammingError(
                msg=e.args[0], errno=1003, sqlstate="42000"
            ) from None
        except duckdb.Error as e:
            # Catch-all for every other DuckDB failure (ConversionException,
            # InvalidInputException, OutOfRangeException, ConstraintException, ...).
            # A raw DuckDB exception escaping here becomes an HTTP 500 in the server,
            # and the Snowflake connector treats 5xx as retryable - so the statement
            # is silently re-executed until the network timeout instead of failing.
            msg = cast(str, e.args[0]).split("\n")[0] if e.args else str(e)
            raise snowflake.connector.errors.ProgrammingError(
                msg=msg, errno=1003, sqlstate="42000"
            ) from None

        if isinstance(transformed, exp.Alter) and isinstance(
            transformed.this, exp.Table
        ):
            self._record_altered_lengths(transformed, transformed.this.name.upper())

        if cmd == "DROP SCHEMA":
            database, schema = self._schema_target(transformed, "")
            if database and schema:
                self._info_schema_manager.unregister_schema(
                    database=database, schema=schema
                )
                if schema == "MAIN":
                    # DuckDB's internal `main` survives the DROP, so its
                    # contents have to go explicitly for the schema to look
                    # empty afterwards.
                    self._info_schema_manager.clear_schema(
                        database=database, schema="main"
                    )

        affected_count = None

        # Generate results for specific commands
        if cmd == "INSERT":
            (affected_count,) = self._duck_cur.fetchall()[0]
            self._generate_result(SQL_INSERTED_ROWS, count=affected_count)
        elif cmd == "UPDATE":
            (affected_count,) = self._duck_cur.fetchall()[0]
            self._generate_result(SQL_UPDATED_ROWS, count=affected_count)
        elif cmd == "DELETE":
            (affected_count,) = self._duck_cur.fetchall()[0]
            self._generate_result(SQL_DELETED_ROWS, count=affected_count)
        elif cmd == "TRUNCATETABLE":
            self._generate_result(SQL_SUCCESS)
        elif (
            eid := transformed.find(sqlglot.exp.Identifier, bfs=False)
        ) and isinstance(eid.this, str):
            ident = eid.this if eid.quoted else eid.this.upper()
            if cmd == "CREATE DATABASE":
                self._info_schema_manager.create_database_information_schema(
                    database=ident
                )
                self._generate_result(SQL_CREATED_DATABASE, name=ident)
            elif cmd == "CREATE SCHEMA":
                database, schema = self._schema_target(transformed, ident)
                if database:
                    # MAIN is made idempotent during translation because DuckDB
                    # owns a schema of that name; the duplicate check Snowflake
                    # would have done therefore happens here.
                    if (
                        schema == "MAIN"
                        and not transformed.args.get("exists")
                        and self._info_schema_manager.has_registered_schema(
                            database=database, schema=schema
                        )
                    ):
                        raise snowflake.connector.errors.ProgrammingError(
                            msg=f"Schema '{database}.{schema}' already exists.",
                            errno=2002,
                            sqlstate="42710",
                        )
                    self._info_schema_manager.register_schema(
                        database=database, schema=schema
                    )
                self._generate_result(SQL_CREATED_SCHEMA, name=schema)
            elif cmd == "CREATE VIEW":
                self._generate_result(SQL_CREATED_VIEW, name=ident)
            elif cmd == "CREATE TABLE":
                self._record_declared_lengths(transformed, ident)
                self._generate_result(SQL_CREATED_TABLE, name=ident)
            elif cmd == "CREATE FUNCTION":
                self._generate_result(SQL_CREATED_FUNCTION, name=ident)
            elif cmd == "USE DATABASE":
                self._sf_conn.use_database(ident)
                self._generate_result(SQL_SUCCESS)
            elif cmd == "USE SCHEMA":
                self._sf_conn.use_schema(ident)
                self._generate_result(SQL_SUCCESS)

        self._duck_description = self._duck_cur.description
        self._arrow_table = self._duck_cur.fetch_arrow_table()
        # Fallback to num_rows ONLY if affected_count is explicitly None (it will be None for SELECTs, but 0 for DML)
        self._rowcount = (
            affected_count if affected_count is not None else self._arrow_table.num_rows
        )
        self._sfqid = str(uuid.uuid4())
        self._sf_conn.last_query_id = self._sfqid

    def _apply_alter_session(self, expression: exp.Expression) -> None:
        """Record ALTER SESSION SET/UNSET so SHOW PARAMETERS can read it back.

        dbt reads `query_tag` with SHOW PARAMETERS, overwrites it for the
        duration of a materialization and restores it afterwards; a no-op
        ALTER SESSION made it restore the wrong value.
        """
        for action in expression.args.get("actions") or []:
            if not isinstance(action, exp.AlterSession):
                continue
            unset = bool(action.args.get("unset"))
            for item in action.expressions:
                target = item.this if isinstance(item, exp.SetItem) else item
                if unset:
                    name = _identifier_name(target)
                    if name:
                        self._sf_conn.unset_session_parameter(name)
                elif isinstance(target, exp.EQ):
                    name = _identifier_name(target.this)
                    if not name:
                        continue
                    value = target.expression
                    if isinstance(value, exp.Literal):
                        self._sf_conn.set_session_parameter(name, str(value.this))
                    elif isinstance(value, exp.Boolean):
                        self._sf_conn.set_session_parameter(
                            name, "true" if value.this else "false"
                        )
                    else:
                        self._sf_conn.set_session_parameter(
                            name, value.sql(dialect="snowflake")
                        )

    def _rewrite_with_params(
        self,
        command: str,
        params: Sequence[Any] | dict[Any, Any] | None = None,
    ) -> tuple[str, Sequence[Any] | dict[Any, Any] | None]:
        if not params:
            return command, params

        # Check if using qmark style (?) - DuckDB supports this natively
        # This is what Snowflake SQL REST API uses
        if "?" in command and not isinstance(params, dict):
            # Pass through to DuckDB which natively supports ? placeholders
            return command, params

        # For pyformat/format style (%s), we need to do Python string formatting
        # because DuckDB doesn't support %s syntax
        if self._sf_conn.paramstyle in ("pyformat", "format"):

            def convert(param: Any) -> Any:
                # Snowflake returns float for Python float parameters (REAL/FLOAT type)
                # DuckDB treats numeric literals as DECIMAL by default
                # Wrap floats with CAST(...  AS DOUBLE) to match Snowflake behavior
                if isinstance(param, float):
                    return f"CAST({param} AS DOUBLE)"
                return self._converter.quote(
                    self._converter.escape(self._converter.to_snowflake(param))
                )

            if isinstance(params, dict):
                params = {k: convert(v) for k, v in params.items()}
            else:
                params = tuple(convert(v) for v in params)

            return command % params, None

        return command, params

    def fetchone(self) -> dict[str, Any] | tuple[Any, ...] | None:
        result = self.fetchmany(1)
        return result[0] if result else None

    def fetchmany(
        self, size: int | None = None
    ) -> list[tuple[Any, ...]] | list[dict[str, Any]]:
        if self._arrow_table is None:
            # Raise an error if no result set is open
            # This is consistent with the behavior of the Snowflake cursor
            # when calling fetchmany without calling execute first
            raise TypeError("No open result set")
        if size is None:
            size = self.arraysize

        start_index = self._arrow_table_fetch_index
        tslice = self._arrow_table.slice(offset=start_index, length=size).to_pylist()
        self._arrow_table_fetch_index += len(tslice)

        # Return as a list of dictionaries or tuples based on _use_dict_result
        if self._use_dict_result:
            return tslice
        else:
            return [tuple(d.values()) for d in tslice]  # Convert dictionaries to tuples

    def fetchall(self) -> list[tuple[Any, ...]] | list[dict[str, Any]]:
        if self._arrow_table is None:
            raise TypeError("No open result set")
        # Fetch everything remaining from the current index
        return self.fetchmany(
            self._arrow_table.num_rows - self._arrow_table_fetch_index
        )

    def fetch_pandas_all(self, **kwargs: Any) -> "pd.DataFrame":
        """
        Fetch all rows as a pandas DataFrame.

        This matches Snowflake's cursor.fetch_pandas_all() method.

        Returns:
            pandas.DataFrame: All remaining rows as a DataFrame.
        """

        if self._arrow_table is None:
            raise TypeError("No open result set")

        # Return remaining rows from current index
        remaining = self._arrow_table.slice(
            offset=self._arrow_table_fetch_index,
            length=self._arrow_table.num_rows - self._arrow_table_fetch_index,
        )
        self._arrow_table_fetch_index = self._arrow_table.num_rows
        return remaining.to_pandas()

    def fetch_pandas_batches(self, **kwargs: Any) -> "Iterator[pd.DataFrame]":
        """
        Fetch results as an iterator of pandas DataFrames.

        This matches Snowflake's cursor.fetch_pandas_batches() method.
        Yields batches of rows as DataFrames.

        Yields:
            pandas.DataFrame: Batches of rows as DataFrames.
        """

        if self._arrow_table is None:
            raise TypeError("No open result set")

        batch_size = kwargs.get("batch_size", 10000)

        while self._arrow_table_fetch_index < self._arrow_table.num_rows:
            remaining = self._arrow_table.num_rows - self._arrow_table_fetch_index
            size = min(batch_size, remaining)

            batch = self._arrow_table.slice(
                offset=self._arrow_table_fetch_index, length=size
            )
            self._arrow_table_fetch_index += size
            yield batch.to_pandas()

    def get_result_batches(self) -> list["pa.RecordBatch"]:
        """
        Get all Arrow result batches from the current result set.

        Returns:
            list[pyarrow.RecordBatch]: List of Arrow record batches.
        """
        if self._arrow_table is None:
            raise TypeError("No open result set")
        return self._arrow_table.to_batches()

    def is_closed(self) -> bool:
        return self._is_closed

    def close(self) -> bool | None:
        try:
            if self.is_closed():
                return False
            self._last_sql = None
            self._last_params = None
            self._is_closed = True
            # The DuckDB connection belongs to the session, not to this cursor.
            self._arrow_table = None
            self._duck_description = None
            return True
        except Exception:
            return None

    @property
    def rowcount(self) -> int | None:
        return self._rowcount

    @property
    def sfqid(self) -> str | None:
        return self._sfqid

    @property
    def sqlstate(self) -> str | None:
        return self._sqlstate

    @property
    def last_table_name(self) -> str | None:
        return self._last_table_name
