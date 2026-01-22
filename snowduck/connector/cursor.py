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
SQL_DROPPED = Template("SELECT '${name} successfully dropped.' as 'status'")
SQL_INSERTED_ROWS = Template("SELECT ${count} as 'number of rows inserted'")
SQL_UPDATED_ROWS = Template(
    "SELECT ${count} as 'number of rows updated', 0 as 'number of multi-joined rows updated'"
)
SQL_DELETED_ROWS = Template("SELECT ${count} as 'number of rows deleted'")


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


class Cursor:
    def __init__(
        self,
        sf_conn: "Connection",
        duck_conn: DuckDBPyConnection,
        info_schema_manager: InfoSchemaManager,
        use_dict_result: bool = False,
    ) -> None:
        self._sf_conn = sf_conn
        self._duck_cur = duck_conn.cursor()
        self._info_schema_manager = info_schema_manager
        self._use_dict_result = use_dict_result
        self._is_closed = False
        self._last_table_name: str | None = None

        self._last_sql: str | None = None
        self._last_params: Sequence[Any] | dict[Any, Any] | None = None
        self._sqlstate: str | None = None
        self._arrow_table: pa.Table | None = None
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
        return describe_as_result_metadata(self.fetchall())

    @property
    def description(self) -> list[ResultMetadata]:
        table_name = self._infer_table_name()
        return describe_as_result_metadata(
            self._describe_last_sql(),
            database=self._sf_conn.database,
            schema=self._sf_conn.schema,
            table=table_name,
        )

    def describe_last_sql(self) -> list:
        if not self._duck_cur.description:
            raise TypeError("No result set available to describe")
        nullability = self._infer_nullability()
        if nullability:
            patched = []
            for column in self._duck_cur.description:
                name, type_name, *rest = column
                null_ok = nullability.get(name)
                patched.append((name, type_name, None, None, None, null_ok))
            return convert_dbapi_description_to_describe(patched)

        return convert_dbapi_description_to_describe(self._duck_cur.description)

    def _infer_table_name(self) -> str | None:
        if not self._last_sql:
            return None
        try:
            expr = sqlglot.parse_one(self._last_sql, read="duckdb")
        except Exception:
            return None

        if not isinstance(expr, exp.Select):
            return None

        from_expr = expr.args.get("from")
        if not from_expr or not isinstance(from_expr.this, exp.Table):
            return None

        table = from_expr.this
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

        from_expr = expr.args.get("from")
        if not from_expr or not isinstance(from_expr.this, exp.Table):
            return {}

        table = from_expr.this
        table_name = table.name
        if not table_name:
            return {}

        schema = table.db or "main"
        pragma_target = f"{schema}.{table_name}"

        try:
            pragma_cur = self._duck_cur.connection.cursor()
            rows = pragma_cur.execute(
                f"PRAGMA table_info('{pragma_target}')"
            ).fetchall()
            pragma_cur.close()
        except Exception:
            return {}

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
        
        def replace_func(match):
            json_arg = match.group(1)
            keys_str = match.group(2)
            
            # Extract individual keys (quoted strings)
            keys = re.findall(r"'([^']+)'", keys_str)
            
            if not keys:
                return match.group(0)  # No change if no keys found
            
            # Build the path
            path = '.'.join(keys)
            
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
        try:
            self._sqlstate = None

            command, params = self._rewrite_with_params(command, params)

            # Pre-process JSON_EXTRACT_PATH_TEXT to build full JSONPath
            # Snowflake parser incorrectly only includes first key
            command = self._preprocess_json_extract_path_text(command)

            expressions = sqlglot.parse(command, read="snowflake")

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

    def _generate_result(self, template: Template | str, **kwargs) -> None:
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
    ):
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
            )
        )

        replace_drop_sql: str | None = None
        if isinstance(transformed, exp.Create) and transformed.args.get("replace"):
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
            self._arrow_table = self._duck_cur.fetch_arrow_table()
            self._rowcount = self._arrow_table.num_rows
            self._sfqid = str(uuid.uuid4())
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
                raise e
        except duckdb.ConnectionException as e:
            raise snowflake.connector.errors.DatabaseError(
                msg=e.args[0], errno=250002, sqlstate="08003"
            ) from None
        except duckdb.ParserException as e:
            raise snowflake.connector.errors.ProgrammingError(
                msg=e.args[0], errno=1003, sqlstate="42000"
            ) from None

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
                self._generate_result(SQL_CREATED_SCHEMA, name=ident)
            elif cmd == "CREATE VIEW":
                self._generate_result(SQL_CREATED_VIEW, name=ident)
            elif cmd == "CREATE TABLE":
                self._generate_result(SQL_CREATED_TABLE, name=ident)
            elif cmd == "USE DATABASE":
                self._sf_conn.use_database(ident)
                self._generate_result(SQL_SUCCESS)
            elif cmd == "USE SCHEMA":
                self._sf_conn.use_schema(ident)
                self._generate_result(SQL_SUCCESS)

        self._arrow_table = self._duck_cur.fetch_arrow_table()
        # Fallback to num_rows ONLY if affected_count is explicitly None (it will be None for SELECTs, but 0 for DML)
        self._rowcount = (
            affected_count if affected_count is not None else self._arrow_table.num_rows
        )
        self._sfqid = str(uuid.uuid4())

    def _rewrite_with_params(
        self,
        command: str,
        params: Sequence[Any] | dict[Any, Any] | None = None,
    ) -> tuple[str, Sequence[Any] | dict[Any, Any] | None]:
        if params and self._sf_conn.paramstyle in ("pyformat", "format"):

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

    def fetchone(self) -> dict | tuple | None:
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

    def fetchall(self) -> list[tuple] | list[dict]:
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
            length=self._arrow_table.num_rows - self._arrow_table_fetch_index
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
                offset=self._arrow_table_fetch_index,
                length=size
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
            self._duck_cur.close()
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
