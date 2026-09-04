"""Rendering of ``SHOW`` requests as DuckDB queries.

Every branch here aims at Snowflake's documented *column shape* for that object
type, not just the columns SnowDuck happens to be able to fill. Clients index
SHOW output positionally or by name - dbt-snowflake selects
``database_name, schema_name, name, kind, is_dynamic, is_iceberg`` out of SHOW
OBJECTS and ``catalog_name, schema_name, name, is_builtin`` out of SHOW USER
FUNCTIONS - so a missing column is a hard failure at the client, not a
degraded result. Object types SnowDuck cannot host locally (dynamic tables,
streams, tasks, ...) therefore return an *empty result with the right columns*
rather than an error.

Identifier matching is case-insensitive. Snowflake gets the same effect by
folding unquoted identifiers to upper case at DDL time; DuckDB stores them as
written, so the comparison is folded instead.
"""

from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING, Sequence

from ..helper import load_sql, sql_literal
from .parser import ResolvedShowRequest

if TYPE_CHECKING:
    from ..dialect.context import DialectContext

_TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

# DuckDB catalogs that are not Snowflake databases.
_INTERNAL_DATABASES = ("memory", "system", "temp")


class UnsupportedShowError(ValueError):
    """A ``SHOW`` variant SnowDuck does not emulate."""


# --- Snowflake session parameters -----------------------------------------
# key -> (default value, type, description). Only the parameters a client is
# likely to read back are listed; SHOW PARAMETERS reports session overrides on
# top of these.
SESSION_PARAMETER_DEFAULTS: dict[str, tuple[str, str, str]] = {
    "ABORT_DETACHED_QUERY": ("false", "BOOLEAN", "Aborts a detached query."),
    "AUTOCOMMIT": ("true", "BOOLEAN", "The autocommit property."),
    "BINARY_INPUT_FORMAT": ("HEX", "STRING", "Input format for binary values."),
    "BINARY_OUTPUT_FORMAT": ("HEX", "STRING", "Output format for binary values."),
    "CLIENT_TIMESTAMP_TYPE_MAPPING": (
        "TIMESTAMP_LTZ",
        "STRING",
        "Timestamp type the client maps TIMESTAMP to.",
    ),
    "DATE_INPUT_FORMAT": ("AUTO", "STRING", "Input format for date values."),
    "DATE_OUTPUT_FORMAT": ("YYYY-MM-DD", "STRING", "Output format for date values."),
    "ERROR_ON_NONDETERMINISTIC_MERGE": (
        "true",
        "BOOLEAN",
        "Raise an error on a nondeterministic MERGE.",
    ),
    "ERROR_ON_NONDETERMINISTIC_UPDATE": (
        "false",
        "BOOLEAN",
        "Raise an error on a nondeterministic UPDATE.",
    ),
    "JSON_INDENT": ("2", "NUMBER", "Number of blanks used to indent JSON output."),
    "LOCK_TIMEOUT": ("43200", "NUMBER", "Seconds to wait while trying to lock."),
    "MULTI_STATEMENT_COUNT": ("1", "NUMBER", "Number of statements to execute."),
    "QUERY_TAG": ("", "STRING", "String tag attached to statements in this session."),
    "QUOTED_IDENTIFIERS_IGNORE_CASE": (
        "false",
        "BOOLEAN",
        "Treat double-quoted identifiers as case-insensitive.",
    ),
    "ROWS_PER_RESULTSET": ("0", "NUMBER", "Maximum rows returned in a result set."),
    "STATEMENT_TIMEOUT_IN_SECONDS": (
        "172800",
        "NUMBER",
        "Seconds a statement may run before being cancelled.",
    ),
    "TIMESTAMP_INPUT_FORMAT": ("AUTO", "STRING", "Input format for timestamps."),
    "TIMESTAMP_LTZ_OUTPUT_FORMAT": (
        "",
        "STRING",
        "Output format for TIMESTAMP_LTZ values.",
    ),
    "TIMESTAMP_NTZ_OUTPUT_FORMAT": (
        "YYYY-MM-DD HH24:MI:SS.FF3",
        "STRING",
        "Output format for TIMESTAMP_NTZ values.",
    ),
    "TIMESTAMP_OUTPUT_FORMAT": (
        "YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM",
        "STRING",
        "Output format for timestamps.",
    ),
    "TIMESTAMP_TYPE_MAPPING": (
        "TIMESTAMP_NTZ",
        "STRING",
        "Timestamp type TIMESTAMP maps to.",
    ),
    "TIMESTAMP_TZ_OUTPUT_FORMAT": (
        "",
        "STRING",
        "Output format for TIMESTAMP_TZ values.",
    ),
    "TIMEZONE": ("America/Los_Angeles", "STRING", "Time zone for the session."),
    "TIME_INPUT_FORMAT": ("AUTO", "STRING", "Input format for time values."),
    "TIME_OUTPUT_FORMAT": ("HH24:MI:SS", "STRING", "Output format for time values."),
    "TRANSACTION_DEFAULT_ISOLATION_LEVEL": (
        "READ COMMITTED",
        "STRING",
        "Isolation level for transactions.",
    ),
    "TWO_DIGIT_CENTURY_START": ("1970", "NUMBER", "Century start for 2-digit years."),
    "UNSUPPORTED_DDL_ACTION": ("ignore", "STRING", "Action for unsupported DDL."),
    "USE_CACHED_RESULT": ("true", "BOOLEAN", "Reuse persisted query results."),
    "WEEK_OF_YEAR_POLICY": ("0", "NUMBER", "Policy for the first week of the year."),
    "WEEK_START": ("0", "NUMBER", "First day of the week."),
}

# Object types SnowDuck has no local equivalent for. Snowflake's column shape is
# still reported so a client that indexes into the result fails on the missing
# *object*, not on a missing column.
_EMPTY_RESULT_SHAPES: dict[str, tuple[tuple[str, str], ...]] = {
    "DYNAMIC TABLES": (
        ("created_on", "TIMESTAMPTZ"),
        ("name", "VARCHAR"),
        ("reserved", "VARCHAR"),
        ("database_name", "VARCHAR"),
        ("schema_name", "VARCHAR"),
        ("cluster_by", "VARCHAR"),
        ("rows", "BIGINT"),
        ("bytes", "BIGINT"),
        ("owner", "VARCHAR"),
        ("target_lag", "VARCHAR"),
        ("refresh_mode", "VARCHAR"),
        ("refresh_mode_reason", "VARCHAR"),
        ("warehouse", "VARCHAR"),
        ("comment", "VARCHAR"),
        ("text", "VARCHAR"),
        ("automatic_clustering", "VARCHAR"),
        ("scheduling_state", "VARCHAR"),
        ("last_suspended_on", "TIMESTAMPTZ"),
        ("is_clone", "VARCHAR"),
        ("is_replica", "VARCHAR"),
        ("data_timestamp", "TIMESTAMPTZ"),
        ("owner_role_type", "VARCHAR"),
        ("budget", "VARCHAR"),
    ),
    "ICEBERG TABLES": (
        ("created_on", "TIMESTAMPTZ"),
        ("name", "VARCHAR"),
        ("database_name", "VARCHAR"),
        ("schema_name", "VARCHAR"),
        ("comment", "VARCHAR"),
        ("owner", "VARCHAR"),
        ("owner_role_type", "VARCHAR"),
        ("external_volume_name", "VARCHAR"),
        ("catalog_name", "VARCHAR"),
        ("iceberg_table_type", "VARCHAR"),
    ),
    "EXTERNAL TABLES": (
        ("created_on", "TIMESTAMPTZ"),
        ("name", "VARCHAR"),
        ("database_name", "VARCHAR"),
        ("schema_name", "VARCHAR"),
        ("invalid", "VARCHAR"),
        ("invalid_reason", "VARCHAR"),
        ("owner", "VARCHAR"),
        ("comment", "VARCHAR"),
        ("stage", "VARCHAR"),
        ("location", "VARCHAR"),
        ("file_format_name", "VARCHAR"),
        ("file_format_type", "VARCHAR"),
        ("owner_role_type", "VARCHAR"),
    ),
    "PROCEDURES": (
        ("created_on", "TIMESTAMPTZ"),
        ("name", "VARCHAR"),
        ("schema_name", "VARCHAR"),
        ("is_builtin", "VARCHAR"),
        ("is_aggregate", "VARCHAR"),
        ("is_ansi", "VARCHAR"),
        ("min_num_arguments", "BIGINT"),
        ("max_num_arguments", "BIGINT"),
        ("arguments", "VARCHAR"),
        ("description", "VARCHAR"),
        ("catalog_name", "VARCHAR"),
        ("is_table_function", "VARCHAR"),
        ("valid_for_clustering", "VARCHAR"),
        ("is_secure", "VARCHAR"),
    ),
    "EXTERNAL FUNCTIONS": (
        ("created_on", "TIMESTAMPTZ"),
        ("name", "VARCHAR"),
        ("schema_name", "VARCHAR"),
        ("is_builtin", "VARCHAR"),
        ("is_aggregate", "VARCHAR"),
        ("is_ansi", "VARCHAR"),
        ("min_num_arguments", "BIGINT"),
        ("max_num_arguments", "BIGINT"),
        ("arguments", "VARCHAR"),
        ("description", "VARCHAR"),
        ("catalog_name", "VARCHAR"),
        ("is_table_function", "VARCHAR"),
        ("valid_for_clustering", "VARCHAR"),
        ("is_secure", "VARCHAR"),
        ("language", "VARCHAR"),
    ),
    "STREAMS": (
        ("created_on", "TIMESTAMPTZ"),
        ("name", "VARCHAR"),
        ("database_name", "VARCHAR"),
        ("schema_name", "VARCHAR"),
        ("owner", "VARCHAR"),
        ("comment", "VARCHAR"),
        ("table_name", "VARCHAR"),
        ("source_type", "VARCHAR"),
        ("base_tables", "VARCHAR"),
        ("type", "VARCHAR"),
        ("stale", "VARCHAR"),
        ("mode", "VARCHAR"),
        ("stale_after", "TIMESTAMPTZ"),
        ("invalid_reason", "VARCHAR"),
        ("owner_role_type", "VARCHAR"),
    ),
    "TASKS": (
        ("created_on", "TIMESTAMPTZ"),
        ("name", "VARCHAR"),
        ("id", "VARCHAR"),
        ("database_name", "VARCHAR"),
        ("schema_name", "VARCHAR"),
        ("owner", "VARCHAR"),
        ("comment", "VARCHAR"),
        ("warehouse", "VARCHAR"),
        ("schedule", "VARCHAR"),
        ("predecessors", "VARCHAR"),
        ("state", "VARCHAR"),
        ("definition", "VARCHAR"),
        ("condition", "VARCHAR"),
        ("allow_overlapping_execution", "VARCHAR"),
        ("error_integration", "VARCHAR"),
        ("last_committed_on", "TIMESTAMPTZ"),
        ("last_suspended_on", "TIMESTAMPTZ"),
        ("owner_role_type", "VARCHAR"),
        ("config", "VARCHAR"),
        ("budget", "VARCHAR"),
    ),
    "FILE FORMATS": (
        ("created_on", "TIMESTAMPTZ"),
        ("name", "VARCHAR"),
        ("database_name", "VARCHAR"),
        ("schema_name", "VARCHAR"),
        ("type", "VARCHAR"),
        ("owner", "VARCHAR"),
        ("comment", "VARCHAR"),
        ("format_options", "VARCHAR"),
        ("owner_role_type", "VARCHAR"),
    ),
    "PIPES": (
        ("created_on", "TIMESTAMPTZ"),
        ("name", "VARCHAR"),
        ("database_name", "VARCHAR"),
        ("schema_name", "VARCHAR"),
        ("definition", "VARCHAR"),
        ("owner", "VARCHAR"),
        ("notification_channel", "VARCHAR"),
        ("comment", "VARCHAR"),
        ("integration", "VARCHAR"),
        ("pattern", "VARCHAR"),
        ("error_integration", "VARCHAR"),
        ("owner_role_type", "VARCHAR"),
        ("invalid_reason", "VARCHAR"),
    ),
    "GRANTS": (
        ("created_on", "TIMESTAMPTZ"),
        ("privilege", "VARCHAR"),
        ("granted_on", "VARCHAR"),
        ("name", "VARCHAR"),
        ("granted_to", "VARCHAR"),
        ("grantee_name", "VARCHAR"),
        ("grant_option", "VARCHAR"),
        ("granted_by", "VARCHAR"),
    ),
    "PRIMARY KEYS": (
        ("created_on", "TIMESTAMPTZ"),
        ("database_name", "VARCHAR"),
        ("schema_name", "VARCHAR"),
        ("table_name", "VARCHAR"),
        ("column_name", "VARCHAR"),
        ("key_sequence", "BIGINT"),
        ("constraint_name", "VARCHAR"),
        ("rely", "VARCHAR"),
        ("comment", "VARCHAR"),
    ),
    "IMPORTED KEYS": (
        ("created_on", "TIMESTAMPTZ"),
        ("pk_database_name", "VARCHAR"),
        ("pk_schema_name", "VARCHAR"),
        ("pk_table_name", "VARCHAR"),
        ("pk_column_name", "VARCHAR"),
        ("fk_database_name", "VARCHAR"),
        ("fk_schema_name", "VARCHAR"),
        ("fk_table_name", "VARCHAR"),
        ("fk_column_name", "VARCHAR"),
        ("key_sequence", "BIGINT"),
        ("update_rule", "VARCHAR"),
        ("delete_rule", "VARCHAR"),
        ("fk_name", "VARCHAR"),
        ("pk_name", "VARCHAR"),
        ("deferrability", "VARCHAR"),
        ("rely", "VARCHAR"),
        ("comment", "VARCHAR"),
    ),
    "UNIQUE KEYS": (
        ("created_on", "TIMESTAMPTZ"),
        ("database_name", "VARCHAR"),
        ("schema_name", "VARCHAR"),
        ("table_name", "VARCHAR"),
        ("column_name", "VARCHAR"),
        ("key_sequence", "BIGINT"),
        ("constraint_name", "VARCHAR"),
        ("rely", "VARCHAR"),
        ("comment", "VARCHAR"),
    ),
    "TRANSACTIONS": (
        ("id", "BIGINT"),
        ("user", "VARCHAR"),
        ("session", "BIGINT"),
        ("started", "BIGINT"),
        ("state", "VARCHAR"),
    ),
}

_TERSE_TEMPLATES = {
    "OBJECTS": "objects_terse.sql",
    "TABLES": "objects_terse.sql",
    "VIEWS": "objects_terse.sql",
    "SCHEMAS": "schemas_terse.sql",
    "DATABASES": "databases_terse.sql",
}


def _template(name: str) -> str:
    return os.path.join(_TEMPLATE_DIR, name)


def _quoted_list(values: Sequence[str]) -> str:
    return ", ".join(sql_literal(value) for value in values)


def build_show_sql(request: ResolvedShowRequest, context: "DialectContext") -> str:
    """Translate one resolved ``SHOW`` request into a DuckDB query."""
    builder = _ShowBuilder(request, context)
    return builder.build()


class _ShowBuilder:
    def __init__(self, request: ResolvedShowRequest, context: "DialectContext") -> None:
        self._request = request
        self._context = context
        manager = context.info_schema_manager
        self._account_catalog = manager.account_catalog_name
        self._base_catalog = manager.base_catalog_name
        self._info_schema = manager.info_schema_name

    # -- entry point --------------------------------------------------------

    def build(self) -> str:
        kind = self._request.kind
        handler = {
            "OBJECTS": self._objects,
            "TABLES": self._tables,
            "VIEWS": self._views,
            "SCHEMAS": self._schemas,
            "DATABASES": self._databases,
            "TERSE DATABASES": self._databases,
            "COLUMNS": self._columns,
            "FUNCTIONS": self._functions,
            "USER FUNCTIONS": self._functions,
            "SEQUENCES": self._sequences,
            "STAGES": self._stages,
            "PARAMETERS": self._parameters,
            "VARIABLES": self._variables,
            "WAREHOUSES": self._warehouses,
        }.get(kind)

        if handler is not None:
            return handler()

        shape = _EMPTY_RESULT_SHAPES.get(kind)
        if shape is not None:
            return _empty_result(shape)

        raise UnsupportedShowError(
            f"SHOW {kind} is not supported by SnowDuck. "
            "Supported object types: "
            + ", ".join(
                sorted(
                    {
                        "OBJECTS",
                        "TABLES",
                        "VIEWS",
                        "SCHEMAS",
                        "DATABASES",
                        "COLUMNS",
                        "FUNCTIONS",
                        "USER FUNCTIONS",
                        "SEQUENCES",
                        "STAGES",
                        "PARAMETERS",
                        "VARIABLES",
                        "WAREHOUSES",
                        *_EMPTY_RESULT_SHAPES,
                    }
                )
            )
        )

    # -- shared fragments ---------------------------------------------------

    @property
    def _excluded_databases(self) -> str:
        return _quoted_list(
            [*_INTERNAL_DATABASES, self._account_catalog, self._base_catalog]
        )

    def _object_source(self) -> str:
        return load_sql(
            _template("_objects.sql"),
            info_schema_name=self._info_schema,
            fragments={"excluded_databases": self._excluded_databases},
        )

    def _scope_predicates(self, *, catalog: str, schema: str | None) -> str:
        clauses = []
        if self._request.database:
            clauses.append(
                f"upper({catalog}) = upper({sql_literal(self._request.database)})"
            )
        if schema and self._request.schema:
            clauses.append(
                f"upper({schema}) = upper({sql_literal(self._request.schema)})"
            )
        return "".join(f"\n  AND {clause}" for clause in clauses)

    def _name_predicates(self, column: str) -> str:
        """LIKE / STARTS WITH / FROM, the three ways SHOW narrows by name.

        ``STARTS WITH`` is a literal prefix in Snowflake, so it uses DuckDB's
        ``starts_with`` rather than LIKE - a model named ``pct_100%`` would
        otherwise match far more than itself.
        """
        request = self._request
        clauses = []
        if request.like is not None:
            clauses.append(f"{column} ILIKE {sql_literal(request.like)}")
        if request.starts_with is not None:
            clauses.append(
                f"starts_with(upper({column}), upper({sql_literal(request.starts_with)}))"
            )
        if request.from_ is not None:
            clauses.append(f"{column} > {sql_literal(request.from_)}")
        return "".join(f"\n  AND {clause}" for clause in clauses)

    def _tail(self, order_by: str) -> str:
        tail = f"ORDER BY {order_by}"
        if self._request.limit is not None:
            tail += f"\nLIMIT {int(self._request.limit)}"
        return tail

    # -- object types -------------------------------------------------------

    def _object_predicates(self) -> str:
        return self._scope_predicates(
            catalog="o.database_name", schema="o.schema_name"
        ) + self._name_predicates("o.name")

    def _object_tail(self) -> str:
        return self._tail("o.database_name, o.schema_name, o.name")

    def _objects(self) -> str:
        template = "objects_terse.sql" if self._request.terse else "objects.sql"
        return load_sql(
            _template(template),
            fragments={
                "objects": self._object_source(),
                "predicates": self._object_predicates(),
                "tail": self._object_tail(),
            },
        )

    def _tables(self) -> str:
        if self._request.terse:
            return load_sql(
                _template(_TERSE_TEMPLATES["TABLES"]),
                fragments={
                    "objects": self._object_source(),
                    "predicates": "\n  AND o.kind = 'TABLE'"
                    + self._object_predicates(),
                    "tail": self._object_tail(),
                },
            )
        return load_sql(
            _template("tables.sql"),
            fragments={
                "objects": self._object_source(),
                "predicates": self._object_predicates(),
                "tail": self._object_tail(),
            },
        )

    def _views(self) -> str:
        if self._request.terse:
            return load_sql(
                _template(_TERSE_TEMPLATES["VIEWS"]),
                fragments={
                    "objects": self._object_source(),
                    "predicates": "\n  AND o.kind = 'VIEW'" + self._object_predicates(),
                    "tail": self._object_tail(),
                },
            )
        return load_sql(
            _template("views.sql"),
            fragments={
                "objects": self._object_source(),
                "predicates": self._object_predicates(),
                "tail": self._object_tail(),
            },
        )

    def _schemas(self) -> str:
        template = "schemas_terse.sql" if self._request.terse else "schemas.sql"
        predicates = self._scope_predicates(
            catalog="s.database_name", schema=None
        ) + self._name_predicates("s.schema_name")
        return load_sql(
            _template(template),
            account_catalog_name=self._account_catalog,
            info_schema_name=self._info_schema,
            fragments={
                "excluded_databases": self._excluded_databases,
                "current_schema": sql_literal(self._context.current_schema or ""),
                "predicates": predicates,
                "tail": self._tail("s.database_name, s.schema_name"),
            },
        )

    def _databases(self) -> str:
        template = "databases_terse.sql" if self._request.terse else "databases.sql"
        return load_sql(
            _template(template),
            fragments={
                "excluded_databases": self._excluded_databases,
                "current_database": sql_literal(self._context.current_database or ""),
                "predicates": self._name_predicates("d.database_name"),
                "tail": self._tail("d.database_name"),
            },
        )

    def _columns(self) -> str:
        predicates = self._scope_predicates(
            catalog="c.table_catalog", schema="c.table_schema"
        )
        if self._request.table:
            predicates += (
                f"\n  AND upper(c.table_name) = "
                f"upper({sql_literal(self._request.table)})"
            )
        predicates += self._name_predicates("c.column_name")
        return load_sql(
            _template("columns.sql"),
            account_catalog_name=self._account_catalog,
            info_schema_name=self._info_schema,
            fragments={
                "predicates": predicates,
                "tail": self._tail("c.table_name, c.ordinal_position"),
            },
        )

    def _functions(self) -> str:
        """SHOW [USER] FUNCTIONS.

        ``SHOW FUNCTIONS`` includes the engine's built-ins as well, which is
        what Snowflake does; ``SHOW USER FUNCTIONS`` lists only UDFs. UDFs are
        DuckDB macros; SnowDuck's own compatibility macros live in each
        database's ``main`` schema and are filtered out here.
        """
        scope = self._scope_predicates(
            catalog="f.database_name", schema="f.schema_name"
        ).replace("\n  AND ", "\n       AND ")
        selector = (
            "NOT f.internal"
            "\n       AND f.function_type IN ('macro', 'table_macro')"
            f"\n       AND f.database_name NOT IN ({self._excluded_databases})"
            f"\n       AND f.schema_name NOT IN ('{self._info_schema}', 'main')" + scope
        )
        if self._request.kind == "FUNCTIONS":
            selector += "\n    OR f.internal"

        return load_sql(
            _template("functions.sql"),
            fragments={
                "selector": selector,
                "predicates": self._name_predicates("f.function_name"),
                "tail": self._tail("f.function_name"),
            },
        )

    def _sequences(self) -> str:
        predicates = self._scope_predicates(
            catalog="s.database_name", schema="s.schema_name"
        ) + self._name_predicates("s.sequence_name")
        return load_sql(
            _template("sequences.sql"),
            info_schema_name=self._info_schema,
            fragments={
                "excluded_databases": self._excluded_databases,
                "predicates": predicates,
                "tail": self._tail("s.database_name, s.schema_name, s.sequence_name"),
            },
        )

    def _stages(self) -> str:
        """Stages are directories under ``SNOWDUCK_STAGE_DIR``."""
        stage_root = os.getenv("SNOWDUCK_STAGE_DIR", "/tmp/snowduck_stage")
        try:
            names = sorted(
                entry
                for entry in os.listdir(stage_root)
                if os.path.isdir(os.path.join(stage_root, entry))
            )
        except OSError:
            names = []

        columns = (
            ("created_on", "TIMESTAMPTZ"),
            ("name", "VARCHAR"),
            ("database_name", "VARCHAR"),
            ("schema_name", "VARCHAR"),
            ("url", "VARCHAR"),
            ("has_credentials", "VARCHAR"),
            ("has_encryption_key", "VARCHAR"),
            ("owner", "VARCHAR"),
            ("comment", "VARCHAR"),
            ("region", "VARCHAR"),
            ("type", "VARCHAR"),
            ("cloud", "VARCHAR"),
            ("notification_channel", "VARCHAR"),
            ("storage_integration", "VARCHAR"),
            ("endpoint", "VARCHAR"),
            ("owner_role_type", "VARCHAR"),
            ("directory_enabled", "VARCHAR"),
        )
        database = self._request.database or ""
        schema = self._request.schema or ""
        rows = [
            (
                "TO_TIMESTAMP(0)::TIMESTAMPTZ",
                sql_literal(name),
                sql_literal(database),
                sql_literal(schema),
                sql_literal(os.path.join(stage_root, name)),
                "'N'",
                "'N'",
                "'SYSADMIN'",
                "NULL",
                "NULL",
                "'INTERNAL'",
                "NULL",
                "NULL",
                "NULL",
                "NULL",
                "'ROLE'",
                "'N'",
            )
            for name in self._filter_names(names)
        ]
        return _values_result(columns, rows)

    def _parameters(self) -> str:
        """SHOW PARAMETERS - defaults overlaid with this session's overrides."""
        columns = (
            ("key", "VARCHAR"),
            ("value", "VARCHAR"),
            ("default", "VARCHAR"),
            ("level", "VARCHAR"),
            ("description", "VARCHAR"),
            ("type", "VARCHAR"),
        )
        overrides = self._context.session_parameters
        rows = []
        keys = sorted(set(SESSION_PARAMETER_DEFAULTS) | set(overrides))
        for key in self._filter_names(keys):
            default, type_name, description = SESSION_PARAMETER_DEFAULTS.get(
                key, ("", "STRING", "")
            )
            value = overrides.get(key, default)
            level = "SESSION" if key in overrides else ""
            rows.append(
                (
                    sql_literal(key.lower()),
                    sql_literal(value),
                    sql_literal(default),
                    sql_literal(level),
                    sql_literal(description),
                    sql_literal(type_name),
                )
            )
        return _values_result(columns, rows)

    def _variables(self) -> str:
        columns = (
            ("session_id", "VARCHAR"),
            ("created_on", "TIMESTAMPTZ"),
            ("updated_on", "TIMESTAMPTZ"),
            ("name", "VARCHAR"),
            ("value", "VARCHAR"),
            ("type", "VARCHAR"),
            ("comment", "VARCHAR"),
        )
        variables = self._context.session_variables
        rows = [
            (
                "'1'",
                "TO_TIMESTAMP(0)::TIMESTAMPTZ",
                "TO_TIMESTAMP(0)::TIMESTAMPTZ",
                sql_literal(name),
                sql_literal(variables[name]),
                "'TEXT'",
                "NULL",
            )
            for name in self._filter_names(sorted(variables))
        ]
        return _values_result(columns, rows)

    def _warehouses(self) -> str:
        columns = (
            ("name", "VARCHAR"),
            ("state", "VARCHAR"),
            ("type", "VARCHAR"),
            ("size", "VARCHAR"),
            ("running", "BIGINT"),
            ("queued", "BIGINT"),
            ("is_default", "VARCHAR"),
            ("is_current", "VARCHAR"),
            ("auto_suspend", "BIGINT"),
            ("auto_resume", "VARCHAR"),
            ("available", "VARCHAR"),
            ("provisioning", "VARCHAR"),
            ("quiescing", "VARCHAR"),
            ("other", "VARCHAR"),
            ("created_on", "TIMESTAMPTZ"),
            ("resumed_on", "TIMESTAMPTZ"),
            ("updated_on", "TIMESTAMPTZ"),
            ("owner", "VARCHAR"),
            ("comment", "VARCHAR"),
            ("owner_role_type", "VARCHAR"),
        )
        warehouse = self._context.current_warehouse or "DEFAULT_WAREHOUSE"
        rows = [
            (
                sql_literal(warehouse),
                "'STARTED'",
                "'STANDARD'",
                "'X-Small'",
                "0",
                "0",
                "'Y'",
                "'Y'",
                "600",
                "'true'",
                "'100'",
                "'0'",
                "'0'",
                "'0'",
                "TO_TIMESTAMP(0)::TIMESTAMPTZ",
                "TO_TIMESTAMP(0)::TIMESTAMPTZ",
                "TO_TIMESTAMP(0)::TIMESTAMPTZ",
                "'SYSADMIN'",
                "NULL",
                "'ROLE'",
            )
            for _ in self._filter_names([warehouse])
        ]
        return _values_result(columns, rows)

    # -- Python-side name filtering ----------------------------------------

    def _filter_names(self, names: Sequence[str]) -> list[str]:
        """Apply LIKE / STARTS WITH / FROM / LIMIT to a Python-built result."""
        request = self._request
        result = list(names)
        if request.like is not None:
            pattern = _like_to_regex(request.like)
            result = [n for n in result if pattern.match(n)]
        if request.starts_with is not None:
            prefix = request.starts_with.upper()
            result = [n for n in result if n.upper().startswith(prefix)]
        if request.from_ is not None:
            result = [n for n in result if n > request.from_]
        if request.limit is not None:
            result = result[: request.limit]
        return result


def _like_to_regex(pattern: str) -> "re.Pattern[str]":
    """Snowflake's SHOW ... LIKE - SQL wildcards, matched case-insensitively."""
    translated = "".join(
        {"%": ".*", "_": "."}.get(char, re.escape(char)) for char in pattern
    )
    return re.compile(f"^{translated}$", re.IGNORECASE)


def _empty_result(columns: Sequence[tuple[str, str]]) -> str:
    projection = ",\n    ".join(
        f"NULL::{type_name} AS '{name}'" for name, type_name in columns
    )
    return f"SELECT\n    {projection}\nWHERE FALSE"


def _values_result(
    columns: Sequence[tuple[str, str]], rows: Sequence[Sequence[str]]
) -> str:
    """A literal result set, named the way Snowflake names those columns."""
    if not rows:
        return _empty_result(columns)
    aliases = ", ".join(f"c{index}" for index in range(len(columns)))
    projection = ",\n    ".join(
        f"c{index} AS '{name}'" for index, (name, _) in enumerate(columns)
    )
    values = ",\n    ".join("(" + ", ".join(row) + ")" for row in rows)
    return f"SELECT\n    {projection}\nFROM (VALUES\n    {values}) AS _v({aliases})"
