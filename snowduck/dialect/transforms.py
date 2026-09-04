import os
from typing import cast

import sqlglot
from sqlglot import exp

from ..show import build_show_sql, parse_show
from .context import DialectContext


def transform_set(expression: exp.Expression, context: DialectContext) -> str:
    """
    Transform SET variable = value into context storage.

    Snowflake: SET my_var = 'hello'
    Action: Store in context.session_variables, return success message
    """
    if isinstance(expression, exp.Set):
        for set_item in expression.expressions:
            if isinstance(set_item, exp.SetItem):
                # Extract variable name and value
                if isinstance(set_item.this, exp.EQ):
                    var_node = set_item.this.this
                    val_node = set_item.this.expression

                    if isinstance(var_node, exp.Column):
                        var_name = var_node.name.upper()
                    elif isinstance(var_node, exp.Identifier):
                        var_name = var_node.this.upper()
                    else:
                        continue

                    # Get the literal value
                    if isinstance(val_node, exp.Literal):
                        var_value = val_node.this
                    else:
                        # For non-literal values, convert to SQL string
                        var_value = val_node.sql(dialect="duckdb")

                    # Store in context
                    context.session_variables[var_name] = var_value

        # Return a dummy SELECT to satisfy the execute
        return "SELECT 'Statement executed successfully.' AS status"

    return ""


def transform_function(expression: exp.Create, context: DialectContext) -> str:
    """Transform a Snowflake SQL UDF into a DuckDB macro.

    Snowflake:
        CREATE OR REPLACE FUNCTION f(a VARCHAR) RETURNS BOOLEAN AS $$ LENGTH(a) = 11 $$
    DuckDB:
        CREATE OR REPLACE MACRO f(a) AS (LENGTH(a) = 11)

    The body arrives as a RawString/Literal. Without this transform the DuckDB
    generator emits it as a *string literal*, so DuckDB creates a macro that
    returns the function's own source text instead of evaluating it - silently,
    with no error at either DDL or call time.
    """
    from .dialect import Dialect

    udf = expression.this
    if not isinstance(udf, exp.UserDefinedFunction):
        raise ValueError(
            "CREATE FUNCTION without a function signature is not supported"
        )

    # Only SQL UDFs can be expressed as DuckDB macros. Fail loudly for the rest
    # rather than emitting something that silently returns the wrong thing.
    properties = expression.args.get("properties")
    for prop in properties.expressions if properties else []:
        if isinstance(prop, exp.LanguageProperty):
            language = (
                prop.this.name
                if isinstance(prop.this, exp.Expression)
                else str(prop.this)
            )
            if language.upper() != "SQL":
                raise ValueError(
                    f"CREATE FUNCTION ... LANGUAGE {language.upper()} is not supported; "
                    "only SQL UDFs can be emulated (as DuckDB macros)"
                )

    body = expression.expression
    if body is None:
        raise ValueError("CREATE FUNCTION without a body is not supported")

    # RawString ($$ ... $$) and Literal (' ... ') both carry the body as text.
    if isinstance(body, (exp.RawString, exp.Literal)):
        body_sql = body.this
    else:
        body_sql = body.sql(dialect="snowflake")

    parsed_body = sqlglot.parse_one(body_sql, read="snowflake")
    # Translate the body through the full SnowDuck pipeline so Snowflake
    # functions used inside the UDF (TO_NUMBER, REGEXP_SUBSTR, ...) are mapped too.
    translated_body = parsed_body.sql(dialect=Dialect(context=context))

    params = ", ".join(
        param.name if isinstance(param, exp.ColumnDef) else param.sql(dialect="duckdb")
        for param in udf.expressions or []
    )

    name = (
        udf.this.sql(dialect="duckdb")
        if isinstance(udf.this, exp.Expression)
        else str(udf.this)
    )
    or_replace = "OR REPLACE " if expression.args.get("replace") else ""
    if_not_exists = "IF NOT EXISTS " if expression.args.get("exists") else ""

    # Whether a UDF is a table function is decided by its signature, not by the
    # shape of its body: `RETURNS FLOAT AS $$ SELECT amount * 1.2 $$` is a
    # scalar function returning one value, and dbt's SQL function models are
    # written exactly that way. Reading it off the body instead produced a
    # DuckDB table macro that could only be called in a FROM clause.
    if _returns_table(expression):
        as_clause = f"TABLE ({translated_body})"
    elif isinstance(parsed_body, (exp.Select, exp.Union, exp.Subquery)):
        # A scalar function whose body is a query: a scalar subquery.
        as_clause = f"(({translated_body}))"
    else:
        as_clause = f"({translated_body})"

    return f"CREATE {or_replace}MACRO {if_not_exists}{name}({params}) AS {as_clause}"


def _returns_table(expression: exp.Create) -> bool:
    """True for `RETURNS TABLE (...)`."""
    properties = expression.args.get("properties")
    for prop in properties.expressions if properties else []:
        if not isinstance(prop, exp.ReturnsProperty):
            continue
        if prop.args.get("is_table"):
            return True
        returns = prop.this
        if isinstance(returns, exp.Schema) and isinstance(returns.this, exp.Var):
            return str(returns.this.this).upper() == "TABLE"
    return False


def transform_clone(
    expression: exp.Create, clone: exp.Expression, context: DialectContext
) -> str:
    """CREATE TABLE <new> CLONE <source> -> a CTAS copy.

    Snowflake's clone is zero-copy and metadata-only; an eager copy is the
    closest local equivalent and behaves identically for reads and writes.
    """
    target = expression.this
    source = clone.this if isinstance(clone, exp.Clone) else clone
    or_replace = "OR REPLACE " if expression.args.get("replace") else ""
    if_not_exists = "IF NOT EXISTS " if expression.args.get("exists") else ""
    kind = str(expression.args.get("kind")).upper()
    return (
        f"CREATE {or_replace}{kind} {if_not_exists}"
        f"{target.sql(dialect='duckdb')} AS SELECT * FROM {source.sql(dialect='duckdb')}"
    )


def transform_stage(expression: exp.Create, context: DialectContext) -> str:
    """CREATE STAGE makes the local directory that PUT and COPY INTO use."""
    ident = expression.find(exp.Identifier)
    name = (ident.this if ident.quoted else ident.this.upper()) if ident else "STAGE"
    stage_root = os.getenv("SNOWDUCK_STAGE_DIR", "/tmp/snowduck_stage")
    os.makedirs(os.path.join(stage_root, name), exist_ok=True)
    return f"SELECT 'Stage area {name} successfully created.' AS status"


def transform_create(expression: exp.Create, context: DialectContext) -> str:
    """Custom transformation for CREATE DATABASE/SCHEMA to use uppercase identifiers."""
    kind = str(expression.args.get("kind")).upper()

    if kind == "FUNCTION":
        return transform_function(expression, context)

    if kind == "STAGE":
        return transform_stage(expression, context)

    clone = expression.args.get("clone")
    if clone is not None:
        return transform_clone(expression, clone, context)

    if kind == "DATABASE":
        ident = expression.find(exp.Identifier)
        if not ident:
            raise ValueError(
                f"No identifier found in CREATE DATABASE statement: {expression.sql}"
            )

        # Use uppercase for unquoted identifiers to match Snowflake behavior
        db_name = ident.this if ident.quoted else ident.this.upper()
        db_file = ":memory:"
        if_not_exists = "IF NOT EXISTS " if expression.args.get("exists") else ""

        return f"ATTACH {if_not_exists}DATABASE '{db_file}' AS {db_name}"

    if kind in ("TABLE", "VIEW"):
        _normalise_relation_properties(expression)

    return expression.sql(dialect="duckdb")


#: Snowflake relation properties DuckDB has no equivalent for. TRANSIENT only
#: removes Fail-safe, which SnowDuck does not have either; TEMPORARY is dropped
#: only when the relation is qualified, because DuckDB puts temporary objects in
#: its own `temp` catalog and rejects any other qualification - and dbt creates
#: its incremental staging relations as
#: `create or replace temporary view <db>.<schema>.<name>__dbt_tmp`.
_DROPPED_RELATION_PROPERTIES = (exp.TransientProperty,)


def _normalise_relation_properties(expression: exp.Create) -> None:
    properties = expression.args.get("properties")
    if properties is None:
        return

    target = expression.this
    if isinstance(target, exp.Schema):
        target = target.this
    qualified = isinstance(target, exp.Table) and bool(target.db or target.catalog)

    kept = [
        prop
        for prop in properties.expressions
        if not isinstance(prop, _DROPPED_RELATION_PROPERTIES)
        and not (qualified and isinstance(prop, exp.TemporaryProperty))
    ]
    if len(kept) == len(properties.expressions):
        return
    if kept:
        properties.set("expressions", kept)
    else:
        expression.set("properties", None)


def transform_describe(expression: exp.Describe, context: DialectContext) -> str:
    """DESCRIBE TABLE/VIEW -> an information-schema query in Snowflake's shape."""
    if str(expression.args.get("kind")).upper() not in ("TABLE", "VIEW"):
        return expression.sql(dialect="duckdb")

    table = expression.find(exp.Table)
    if table is None:
        return expression.sql(dialect="duckdb")

    manager = context.info_schema_manager
    database = table.catalog or context.current_database
    schema = table.db or context.current_schema

    if not database:
        raise ValueError(
            f"No database context for DESCRIBE {table.name}. "
            f"Use 'USE DATABASE <db>' or specify database explicitly."
        )
    if not schema:
        raise ValueError(
            f"No schema context for DESCRIBE {table.name}. "
            f"Use 'USE SCHEMA <schema>' or specify schema explicitly."
        )
    if not table.name:
        raise ValueError("Table name must be specified for DESCRIBE")

    # An INFORMATION_SCHEMA view has already been rewritten onto its backing
    # view by preprocess_info_schema, so the schema here is the internal name.
    # Its columns come from DuckDB rather than from _columns, which only tracks
    # user tables.
    if schema.upper() in ("INFORMATION_SCHEMA", manager.info_schema_name.upper()):
        return manager.describe_info_schema_sql(
            view=f"{database}.{manager.info_schema_name}.{table.name}"
        )

    return manager.describe_table_sql(
        database=database, schema=schema, table=table.name
    )


def transform_use(expression: exp.Use, context: DialectContext) -> str:
    """Convert USE SCHEMA/DATABASE to SET schema."""
    kind = expression.args.get("kind")
    if not isinstance(kind, exp.Var) or not kind.name:
        return expression.sql(dialect="duckdb")

    if kind.name.upper() == "DATABASE":
        database = expression.this.name
        return f"SET schema = '{database}.PUBLIC'"

    elif kind.name.upper() == "SCHEMA":
        db_name = (
            expression.this.args.get("db").name
            if expression.this.args.get("db")
            else context.current_database
        )
        schema = expression.this.name
        if not db_name:
            raise ValueError(
                f"No database context for schema '{schema}'. "
                f"Use 'USE DATABASE <db>' first or specify database explicitly."
            )

        return f"SET schema = '{db_name}.{schema}'"

    return expression.sql(dialect="duckdb")


def transform_show(expression: exp.Expression, context: DialectContext) -> str:
    """Render a Snowflake ``SHOW`` as a DuckDB query with Snowflake's columns.

    ``exp.Show`` and ``exp.Command`` both reach this - sqlglot models only part
    of the SHOW grammar and falls back to ``Command`` for the rest - so the
    statement is re-read from its own Snowflake SQL by a single scanner.
    """
    request = parse_show(expression)
    if request is None:
        return expression.sql(dialect="duckdb")

    resolved = request.resolved(
        database=context.current_database,
        schema=context.current_schema,
    )
    return build_show_sql(resolved, context)


def transform_command(expression: exp.Command, context: DialectContext) -> str:
    """Statements sqlglot parsed as opaque commands.

    Only ``SHOW`` is claimed here; everything else keeps its previous
    behaviour of being handed to DuckDB verbatim.
    """
    if parse_show(expression) is not None:
        return transform_show(expression, context)
    return expression.sql(dialect="duckdb")


def flatten_input_sql(
    expression: exp.Expression | None, dialect: str = "duckdb"
) -> str:
    """SQL for a FLATTEN input. The JSON[] cast is applied during preprocessing."""
    if expression is None:
        return "NULL"
    return expression.sql(dialect=dialect)


def _explode_input(explode: exp.Expression) -> exp.Expression | None:
    """The `input =>` argument of a FLATTEN call."""
    kwarg = explode.args.get("this")
    if isinstance(kwarg, exp.Kwarg):
        return cast(exp.Expression, kwarg.expression)
    return kwarg if isinstance(kwarg, exp.Expression) else None


def transform_lateral(expression: exp.Lateral, context: DialectContext) -> str:
    """Transform LATERAL FLATTEN/EXPLODE into DuckDB UNNEST."""
    if isinstance(expression.this, exp.Explode):
        input_sql = flatten_input_sql(_explode_input(expression.this))

        alias = expression.args.get("alias")
        alias_name = (
            alias.this.sql(dialect="duckdb") if alias and alias.this else "_flattened"
        )
        return f"LATERAL UNNEST({input_sql}) AS {alias_name}(VALUE)"

    return expression.sql(dialect="duckdb")


def transform_table_from_rows(
    expression: exp.TableFromRows, context: DialectContext
) -> str:
    """Transform Snowflake's `TABLE(<table function>)` in a FROM clause.

    Snowflake wraps every table function call in `TABLE(...)`; DuckDB calls the
    function directly. FLATTEN and SPLIT_TO_TABLE additionally have no DuckDB
    counterpart and become UNNEST.
    """
    inner = expression.this
    alias = expression.args.get("alias")
    alias_sql = f" AS {alias.sql(dialect='duckdb')}" if alias else ""

    if isinstance(inner, exp.Explode):
        return f"UNNEST({flatten_input_sql(_explode_input(inner))}){alias_sql or ' AS _flattened(VALUE)'}"

    if isinstance(inner, exp.Anonymous) and isinstance(inner.this, str):
        name = inner.this.upper()
        if name in ("SPLIT_TO_TABLE", "STRTOK_SPLIT_TO_TABLE"):
            args = list(inner.expressions)
            if len(args) >= 2:
                subject = args[0].sql(dialect="duckdb")
                delim = args[1].sql(dialect="duckdb")
                split = f"str_split({subject}, {delim})"
                return f"UNNEST({split}){alias_sql or ' AS _split(VALUE)'}"

    return f"{inner.sql(dialect='duckdb')}{alias_sql}"


def transform_copy(expression: exp.Copy, context: DialectContext) -> str:
    """Transform COPY INTO <table> FROM @<stage> into DuckDB COPY FROM local stage directory."""
    if not expression.args.get("kind"):
        return expression.sql(dialect="duckdb")

    this = expression.this
    table_sql = this.sql(dialect="duckdb") if this is not None else None
    if not table_sql:
        return expression.sql(dialect="duckdb")

    stage_path = None
    files = expression.args.get("files") or []
    if files:
        first = files[0]
        if isinstance(first, exp.Table) and isinstance(first.this, exp.Var):
            stage_path = str(first.this).lstrip("@")

    if not stage_path:
        return expression.sql(dialect="duckdb")

    local_stage = os.getenv("SNOWDUCK_STAGE_DIR", "/tmp/snowduck_stage")
    source_path = f"{local_stage}/{stage_path}"

    return f"COPY {table_sql} FROM '{source_path}'"
