import hashlib
import os
import re
import shutil
from typing import cast

import sqlglot
from sqlglot import exp

from ..helper import sql_literal
from ..show import build_show_sql, parse_show
from .context import DialectContext

# A statement Snowflake accepts that has no local effect.
SQL_NOOP = "SELECT 'Statement executed successfully.' AS status"


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


def stage_root() -> str:
    """The directory PUT, COPY INTO and the stage commands share."""
    return os.getenv("SNOWDUCK_STAGE_DIR", "/tmp/snowduck_stage")


def transform_drop(expression: exp.Drop, context: DialectContext) -> str:
    """DROP for object kinds DuckDB does not have."""
    if str(expression.args.get("kind")).upper() == "STAGE":
        return transform_drop_stage(expression, context)
    return expression.sql(dialect="duckdb")


def transform_drop_stage(expression: exp.Drop, context: DialectContext) -> str:
    """DROP STAGE removes the local directory CREATE STAGE made."""
    ident = expression.find(exp.Identifier)
    name = (ident.this if ident.quoted else ident.this.upper()) if ident else ""
    if name:
        shutil.rmtree(os.path.join(stage_root(), name), ignore_errors=True)
    return f"SELECT '{name} successfully dropped.' AS status"


def transform_stage(expression: exp.Create, context: DialectContext) -> str:
    """CREATE STAGE makes the local directory that PUT and COPY INTO use."""
    ident = expression.find(exp.Identifier)
    name = (ident.this if ident.quoted else ident.this.upper()) if ident else "STAGE"
    os.makedirs(os.path.join(stage_root(), name), exist_ok=True)
    return f"SELECT 'Stage area {name} successfully created.' AS status"


_IDENTITY_CONSTRAINTS = (
    exp.AutoIncrementColumnConstraint,
    exp.GeneratedAsIdentityColumnConstraint,
)


def _identity_columns(create: exp.Create) -> list[tuple[str, int, int]]:
    """Columns declared IDENTITY / AUTOINCREMENT, with their start and step."""
    found: list[tuple[str, int, int]] = []
    for column in create.find_all(exp.ColumnDef):
        for constraint in column.args.get("constraints") or []:
            kind = constraint.kind
            if not isinstance(kind, _IDENTITY_CONSTRAINTS):
                continue

            def _number(node: object, default: int) -> int:
                if isinstance(node, exp.Literal) and not node.is_string:
                    try:
                        return int(node.this)
                    except (TypeError, ValueError):
                        return default
                return default

            start = _number(getattr(kind, "args", {}).get("start"), 1)
            step = _number(getattr(kind, "args", {}).get("increment"), 1)
            found.append((column.name, start, step))
    return found


def transform_identity_table(
    expression: exp.Create, columns: list[tuple[str, int, int]], context: DialectContext
) -> str:
    """Back IDENTITY / AUTOINCREMENT columns with DuckDB sequences.

    DuckDB has neither constraint ("The AUTOINCREMENT column constraint is not
    supported"), but a sequence used as the column default behaves the same for
    the inserts that matter.
    """
    table = expression.this
    table_name = table.this if isinstance(table, exp.Schema) else table
    qualified = table_name.sql(dialect="duckdb")
    prefix = qualified.replace(".", "_").replace('"', "")

    stripped = expression.copy()
    statements: list[str] = []
    defaults: dict[str, str] = {}

    for name, start, step in columns:
        sequence = f"_snowduck_seq_{prefix}_{name}"
        statements.append(f"DROP SEQUENCE IF EXISTS {sequence}")
        statements.append(f"CREATE SEQUENCE {sequence} START {start} INCREMENT {step}")
        defaults[name] = sequence

    for column in stripped.find_all(exp.ColumnDef):
        constraints = [
            constraint
            for constraint in column.args.get("constraints") or []
            if not isinstance(constraint.kind, _IDENTITY_CONSTRAINTS)
        ]
        sequence_name = defaults.get(column.name)
        if sequence_name is not None:
            constraints.append(
                exp.ColumnConstraint(
                    kind=exp.DefaultColumnConstraint(
                        this=exp.Anonymous(
                            this="nextval",
                            expressions=[exp.Literal.string(sequence_name)],
                        )
                    )
                )
            )
        column.set("constraints", constraints)

    statements.append(stripped.sql(dialect="duckdb"))
    return "; ".join(statements)


def transform_create(expression: exp.Create, context: DialectContext) -> str:
    """Custom transformation for CREATE DATABASE/SCHEMA to use uppercase identifiers."""
    kind = str(expression.args.get("kind")).upper()

    if kind == "FUNCTION":
        return transform_function(expression, context)

    if kind == "STAGE":
        return transform_stage(expression, context)

    if kind == "TABLE":
        identity = _identity_columns(expression)
        if identity:
            return transform_identity_table(expression, identity, context)

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
        # A file-backed session stores each catalog beside its own file, so a
        # database created at runtime survives a restart like any other data.
        db_file = context.info_schema_manager.database_path(db_name)
        if_not_exists = "IF NOT EXISTS " if expression.args.get("exists") else ""

        return f"ATTACH {if_not_exists}DATABASE '{db_file}' AS {db_name}"

    if kind in ("TABLE", "VIEW"):
        _normalise_relation_properties(expression)
        # Comments have to be detached before generation: DuckDB's generator
        # has no syntax for either the relation-level `COMMENT = '...'` or a
        # column's `COMMENT '...'` and drops both with a warning.
        comments = _extract_comments(expression, kind)
        # `IF NOT EXISTS` on a relation that is already there does nothing in
        # Snowflake, comments included - and `COMMENT ON` has no such guard, so
        # it would otherwise overwrite the existing description.
        if (
            comments
            and expression.args.get("exists")
            and _relation_exists(expression, context)
        ):
            comments = []
        if comments:
            return "; ".join([expression.sql(dialect="duckdb"), *comments])

    return expression.sql(dialect="duckdb")


#: Snowflake relation properties DuckDB has no equivalent for. TRANSIENT only
#: removes Fail-safe, which SnowDuck does not have either; SECURE and
#: MATERIALIZED are recorded in the catalog (see `secure_relations` and
#: `is_materialized`) rather than changing how the relation is built; TEMPORARY
#: is dropped only when the relation is qualified, because DuckDB puts
#: temporary objects in its own `temp` catalog and rejects any other
#: qualification - and dbt creates its incremental staging relations as
#: `create or replace temporary view <db>.<schema>.<name>__dbt_tmp`.
_DROPPED_RELATION_PROPERTIES = (
    exp.TransientProperty,
    exp.SecureProperty,
    exp.MaterializedProperty,
)


def _relation_target(expression: exp.Create) -> exp.Table | None:
    """The table/view a CREATE names, unwrapped from its column list."""
    target = expression.this
    if isinstance(target, exp.Schema):
        target = target.this
    return target if isinstance(target, exp.Table) else None


def _relation_exists(expression: exp.Create, context: DialectContext) -> bool:
    target = _relation_target(expression)
    if target is None:
        return False
    return context.info_schema_manager.has_relation(
        database=target.catalog or context.current_database or "",
        schema=target.db or context.current_schema or "",
        name=target.name,
    )


def _extract_comments(expression: exp.Create, kind: str) -> list[str]:
    """Pull every COMMENT off a CREATE, as the `COMMENT ON` it is equivalent to.

    Snowflake attaches comments inline (`COMMENT = '...'` on the relation,
    `COMMENT '...'` on a column); DuckDB only has the standalone `COMMENT ON`
    statement. dbt's `persist_docs` writes documentation through both forms, so
    dropping them silently loses every model and column description.
    """
    target = _relation_target(expression)
    if target is None:
        return []
    relation = target.sql(dialect="duckdb")

    statements: list[str] = []

    properties = expression.args.get("properties")
    for prop in list(properties.expressions if properties else []):
        if isinstance(prop, exp.SchemaCommentProperty):
            text = _comment_literal(prop.this)
            if text is not None:
                statements.append(f"COMMENT ON {kind} {relation} IS {text}")
            prop.pop()

    schema = expression.this
    if isinstance(schema, exp.Schema):
        for column in schema.expressions:
            if not isinstance(column, exp.ColumnDef):
                continue
            for constraint in list(column.args.get("constraints") or []):
                inner = constraint.args.get("kind")
                if not isinstance(inner, exp.CommentColumnConstraint):
                    continue
                text = _comment_literal(inner.this)
                if text is not None:
                    name = exp.to_identifier(column.name).sql(dialect="duckdb")
                    statements.append(f"COMMENT ON COLUMN {relation}.{name} IS {text}")
                constraint.pop()

    if properties is not None and not properties.expressions:
        expression.set("properties", None)

    return statements


def _comment_literal(node: exp.Expression | None) -> str | None:
    """A comment's text as a DuckDB string literal."""
    if isinstance(node, exp.Literal) and node.is_string:
        return sql_literal(node.this)
    if isinstance(node, exp.Expression):
        return node.sql(dialect="duckdb")
    return None


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


def transform_alter(expression: exp.Alter, context: DialectContext) -> str:
    """ALTER TABLE clauses Snowflake has and DuckDB does not.

    `SWAP WITH` is the interesting one: dbt uses it to publish a rebuilt table
    atomically. DuckDB has no such statement, so it becomes the three renames
    it is equivalent to.
    """
    actions = expression.args.get("actions") or []

    for action in actions:
        if isinstance(action, exp.SwapTable):
            return _swap_tables(expression, action)

        if isinstance(action, exp.ClusterProperty):
            # Clustering is a Snowflake storage hint with no local meaning.
            return SQL_NOOP

        if isinstance(action, exp.AlterSet):
            comment = action.find(exp.SchemaCommentProperty)
            if comment is not None:
                table = expression.this.sql(dialect="duckdb")
                text = comment.this.sql(dialect="duckdb")
                return f"COMMENT ON TABLE {table} IS {text}"
            # SET on any other table property (DATA_RETENTION_TIME_IN_DAYS,
            # CHANGE_TRACKING, ...) has no local equivalent.
            return SQL_NOOP

        if isinstance(action, exp.AlterColumn):
            rendered = _alter_column(expression, action)
            if rendered is not None:
                return rendered

        if isinstance(action, exp.Drop) and _is_constraint_drop(action):
            # Snowflake tracks constraints as metadata and does not enforce
            # them; DuckDB has no DROP CONSTRAINT at all.
            return SQL_NOOP

        if isinstance(action, exp.AddConstraint):
            return SQL_NOOP

    return expression.sql(dialect="duckdb")


def transform_truncate(expression: exp.TruncateTable, context: DialectContext) -> str:
    """TRUNCATE TABLE [IF EXISTS] - DuckDB's TRUNCATE has no IF EXISTS.

    The guard is resolved against the catalog here rather than pushed into SQL,
    because DuckDB parses the whole statement before deciding anything.
    """
    if not expression.args.get("exists"):
        return expression.sql(dialect="duckdb")

    manager = context.info_schema_manager
    for table in expression.expressions:
        if not isinstance(table, exp.Table):
            continue
        database = table.catalog or context.current_database or ""
        schema = table.db or context.current_schema or ""
        if not manager.has_table(database=database, schema=schema, table=table.name):
            return SQL_NOOP

    without_guard = expression.copy()
    without_guard.set("exists", False)
    return str(without_guard.sql(dialect="duckdb"))


def _is_constraint_drop(action: exp.Drop) -> bool:
    kind = action.args.get("kind")
    return isinstance(kind, str) and kind.upper() == "CONSTRAINT"


def _alter_column(expression: exp.Alter, action: exp.AlterColumn) -> str | None:
    """The ALTER COLUMN forms DuckDB's generator cannot render.

    ``COMMENT`` has no DuckDB column-level clause at all, just the standalone
    ``COMMENT ON COLUMN``. ``SET NOT NULL`` is rendered here for a different
    reason: sqlglot's DuckDB generator logs "Unsupported ALTER COLUMN syntax"
    for it while emitting SQL DuckDB does accept, and a warning about a
    statement that works only teaches the reader to ignore the ones that
    matter. The remaining forms (``SET DATA TYPE``, ``DROP NOT NULL``,
    ``SET``/``DROP DEFAULT``) generate SQL DuckDB accepts as they stand,
    quietly.
    """
    table = expression.this.sql(dialect="duckdb")
    column = exp.to_identifier(action.name).sql(dialect="duckdb")

    comment = action.args.get("comment")
    if comment is not None:
        text = _comment_literal(comment)
        return f"COMMENT ON COLUMN {table}.{column} IS {text}" if text else None

    if action.args.get("allow_null") is False and not action.args.get("drop"):
        return f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"

    return None


def _swap_tables(expression: exp.Alter, action: exp.SwapTable) -> str:
    """`ALTER TABLE a SWAP WITH b` -> the three renames that exchange them."""
    left = expression.this
    right = action.this
    if not isinstance(left, exp.Table) or not isinstance(right, exp.Table):
        return expression.sql(dialect="duckdb")

    left_sql = left.sql(dialect="duckdb")
    right_sql = right.sql(dialect="duckdb")
    # The temporary name lives in the same schema as the left-hand table so the
    # renames stay within one catalog.
    staging = left.copy()
    staging.set("this", exp.to_identifier(f"_snowduck_swap_{left.name}"))
    staging_sql = staging.sql(dialect="duckdb")
    bare_staging = staging.name
    bare_right = right.name

    return (
        f"ALTER TABLE {left_sql} RENAME TO {bare_staging}; "
        f"ALTER TABLE {right_sql} RENAME TO {left.name}; "
        f"ALTER TABLE {staging_sql} RENAME TO {bare_right}"
    )


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


def claims_command(expression: exp.Command) -> bool:
    """Whether SnowDuck re-reads this opaque Command itself.

    sqlglot warns whenever it degrades a statement to a Command. For the ones
    handled here that warning is noise, so the cursor holds it back - and asks
    this to know which those are.
    """
    if parse_show(expression) is not None:
        return True
    text = expression.sql(dialect="snowflake")
    keyword = text.split(None, 1)[0].upper() if text else ""
    if keyword in ("UNSET", "LIST", "LS", "REMOVE", "RM"):
        return True
    return keyword == "ALTER" and _multi_column_comment(text) is not None


def transform_command(expression: exp.Command, context: DialectContext) -> str:
    """Statements sqlglot parsed as opaque commands.

    ``SHOW`` and the stage commands are claimed here; everything else keeps its
    previous behaviour of being handed to DuckDB verbatim.
    """
    if parse_show(expression) is not None:
        return transform_show(expression, context)

    text = expression.sql(dialect="snowflake")
    keyword = text.split(None, 1)[0].upper() if text else ""
    if keyword == "UNSET":
        name = text.split(None, 1)[1].strip().rstrip(";").upper() if " " in text else ""
        context.session_variables.pop(name, None)
        return SQL_NOOP
    if keyword in ("LIST", "LS"):
        return _list_stage(text)
    if keyword in ("REMOVE", "RM"):
        return _remove_stage_file(text)
    if keyword == "ALTER":
        rendered = _multi_column_comment(text)
        if rendered is not None:
            return rendered

    return expression.sql(dialect="duckdb")


#: `ALTER TABLE <rel> ALTER [COLUMN] <col> COMMENT <text> [, ...]` - Snowflake's
#: multi-column form, which sqlglot degrades to an opaque Command. dbt-snowflake
#: emits exactly this for `persist_docs` on a model with more than one
#: documented column, so without it every such run failed at the DDL.
_ALTER_COLUMN_COMMENTS_HEAD = re.compile(
    r"^ALTER\s+(?:TABLE|VIEW)?\s*(?P<relation>(?:\"[^\"]+\"|[\w$]+)"
    r"(?:\.(?:\"[^\"]+\"|[\w$]+)){0,2})\s+ALTER\s+(?P<actions>.+?)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)

_ALTER_COLUMN_COMMENT = re.compile(
    r"(?:COLUMN\s+)?(?P<column>\"[^\"]+\"|[\w$]+)\s+COMMENT\s*=?\s*"
    r"(?P<text>\$\$.*?\$\$|\'(?:[^\']|\'\')*\')",
    re.IGNORECASE | re.DOTALL,
)


def _multi_column_comment(text: str) -> str | None:
    """The multi-column ALTER COLUMN ... COMMENT form, as COMMENT ON statements.

    Returns None for any other ALTER, so an unrecognised one keeps its previous
    behaviour of being handed to DuckDB rather than being silently swallowed.
    """
    head = _ALTER_COLUMN_COMMENTS_HEAD.match(text.strip())
    if head is None:
        return None

    actions = head.group("actions")
    statements: list[str] = []
    consumed = 0
    for match in _ALTER_COLUMN_COMMENT.finditer(actions):
        # Everything between two actions must be just a separator; anything
        # else means this is a form with clauses that would be dropped.
        if actions[consumed : match.start()].strip(" \t\n,") != "":
            return None
        consumed = match.end()
        column = exp.to_identifier(match.group("column").strip('"')).sql(
            dialect="duckdb"
        )
        statements.append(
            f"COMMENT ON COLUMN {head.group('relation')}.{column} IS "
            f"{sql_literal(_comment_text(match.group('text')))}"
        )

    if not statements or actions[consumed:].strip(" \t\n,;") != "":
        return None
    return "; ".join(statements)


def _comment_text(literal: str) -> str:
    """The text inside a `$$ ... $$` or `\'...\'` comment literal."""
    if literal.startswith("$$"):
        return literal[2:-2]
    return literal[1:-1].replace("''", "'")


_STAGE_REF = re.compile(r"@(?P<stage>[^/\s]+)(?:/(?P<path>\S*))?", re.IGNORECASE)


def _stage_location(text: str) -> tuple[str, str] | None:
    match = _STAGE_REF.search(text)
    if not match:
        return None
    stage = match.group("stage").strip('"').upper()
    return stage, (match.group("path") or "")


def _list_stage(text: str) -> str:
    """`LIST @stage` -> the files PUT has placed there."""
    location = _stage_location(text)
    if location is None:
        return SQL_NOOP
    stage, prefix = location
    directory = os.path.join(stage_root(), stage)

    rows = []
    for root, _dirs, files in os.walk(directory):
        for filename in sorted(files):
            full = os.path.join(root, filename)
            relative = os.path.relpath(full, directory)
            if prefix and not relative.startswith(prefix):
                continue
            size = os.path.getsize(full)
            rows.append(
                f"(  '{stage}/{relative}', {size}, "
                f"'{_file_md5(full)}', TO_TIMESTAMP(0)::TIMESTAMPTZ)"
            )

    columns = "(name, size, md5, last_modified)"
    if not rows:
        return (
            "SELECT NULL::VARCHAR AS name, NULL::BIGINT AS size, "
            "NULL::VARCHAR AS md5, NULL::TIMESTAMPTZ AS last_modified WHERE FALSE"
        )
    return f"SELECT * FROM (VALUES {', '.join(rows)}) AS t{columns}"


def _file_md5(path: str) -> str:
    digest = hashlib.md5()  # noqa: S324 - a stage listing checksum, not a secret
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _remove_stage_file(text: str) -> str:
    """`REMOVE @stage/path` -> delete the staged file or directory."""
    location = _stage_location(text)
    if location is None:
        return SQL_NOOP
    stage, path = location
    target = (
        os.path.join(stage_root(), stage, path)
        if path
        else os.path.join(stage_root(), stage)
    )
    removed = 0
    if os.path.isdir(target):
        removed = sum(len(files) for _root, _dirs, files in os.walk(target))
        shutil.rmtree(target, ignore_errors=True)
    elif os.path.exists(target):
        os.remove(target)
        removed = 1
    return (
        f"SELECT '{stage}/{path}' AS name, "
        f"'{'removed' if removed else 'not found'}' AS result"
    )


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
