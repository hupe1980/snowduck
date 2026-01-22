import json
import os

from sqlglot import exp

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


def transform_create(expression: exp.Create, context: DialectContext) -> str:
    """Custom transformation for CREATE DATABASE/SCHEMA to use uppercase identifiers."""
    kind = str(expression.args.get("kind")).upper()

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

    if kind == "SCHEMA":
        ident = expression.find(exp.Identifier)
        if ident and not ident.quoted:
            # Uppercase unquoted schema names to match Snowflake behavior
            ident.set("this", ident.this.upper())

    return expression.sql(dialect="duckdb")


def transform_describe(expression: exp.Describe, context: DialectContext) -> str:
    if str(expression.args.get("kind")).upper() in ("TABLE", "VIEW"):
        if table := expression.find(exp.Table):
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

            if (
                schema
                and schema.upper() == context.info_schema_manager.info_schema_name
            ):
                return context.info_schema_manager.describe_info_schema_sql(
                    view=f"{schema}.{table.name}"
                )

            return context.info_schema_manager.describe_table_sql(
                database=database, schema=schema, table=table.name
            )

    return expression.sql(dialect="duckdb")


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


def transform_show(expression: exp.Show, context: DialectContext) -> str:
    """Transform SHOW commands to DuckDB-compatible SQL."""
    if isinstance(expression.this, str) and expression.this.upper() == "DATABASES":
        return context.info_schema_manager.show_databases_sql()

    if isinstance(expression.this, str) and expression.this.upper() == "SCHEMAS":
        database = context.current_database or ""
        if expression.args.get("scope_kind") == "DATABASE":
            scope = expression.args.get("scope")
            if isinstance(scope, exp.Table) and isinstance(scope.this, exp.Identifier):
                database = scope.this.name
        return context.info_schema_manager.show_schemas_sql(database=database)

    if isinstance(expression.this, str) and expression.this.upper() == "OBJECTS":
        database = context.current_database or ""
        schema = context.current_schema or ""
        if expression.args.get("scope_kind") == "SCHEMA":
            scope = expression.args.get("scope")
            if isinstance(scope, exp.Table) and isinstance(scope.this, exp.Identifier):
                schema = scope.this.name
                if isinstance(scope.db, exp.Identifier):
                    database = scope.db.name
        return context.info_schema_manager.show_objects_sql(
            database=database, schema=schema
        )

    return expression.sql(dialect="duckdb")


def transform_lateral(expression: exp.Lateral, context: DialectContext) -> str:
    """Transform LATERAL FLATTEN/EXPLODE into DuckDB UNNEST."""
    if isinstance(expression.this, exp.Explode):
        kwarg = expression.this.args.get("this")
        input_expr = kwarg.expression if isinstance(kwarg, exp.Kwarg) else kwarg
        input_sql = (
            input_expr.sql(dialect="duckdb") if input_expr is not None else "NULL"
        )

        alias = expression.args.get("alias")
        alias_name = (
            alias.this.sql(dialect="duckdb") if alias and alias.this else "_flattened"
        )
        return f"LATERAL UNNEST({input_sql}) AS {alias_name}(VALUE)"

    return expression.sql(dialect="duckdb")


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


def transform_current_session_info(
    expression: exp.Select, context: DialectContext
) -> str:
    """Transform session-related functions to DuckDB-compatible SQL. Returns original SQL if no transformations occur."""
    transformed = False
    select_expressions = []

    for projection in expression.expressions:
        transformed_expr = None
        alias_name = None
        expr = projection

        # Handle cases where projection is an Alias with a Func or special current_* expressions
        if isinstance(projection, exp.Alias):
            expr = projection.this
            alias_name = projection.alias
            if alias_name is None:
                alias_sql = None
            elif isinstance(alias_name, str):
                alias_sql = alias_name
            else:
                alias_sql = alias_name.sql(dialect="duckdb")
            current_database_cls = getattr(exp, "CurrentDatabase", None)
            current_schema_cls = getattr(exp, "CurrentSchema", None)
            if current_database_cls and isinstance(expr, current_database_cls):
                database = context.current_database or ""
                target_alias = alias_sql or "DATABASE"
                transformed_expr = f"'{database}' AS {target_alias}"
            elif current_schema_cls and isinstance(expr, current_schema_cls):
                schema = context.current_schema or ""
                target_alias = alias_sql or "SCHEMA"
                transformed_expr = f"'{schema}' AS {target_alias}"
            elif isinstance(expr, exp.Func):
                func_name = expr.name.upper()

                if func_name == "CURRENT_ROLE":
                    role = context.current_role or "SYSADMIN"
                    target_alias = alias_sql or "ROLE"
                    transformed_expr = f"'{role}' AS {target_alias}"
                elif func_name == "CURRENT_SECONDARY_ROLES":
                    roles_json = json.dumps({"roles": "", "value": "ALL"})
                    target_alias = alias_sql or "SECONDARY_ROLES"
                    transformed_expr = f"'{roles_json}' AS {target_alias}"
                elif func_name == "CURRENT_DATABASE":
                    database = context.current_database or ""
                    target_alias = alias_sql or "DATABASE"
                    transformed_expr = f"'{database}' AS {target_alias}"
                elif func_name == "CURRENT_SCHEMA":
                    schema = context.current_schema or ""
                    target_alias = alias_sql or "SCHEMA"
                    transformed_expr = f"'{schema}' AS {target_alias}"
                elif func_name == "CURRENT_WAREHOUSE":
                    warehouse = context.current_warehouse or "DEFAULT_WAREHOUSE"
                    target_alias = alias_sql or "WAREHOUSE"
                    transformed_expr = f"'{warehouse}' AS {target_alias}"
        else:
            current_database_cls = getattr(exp, "CurrentDatabase", None)
            current_schema_cls = getattr(exp, "CurrentSchema", None)
            if current_database_cls and isinstance(expr, current_database_cls):
                database = context.current_database or ""
                transformed_expr = f"'{database}'"
            elif current_schema_cls and isinstance(expr, current_schema_cls):
                schema = context.current_schema or ""
                transformed_expr = f"'{schema}'"
            elif isinstance(expr, exp.Func):
                func_name = expr.name.upper()
                if func_name == "CURRENT_ROLE":
                    role = context.current_role or "SYSADMIN"
                    transformed_expr = f"'{role}'"
                elif func_name == "CURRENT_SECONDARY_ROLES":
                    roles_json = json.dumps({"roles": "", "value": "ALL"})
                    transformed_expr = f"'{roles_json}'"
                elif func_name == "CURRENT_DATABASE":
                    database = context.current_database or ""
                    transformed_expr = f"'{database}'"
                elif func_name == "CURRENT_SCHEMA":
                    schema = context.current_schema or ""
                    transformed_expr = f"'{schema}'"
                elif func_name == "CURRENT_WAREHOUSE":
                    warehouse = context.current_warehouse or "DEFAULT_WAREHOUSE"
                    transformed_expr = f"'{warehouse}'"

        # Add transformed expression or fallback to DuckDB SQL
        if transformed_expr:
            transformed = True
            select_expressions.append(transformed_expr)
        else:
            select_expressions.append(projection.sql(dialect="duckdb"))

    # Return original SQL if no transformation occurred
    if not transformed:
        return expression.sql(dialect="duckdb")

    return f"SELECT {', '.join(select_expressions)}"
