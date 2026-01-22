"""Information schema preprocessing."""

from sqlglot import exp

from ..context import DialectContext


def preprocess_info_schema(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Transform INFORMATION_SCHEMA references to internal schema."""
    if isinstance(expression, exp.Table):
        # Check if this is an information_schema table reference
        is_info_schema = False

        # Case 1: Explicitly referenced as db.information_schema.table
        if expression.db and expression.db.upper() == "INFORMATION_SCHEMA":
            is_info_schema = True
        # Case 2: Current schema is information_schema (USE SCHEMA information_schema)
        elif (
            not expression.db
            and context.current_schema
            and context.current_schema.upper() == "INFORMATION_SCHEMA"
        ):
            is_info_schema = True

        if is_info_schema and expression.name.upper() == "DATABASES":
            # Use the account-level information schema database
            account_db = context.info_schema_manager.account_catalog_name
            info_schema = context.info_schema_manager.info_schema_name

            return exp.Table(
                this=exp.Identifier(this="_DATABASES", quoted=False),
                db=exp.Identifier(
                    this=f"{account_db}.{info_schema}",
                    quoted=False,
                ),
            )

    return expression
