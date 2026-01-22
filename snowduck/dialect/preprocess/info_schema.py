"""Information schema preprocessing."""

from sqlglot import exp

from ..context import DialectContext


def preprocess_info_schema(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Transform INFORMATION_SCHEMA references to internal schema."""
    if (
        isinstance(expression, exp.Table)
        and (
            expression.db.upper() == "INFORMATION_SCHEMA"
            or (
                context.current_schema
                and context.current_schema.upper() == "INFORMATION_SCHEMA"
            )
        )
        and expression.name.upper() == "DATABASES"
    ):
        return exp.Table(
            this=exp.Identifier(this="_DATABASES", quoted=False),
            db=exp.Identifier(
                this=f"{context.current_database}.{context.info_schema_manager.info_schema_name}",
                quoted=False,
            ),
        )

    return expression
