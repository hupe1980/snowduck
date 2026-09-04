"""INFORMATION_SCHEMA rewriting.

Snowflake gives every database its own INFORMATION_SCHEMA. DuckDB's is
account-wide and has a different column set, so SnowDuck keeps Snowflake-shaped
views in a hidden schema per database (see
``snowduck/info_schema/database_information_schema.sql``) and rewrites
references onto them here.
"""

from sqlglot import exp

from ..context import DialectContext

#: INFORMATION_SCHEMA view -> the backing view's name in the hidden schema.
#: Views not listed here are left alone and surface as a catalog error naming
#: the view, which is more useful than silently returning nothing.
_DATABASE_SCOPED_VIEWS = {
    "SCHEMATA": "_schemata",
    "TABLES": "_tables",
    "COLUMNS": "_columns",
    "VIEWS": "_views",
    "FUNCTIONS": "_functions",
    "SEQUENCES": "_sequences",
}

#: DATABASES is account-wide even in Snowflake, so it has one backing view.
_ACCOUNT_SCOPED_VIEWS = {"DATABASES": "_DATABASES"}

#: DuckDB's own catalogs. `system.information_schema.columns` is DuckDB's real
#: information schema, not a Snowflake one - rewriting it would point at a view
#: that does not exist there, and it is the only way to see the underlying
#: DuckDB types.
_INTERNAL_CATALOGS = frozenset({"system", "temp", "memory"})


def preprocess_info_schema(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Point INFORMATION_SCHEMA references at SnowDuck's backing views."""
    if not isinstance(expression, exp.Table):
        return expression

    if expression.catalog and expression.catalog.lower() in _INTERNAL_CATALOGS:
        return expression

    if expression.db:
        # <db>.INFORMATION_SCHEMA.<view>
        if expression.db.upper() != "INFORMATION_SCHEMA":
            return expression
    elif not (
        context.current_schema
        and context.current_schema.upper() == "INFORMATION_SCHEMA"
    ):
        # USE SCHEMA INFORMATION_SCHEMA, then an unqualified view name.
        return expression

    view = expression.name.upper()
    manager = context.info_schema_manager

    if view in _ACCOUNT_SCOPED_VIEWS:
        catalog = manager.account_catalog_name
        target = _ACCOUNT_SCOPED_VIEWS[view]
    elif view in _DATABASE_SCOPED_VIEWS:
        catalog = expression.catalog or context.current_database or ""
        target = _DATABASE_SCOPED_VIEWS[view]
        if not catalog:
            return expression
    else:
        return expression

    # Mutated in place rather than replaced so the table's alias, sample clause
    # and join context survive the rewrite.
    expression.set("catalog", exp.Identifier(this=catalog, quoted=False))
    expression.set("db", exp.Identifier(this=manager.info_schema_name, quoted=False))
    expression.set("this", exp.Identifier(this=target, quoted=False))
    return expression
