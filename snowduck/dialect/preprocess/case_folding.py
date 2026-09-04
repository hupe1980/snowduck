"""Snowflake identifier case folding.

Snowflake resolves an unquoted identifier by folding it to upper case, so
``CREATE TABLE my_model`` creates ``MY_MODEL`` and every later reference -
however it was typed - finds it. DuckDB instead stores identifiers as written
and matches them case-insensitively.

The difference is invisible until something reads names back out of the
catalog. dbt-snowflake does exactly that: it lists a schema with ``SHOW
OBJECTS``, then looks the relation up under the upper-cased name its own
Relation renders to. Against a catalog holding ``raw_customers`` that lookup
finds only an "approximate match" and the run stops.

So the fold happens here, once, before anything else looks at the tree.
Quoted identifiers keep their case, as in Snowflake.
"""

from sqlglot import exp

from ..context import DialectContext


def preprocess_case_folding(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Upper-case every unquoted identifier."""
    if isinstance(expression, exp.Identifier) and not expression.quoted:
        name = expression.this
        if isinstance(name, str):
            expression.set("this", name.upper())
    return expression
