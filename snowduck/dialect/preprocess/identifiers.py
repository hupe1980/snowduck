"""Identifier and OBJECT/ARRAY_CONSTRUCT preprocessing."""

from sqlglot import exp

from ..context import DialectContext


def preprocess_identifier(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Convert identifier function to an identifier.

    See https://docs.snowflake.com/en/sql-reference/identifier-literal
    """
    if (
        isinstance(expression, exp.Anonymous)
        and isinstance(expression.this, str)
        and expression.this.upper() == "IDENTIFIER"
    ):
        expression = exp.Identifier(this=expression.expressions[0].this, quoted=False)

    return expression


def preprocess_semi_structured(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Pre-process expression to transform OBJECT_CONSTRUCT/ARRAY_CONSTRUCT and strip Time Travel."""

    # Handle OBJECT_CONSTRUCT parsed as Struct (Snowflake dialect does this)
    if isinstance(expression, exp.Struct):
        # Convert Struct(PropertyEQ(k, v), ...) to json_object(k, v, ...)
        new_args = []
        possible = True
        for e in expression.expressions:
            if isinstance(e, exp.PropertyEQ):
                new_args.append(e.this)
                new_args.append(e.expression)
            else:
                possible = False
                break

        if possible and new_args:
            return exp.Anonymous(this="json_object", expressions=new_args)

    # Handle OBJECT_CONSTRUCT(*) parsed as StarMap
    if isinstance(expression, exp.StarMap):
        # map(*) equivalent -> to_json(row(*))
        # row(*) creates a struct with all cols
        return exp.Anonymous(
            this="to_json",
            expressions=[exp.Anonymous(this="row", expressions=[exp.Star()])],
        )

    # Handle explicit function calls (e.g. ARRAY_CONSTRUCT is parsed as exp.Array usually?)
    if isinstance(expression, exp.Anonymous):
        fname = expression.this.upper()
        if fname == "OBJECT_CONSTRUCT":
            return exp.Anonymous(this="json_object", expressions=expression.expressions)
        elif fname == "ARRAY_CONSTRUCT":
            return exp.Array(expressions=expression.expressions)

    # Handle Time Travel: FROM table AT(...) -> stored in 'when' arg as HistoricalData
    if isinstance(expression, exp.Table):
        if expression.args.get("when"):
            expression.set("when", None)

    return expression
