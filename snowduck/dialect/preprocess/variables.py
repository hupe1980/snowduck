"""Variable substitution preprocessing."""

from sqlglot import exp

from ..context import DialectContext


def preprocess_variables(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """
    Substitute $var placeholders with their values from session variables.

    Snowflake: SELECT $my_var WHERE id = $filter_id
    DuckDB: SELECT 'hello' WHERE id = 1
    """
    # Check if this is a Parameter node (SQLGlot uses this for $var in Snowflake)
    if isinstance(expression, exp.Parameter):
        # Parameter has this=Var(this=var_name)
        if isinstance(expression.this, exp.Var):
            var_name = (
                expression.this.this.upper()
                if isinstance(expression.this.this, str)
                else expression.this.name.upper()
            )
            if var_name in context.session_variables:
                value = context.session_variables[var_name]
                # Try to determine if it's a number or string
                try:
                    # Try integer
                    int_val = int(value)
                    return exp.Literal.number(int_val)
                except ValueError:
                    try:
                        # Try float
                        float_val = float(value)
                        return exp.Literal.number(float_val)
                    except ValueError:
                        # It's a string
                        return exp.Literal.string(value)
            else:
                raise ValueError(f"Undefined session variable: ${var_name}")

    # Also check Placeholder (in case syntax varies)
    # But skip anonymous ? placeholders - those are bind parameters, not session variables
    if isinstance(expression, exp.Placeholder):
        # Anonymous ? placeholders have name='?' and this=None
        # These are bind parameter placeholders for parameterized queries
        if expression.name == "?" or expression.this is None:
            return expression

        var_name = expression.name.upper()
        if var_name in context.session_variables:
            value = context.session_variables[var_name]
            try:
                int_val = int(value)
                return exp.Literal.number(int_val)
            except ValueError:
                try:
                    float_val = float(value)
                    return exp.Literal.number(float_val)
                except ValueError:
                    return exp.Literal.string(value)
        else:
            raise ValueError(f"Undefined session variable: ${var_name}")

    return expression
