"""GENERATOR and sequence functions preprocessing."""

from sqlglot import exp

from ..context import DialectContext


def preprocess_generator(expression: exp.Expression, context: DialectContext) -> exp.Expression:
    """Pre-process GENERATOR table functions into DuckDB generate_series."""

    # sqlglot parses TABLE(GENERATOR(...)) as TableFromRows containing the Anonymous function.
    if isinstance(expression, exp.TableFromRows):
        if isinstance(expression.this, exp.Anonymous) and expression.this.this.upper() == "GENERATOR":
             gen_expr = expression.this
             rowcount = None
             
             # Extract ROWCOUNT
             for arg in gen_expr.expressions:
                 if isinstance(arg, exp.Kwarg) and arg.this.name.upper() == "ROWCOUNT":
                     rowcount = arg.expression
                 elif isinstance(arg, exp.PropertyEQ) and arg.this.name.upper() == "ROWCOUNT":
                     rowcount = arg.expression
             
             if rowcount:
                 # DuckDB uses generate_series(start, stop)
                 return exp.Anonymous(this="generate_series", expressions=[exp.Literal.number(1), rowcount])

    return expression


def preprocess_seq_functions(expression: exp.Expression, context: DialectContext) -> exp.Expression:
    """Transform sequence generation functions like SEQ4() to DuckDB equivalents."""
    
    if isinstance(expression, exp.Anonymous):
        fname = expression.this.upper()
        if fname in ("SEQ1", "SEQ2", "SEQ4", "SEQ8"):
             # Transform to ROW_NUMBER() OVER () - 1
             window = exp.Window(this=exp.Anonymous(this="row_number"), over=exp.WindowSpec())
             return exp.Sub(this=window, expression=exp.Literal.number(1))
             
        elif fname == "UNIFORM":
             # UNIFORM(min, max, seed) -> floor(random() * (max - min + 1) + min)
             args = expression.expressions
             if len(args) >= 2:
                 min_val = args[0]
                 max_val = args[1]
                 range_len = exp.Add(this=exp.Sub(this=max_val, expression=min_val), expression=exp.Literal.number(1))
                 rand_scaled = exp.Mul(this=exp.Anonymous(this="random"), expression=range_len)
                 shifted = exp.Add(this=exp.Floor(this=rand_scaled), expression=min_val)
                 return exp.Cast(this=shifted, to=exp.DataType.build("int"))

    return expression
