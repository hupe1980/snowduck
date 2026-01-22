"""Date functions preprocessing - automatic casting of string literals to DATE.

DuckDB requires explicit type casts for string literals in date functions,
while Snowflake performs implicit conversion. This module adds the necessary
CAST expressions to ensure compatibility.
"""

import re
from sqlglot import exp

from ..context import DialectContext


def _looks_like_date(value: str) -> bool:
    """Check if a string literal looks like a date."""
    # Common date patterns: YYYY-MM-DD, YYYY/MM/DD, DD-MON-YYYY, etc.
    patterns = [
        r"^\d{4}-\d{2}-\d{2}$",  # 2024-01-15
        r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}",  # 2024-01-15T12:00 or 2024-01-15 12:00
        r"^\d{4}/\d{2}/\d{2}$",  # 2024/01/15
        r"^\d{2}-[A-Z]{3}-\d{4}$",  # 15-JAN-2024
    ]
    return any(re.match(p, value, re.IGNORECASE) for p in patterns)


def _cast_to_date(expr: exp.Expression) -> exp.Expression:
    """Wrap expression in CAST to DATE if it's a string literal that looks like a date."""
    if isinstance(expr, exp.Literal) and expr.is_string:
        if _looks_like_date(expr.this):
            return exp.Cast(this=expr, to=exp.DataType.build("DATE"))
    return expr


def _cast_to_timestamp(expr: exp.Expression) -> exp.Expression:
    """Wrap expression in CAST to TIMESTAMP if it's a string literal that looks like a date/time."""
    if isinstance(expr, exp.Literal) and expr.is_string:
        if _looks_like_date(expr.this):
            return exp.Cast(this=expr, to=exp.DataType.build("TIMESTAMP"))
    return expr


def preprocess_date_functions(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Transform date functions to cast string literals to DATE.
    
    Snowflake implicitly converts string literals to DATE/TIMESTAMP in date functions,
    but DuckDB requires explicit CAST. This preprocessor adds the necessary casts.
    """
    
    # Date extraction functions - all take a date/timestamp as first arg
    # exp.Year, exp.Month, exp.Day, exp.DayOfWeek, exp.DayOfYear, 
    # exp.Week, exp.WeekOfYear, exp.Quarter
    if isinstance(expression, (
        exp.Year, exp.Month, exp.Day, exp.DayOfWeek, exp.DayOfYear,
        exp.Week, exp.WeekOfYear, exp.Quarter
    )):
        if expression.this:
            expression.set("this", _cast_to_date(expression.this))
        return expression
    
    # LastDay - takes date as first arg
    if isinstance(expression, exp.LastDay):
        if expression.this:
            expression.set("this", _cast_to_date(expression.this))
        return expression
    
    # MonthsBetween - takes two dates
    if isinstance(expression, exp.MonthsBetween):
        if expression.this:
            expression.set("this", _cast_to_date(expression.this))
        if expression.expression:
            expression.set("expression", _cast_to_date(expression.expression))
        return expression
    
    # AddMonths - date is first arg (handled specially later because of interval transform)
    if isinstance(expression, exp.AddMonths):
        if expression.this:
            expression.set("this", _cast_to_date(expression.this))
        return expression
    
    # TimestampTrunc (DATE_TRUNC) - second arg is the date/timestamp
    if isinstance(expression, exp.TimestampTrunc):
        if expression.this:
            expression.set("this", _cast_to_timestamp(expression.this))
        return expression
    
    # TimeTrunc - second arg is the time
    if isinstance(expression, exp.TimeTrunc):
        if expression.this:
            expression.set("this", _cast_to_timestamp(expression.this))
        return expression
    
    # DateDiff - needs special handling for time-based units (hour, minute, second)
    # to cast arguments to TIMESTAMP instead of DATE
    if isinstance(expression, exp.DateDiff):
        unit = expression.args.get("unit")
        unit_str = ""
        if isinstance(unit, exp.Var):
            unit_str = unit.this.upper()
        elif isinstance(unit, exp.Literal):
            unit_str = unit.this.upper()
        
        # For time-based units, cast to TIMESTAMP to preserve time component
        time_units = ("HOUR", "MINUTE", "SECOND", "MILLISECOND", "MICROSECOND", "NANOSECOND")
        if unit_str in time_units:
            if expression.this:
                expression.set("this", _cast_to_timestamp(expression.this))
            if expression.expression:
                expression.set("expression", _cast_to_timestamp(expression.expression))
        else:
            # For date-based units, cast to DATE
            if expression.this:
                expression.set("this", _cast_to_date(expression.this))
            if expression.expression:
                expression.set("expression", _cast_to_date(expression.expression))
        return expression
    
    # Extract - date is the expression arg
    if isinstance(expression, exp.Extract):
        if expression.expression:
            expression.set("expression", _cast_to_date(expression.expression))
        return expression
    
    # Handle Anonymous functions for HOUR, MINUTE, SECOND, DAYNAME, MONTHNAME, etc.
    if isinstance(expression, exp.Anonymous):
        fname = expression.this.upper()
        
        # Date part extraction functions
        if fname in ("HOUR", "MINUTE", "SECOND"):
            args = list(expression.expressions)
            if args and isinstance(args[0], exp.Literal) and args[0].is_string:
                if _looks_like_date(args[0].this):
                    args[0] = _cast_to_timestamp(args[0])
                    expression.set("expressions", args)
            return expression
        
        # Date name functions
        if fname in ("DAYNAME", "MONTHNAME"):
            args = list(expression.expressions)
            if args and isinstance(args[0], exp.Literal) and args[0].is_string:
                if _looks_like_date(args[0].this):
                    args[0] = _cast_to_date(args[0])
                    expression.set("expressions", args)
            return expression
        
        # NEXT_DAY, PREVIOUS_DAY - first arg is date
        if fname in ("NEXT_DAY", "PREVIOUS_DAY"):
            args = list(expression.expressions)
            if args and isinstance(args[0], exp.Literal) and args[0].is_string:
                if _looks_like_date(args[0].this):
                    args[0] = _cast_to_date(args[0])
                    expression.set("expressions", args)
            return expression
        
        # TIME_SLICE - first arg is date/timestamp
        if fname == "TIME_SLICE":
            args = list(expression.expressions)
            if args and isinstance(args[0], exp.Literal) and args[0].is_string:
                if _looks_like_date(args[0].this):
                    args[0] = _cast_to_timestamp(args[0])
                    expression.set("expressions", args)
            return expression
    
    return expression
