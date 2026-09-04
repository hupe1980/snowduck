"""Snowflake's numeric format model for ``TO_CHAR`` / ``TO_VARCHAR``.

``TO_CHAR(1234.5, '9,999.99')`` is ``' 1,234.50'`` in Snowflake: the digits are
grouped, padded to two decimals, and right-justified in a field as wide as the
model plus one column for the sign. Neither DuckDB nor sqlglot models any of
that - sqlglot logs *"Argument 'format' is not supported for expression
'ToChar'"* and drops the model, so the call silently returned the unformatted
number.

The model is a compile-time constant in practice, so it is interpreted here
rather than at run time: the parsed model decides which DuckDB expression to
build, and DuckDB's own ``format`` does the digit grouping and rounding.

Supported elements, from Snowflake's `Number Formats` table:

======  =====================================================================
``0``   digit position, padded with a leading zero
``9``   digit position, leading zeros suppressed
``.``   decimal point (``D`` is a synonym)
``,``   group separator (``G`` is a synonym)
``$``   currency symbol
``S``   sign, leading or trailing, ``+`` for a positive value
``MI``  trailing minus sign, a blank for a positive value
``PR``  a negative value in angle brackets
``B``   a zero value as the empty string
``TM``  "text minimum" - the shortest representation, unpadded
======  =====================================================================

A value too wide for the model's integer positions renders as ``#`` repeated to
the model's width, as Snowflake does.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import cast

import sqlglot
from sqlglot import exp

from ..context import DialectContext

#: Every character a numeric model may contain. A format made only of these -
#: and holding at least one digit position - is a number format; anything else
#: is a date/time format, which sqlglot's DuckDB generator already handles.
_MODEL_CHARS = set("09,.$SBGDMIPRTMEXsbgdmiprtmex")

_TEXT_MINIMUM = re.compile(r"^TM[9E]?$", re.IGNORECASE)


def is_number_format(model: str) -> bool:
    """Whether a ``TO_CHAR`` format string is a numeric model.

    The two families share the argument position and are told apart by their
    alphabet: a date model needs ``Y``, ``H``, ``:`` or ``/``, none of which a
    numeric model may contain.
    """
    if not model:
        return False
    if _TEXT_MINIMUM.match(model):
        return True
    if not set(model) <= _MODEL_CHARS:
        return False
    return any(character in "09" for character in model)


@dataclass(frozen=True)
class NumberFormat:
    """A parsed Snowflake numeric format model."""

    integer_digits: int
    decimal_digits: int
    grouped: bool
    zero_padded: bool
    currency: bool
    #: One of "", "leading", "trailing", "mi", "pr".
    sign: str
    blank_when_zero: bool
    text_minimum: bool
    width: int


def parse_number_format(model: str) -> NumberFormat | None:
    """Interpret a numeric model, or None if it uses nothing SnowDuck models."""
    if _TEXT_MINIMUM.match(model):
        return NumberFormat(0, 0, False, False, False, "", False, True, 0)

    body = model.upper()
    sign = ""
    blank_when_zero = False

    if body.endswith("MI"):
        sign, body = "mi", body[:-2]
    elif body.endswith("PR"):
        sign, body = "pr", body[:-2]
    elif body.endswith("S"):
        sign, body = "trailing", body[:-1]
    elif body.startswith("S"):
        sign, body = "leading", body[1:]

    if body.startswith("B"):
        blank_when_zero, body = True, body[1:]

    currency = "$" in body
    body = body.replace("$", "")

    # `D` and `G` are the locale-independent spellings of `.` and `,`.
    body = body.replace("D", ".").replace("G", ",")

    if not body or set(body) - set("09,."):
        return None

    integer_part, _, decimal_part = body.partition(".")
    integer_digits = sum(character in "09" for character in integer_part)
    decimal_digits = sum(character in "09" for character in decimal_part)
    if not integer_digits and not decimal_digits:
        return None

    # The field is as wide as the model, plus a column for the sign unless the
    # model places one itself.
    width = len(model)
    if sign == "":
        width += 1
    if blank_when_zero:
        width -= 1

    return NumberFormat(
        integer_digits=integer_digits,
        decimal_digits=decimal_digits,
        grouped="," in integer_part,
        zero_padded="0" in integer_part,
        currency=currency,
        sign=sign,
        blank_when_zero=blank_when_zero,
        text_minimum=False,
        width=width,
    )


def _substitute(template: str, **bindings: exp.Expression) -> exp.Expression:
    """Build an expression from DuckDB SQL with ``_NAME`` placeholders."""
    tree = cast(exp.Expression, sqlglot.parse_one(template, read="duckdb"))
    for node in list(tree.find_all(exp.Column)):
        replacement = bindings.get(node.name)
        if replacement is not None:
            node.replace(replacement.copy())
    return tree


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def build(value: exp.Expression, model: str) -> exp.Expression | None:
    """``TO_CHAR(value, model)`` as a DuckDB expression, or None if unmodelled."""
    parsed = parse_number_format(model)
    if parsed is None:
        return None

    if parsed.text_minimum:
        # "Text minimum": the shortest representation, with no padding at all.
        return exp.Cast(this=value, to=exp.DataType.build("VARCHAR"))

    decimals = parsed.decimal_digits
    grouping = "," if parsed.grouped else ""
    # Rounding happens once, in DECIMAL, so a value that rounds up into an extra
    # integer digit is caught by the overflow test below rather than silently
    # widening the field.
    rounded = _substitute(
        f"ROUND(ABS(CAST(_VALUE AS DECIMAL(38, {min(decimals + 6, 30)}))), {decimals})",
        _VALUE=value,
    )

    if parsed.zero_padded:
        # Adding 10^integer_digits forces every leading position to be printed,
        # and the extra leading 1 - which grouping treats as just another digit,
        # so the separators land where the model puts them - is then dropped.
        offset = 10**parsed.integer_digits
        digits = _substitute(
            f"SUBSTR(FORMAT('{{:{grouping}.{decimals}f}}', "
            f"CAST({offset} AS DECIMAL(38, {decimals})) + _ROUNDED), 2)",
            _ROUNDED=rounded,
        )
    else:
        digits = _substitute(
            f"FORMAT('{{:{grouping}.{decimals}f}}', _ROUNDED)", _ROUNDED=rounded
        )
        if decimals:
            # A `9` in the units position suppresses a lone leading zero:
            # TO_CHAR(0.5, '99.9') is '   .5', not '  0.5'.
            digits = _substitute(
                "REGEXP_REPLACE(_DIGITS, '^0\\.', '.')", _DIGITS=digits
            )

    body = digits
    if parsed.currency:
        body = _substitute("'$' || _BODY", _BODY=body)

    negative = _substitute("_VALUE < 0", _VALUE=value)
    if parsed.sign == "leading":
        signed = _substitute(
            "(CASE WHEN _NEGATIVE THEN '-' ELSE '+' END) || _BODY",
            _NEGATIVE=negative,
            _BODY=body,
        )
    elif parsed.sign == "trailing":
        signed = _substitute(
            "_BODY || (CASE WHEN _NEGATIVE THEN '-' ELSE '+' END)",
            _NEGATIVE=negative,
            _BODY=body,
        )
    elif parsed.sign == "mi":
        signed = _substitute(
            "_BODY || (CASE WHEN _NEGATIVE THEN '-' ELSE ' ' END)",
            _NEGATIVE=negative,
            _BODY=body,
        )
    elif parsed.sign == "pr":
        signed = _substitute(
            "CASE WHEN _NEGATIVE THEN '<' || _BODY || '>' ELSE ' ' || _BODY || ' ' END",
            _NEGATIVE=negative,
            _BODY=body,
        )
    else:
        signed = _substitute(
            "(CASE WHEN _NEGATIVE THEN '-' ELSE '' END) || _BODY",
            _NEGATIVE=negative,
            _BODY=body,
        )

    # The *rounded* value is what has to fit: TO_CHAR(999.6, '999') overflows,
    # because it renders as 1000.
    overflow = _substitute(f"_ROUNDED >= {10**parsed.integer_digits}", _ROUNDED=rounded)
    zero_case = "WHEN _VALUE = 0 THEN '' " if parsed.blank_when_zero else ""
    return _substitute(
        "CASE WHEN _VALUE IS NULL THEN NULL "
        + zero_case
        + f"WHEN _OVERFLOW THEN {_literal('#' * parsed.width)} "
        f"ELSE LPAD(_SIGNED, {parsed.width}, ' ') END",
        _VALUE=value,
        _OVERFLOW=overflow,
        _SIGNED=signed,
    )


def preprocess_number_formats(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Render ``TO_CHAR(<number>, '<model>')`` under Snowflake's format model."""
    if not isinstance(expression, exp.ToChar):
        return expression

    model = expression.args.get("format")
    if not (isinstance(model, exp.Literal) and model.is_string):
        return expression
    if not is_number_format(str(model.this)):
        return expression

    rendered = build(expression.this, str(model.this))
    return rendered if rendered is not None else expression
