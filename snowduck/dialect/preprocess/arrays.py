"""Coercion of ARRAY arguments for DuckDB's list functions.

SnowDuck models Snowflake's ``ARRAY`` as DuckDB ``JSON``, because Snowflake
arrays are heterogeneous and DuckDB lists are not. DuckDB's list functions,
however, only accept a real ``T[]``, so every array function applied to a
*stored* ``ARRAY`` column failed to bind:

    CREATE TABLE t (tags ARRAY);
    SELECT ARRAY_SIZE(tags) FROM t;
    -- Binder Error: No function matches 'array_length(JSON)'

This pass inserts the cast that makes those calls bind. ``CAST(x AS JSON[])``
turns both a JSON array and a native list into a list, so it works whichever
form the argument arrived in.

An argument that is *statically* a list already - an array literal or
``ARRAY_CONSTRUCT(...)`` - is left alone, so its elements keep their own types
instead of each becoming a JSON value.
"""

from sqlglot import exp

from ..context import DialectContext

_JSON_LIST = exp.DataType.build("JSON[]")
_DOUBLE_LIST = exp.DataType.build("DOUBLE[]")
_TEXT_LIST = exp.DataType.build("VARCHAR[]")

# Array functions, and which of their arguments hold an array. sqlglot
# normalises these so `this` is the array whatever order Snowflake wrote the
# call in; `expression`/`expressions` are named only where a second array is
# involved.
_ARRAY_ARGS: dict[type[exp.Expression], tuple[str, ...]] = {
    exp.ArrayAppend: ("this",),
    exp.ArrayCompact: ("this",),
    exp.ArrayConcat: ("this", "expressions"),
    exp.ArrayContains: ("this",),
    exp.ArrayDistinct: ("this",),
    exp.ArrayExcept: ("this", "expression"),
    exp.ArrayIntersect: ("expressions",),
    exp.ArrayOverlaps: ("this", "expression"),
    exp.ArrayPosition: ("this",),
    exp.ArrayPrepend: ("this",),
    exp.ArrayReverse: ("this",),
    exp.ArraySize: ("this",),
    exp.ArraySlice: ("this",),
    exp.Flatten: ("this",),
    exp.GetExtract: ("this",),
    exp.SortArray: ("this",),
}

# Numeric aggregations: the elements must come back as numbers rather than
# JSON text, or MIN/MAX would compare lexicographically and rank '10' below '2'.
_NUMERIC_ARRAY_ARGS: dict[type[exp.Expression], tuple[str, ...]] = {
    exp.ArrayMax: ("this",),
    exp.ArrayMin: ("this",),
    exp.ArraySum: ("this",),
}

# ARRAY_TO_STRING joins the element *values*, so its elements must come back as
# plain text rather than JSON - otherwise every string would keep its quotes.
_TEXT_ARRAY_ARGS: dict[type[exp.Expression], tuple[str, ...]] = {
    exp.ArrayToString: ("this",),
}


def _is_mixed_literal_array(array: exp.Array) -> bool:
    """True when a DuckDB list literal would reject these elements.

    Snowflake arrays are heterogeneous; a DuckDB list literal must be
    homogeneous, so `ARRAY_CONSTRUCT(1, 'two')` fails to convert. Mixing
    string and non-string literals is the case that actually breaks.
    """
    kinds = set()
    for item in array.expressions:
        if isinstance(item, exp.Literal):
            kinds.add("string" if item.is_string else "number")
        elif isinstance(item, exp.Boolean):
            kinds.add("boolean")
    return len(kinds) > 1


# Functions that search an array for a value: the value must be converted to
# JSON alongside the array.
_SEARCH_VALUE_ARGS: dict[type[exp.Expression], str] = {
    exp.ArrayContains: "expression",
    exp.ArrayPosition: "expression",
}


def _jsonify_arg(node: exp.Expression, key: str) -> None:
    value = node.args.get(key)
    if isinstance(value, exp.Expression) and not (
        isinstance(value, exp.Anonymous)
        and isinstance(value.this, str)
        and value.this.lower() == "to_json"
    ):
        node.set(key, exp.Anonymous(this="to_json", expressions=[value.copy()]))


def _needs_coercion(value: object) -> bool:
    if not isinstance(value, exp.Expression):
        return False
    if isinstance(value, exp.Array):
        # A homogeneous array literal is already a native list. A mixed one is
        # rewritten to json_array below, so it does need the cast.
        return _is_mixed_literal_array(value)
    if isinstance(value, exp.Unnest):
        return False
    if isinstance(value, exp.Cast) and value.to in (
        _JSON_LIST,
        _DOUBLE_LIST,
        _TEXT_LIST,
    ):
        return False
    return True


def _is_member_lookup(node: exp.GetExtract) -> bool:
    """True for `GET(object, 'key')` rather than `GET(array, index)`."""
    key = node.expression
    return isinstance(key, exp.Literal) and key.is_string


def _as_list(value: exp.Expression, target: exp.DataType) -> exp.Expression:
    """Cast an array argument to a DuckDB list.

    The value goes through `to_json` first. Casting a list straight to JSON[]
    re-parses each element as JSON, which fails outright on a native list of
    plain strings - `ARRAY_SIZE(SPLIT('a,b', ','))` died with "Malformed JSON".
    `to_json` normalises both representations to a JSON array first, so the
    cast then works whichever form the argument arrived in.
    """
    source = exp.Anonymous(this="to_json", expressions=[value.copy()])
    return exp.Cast(this=source, to=target.copy())


def _coerce_arg(node: exp.Expression, key: str, target: exp.DataType) -> None:
    value = node.args.get(key)

    if isinstance(value, list):
        if any(_needs_coercion(item) for item in value):
            node.set(
                key,
                [
                    _as_list(item, target) if _needs_coercion(item) else item
                    for item in value
                ],
            )
        return

    if _needs_coercion(value) and isinstance(value, exp.Expression):
        node.set(key, _as_list(value, target))


def preprocess_arrays(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Cast ARRAY arguments so DuckDB's list functions accept them."""
    if isinstance(expression, exp.Array) and _is_mixed_literal_array(expression):
        # Build JSON directly rather than a list literal, which cannot hold
        # elements of different types.
        return exp.Anonymous(this="json_array", expressions=expression.expressions)

    if isinstance(expression, exp.GetExtract) and _is_member_lookup(expression):
        # GET(object, 'key') is not an array operation, so coercing its subject
        # to a list would turn the object into a one-element array.
        return expression

    for table, target in (
        (_ARRAY_ARGS, _JSON_LIST),
        (_NUMERIC_ARRAY_ARGS, _DOUBLE_LIST),
        (_TEXT_ARRAY_ARGS, _TEXT_LIST),
    ):
        keys = table.get(type(expression))
        if keys is None:
            continue

        coerced = any(_needs_coercion(expression.args.get(key)) for key in keys)
        for key in keys:
            _coerce_arg(expression, key, target)

        if coerced and type(expression) in _SEARCH_VALUE_ARGS:
            # The array's elements are now JSON values, so the value being
            # searched for has to be one too - otherwise DuckDB tries to read
            # a bare string as JSON and fails on "Malformed JSON".
            _jsonify_arg(expression, _SEARCH_VALUE_ARGS[type(expression)])

        return expression

    return expression
