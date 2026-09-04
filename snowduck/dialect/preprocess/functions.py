"""Snowflake scalar functions that have no direct DuckDB equivalent.

Each entry is a small builder that rewrites one Snowflake function call into
DuckDB expressions. Registering them in one table (rather than as branches of a
long ``elif`` chain) keeps the supported surface introspectable - see
:func:`supported_functions`, which the documentation is generated from.
"""

from typing import Callable, cast

import sqlglot
from sqlglot import exp

from ..context import DialectContext

Builder = Callable[[list[exp.Expression], DialectContext], exp.Expression | None]

_BUILDERS: dict[str, Builder] = {}
_CATEGORIES: dict[str, str] = {}


def _register(category: str, *names: str) -> Callable[[Builder], Builder]:
    def decorate(fn: Builder) -> Builder:
        for name in names:
            _BUILDERS[name] = fn
            _CATEGORIES[name] = category
        return fn

    return decorate


def supported_functions() -> dict[str, list[str]]:
    """Snowflake functions emulated by this module, grouped by category."""
    grouped: dict[str, list[str]] = {}
    for name, category in sorted(_CATEGORIES.items()):
        grouped.setdefault(category, []).append(name)
    return grouped


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _call(name: str, *args: exp.Expression | None) -> exp.Anonymous:
    return exp.Anonymous(this=name, expressions=[a for a in args if a is not None])


def _str(value: str) -> exp.Literal:
    return exp.Literal.string(value)


def _num(value: int) -> exp.Expression:
    return exp.Literal.number(value)


def _template(duckdb_sql: str, **substitutions: exp.Expression) -> exp.Expression:
    """Build an expression from a DuckDB SQL snippet with named placeholders.

    Placeholders are bare identifiers (``_SUBJECT``) replaced by the given
    expressions. Useful where the DuckDB equivalent needs a lambda or a CASE
    that would be tedious to assemble node by node.
    """
    tree: exp.Expression = cast(
        exp.Expression, sqlglot.parse_one(duckdb_sql, read="duckdb")
    )
    for node in list(tree.find_all(exp.Column)):
        replacement = substitutions.get(node.name)
        if replacement is not None:
            node.replace(replacement.copy())
    return tree


def _json_path(key: exp.Expression) -> exp.Expression:
    """Build a JSON path for a member name, quoting it so dots are literal."""
    if isinstance(key, exp.Literal) and key.is_string:
        return _str(f'$."{key.this}"')
    return exp.DPipe(
        this=_str('$."'), expression=exp.DPipe(this=key, expression=_str('"'))
    )


def _json_type_is(value: exp.Expression, *types: str) -> exp.Expression:
    json_type = _call("json_type", value)
    if len(types) == 1:
        return exp.EQ(this=json_type, expression=_str(types[0]))
    return exp.In(this=json_type, expressions=[_str(t) for t in types])


def _truthy(value: exp.Expression) -> exp.Expression:
    return exp.Cast(this=value, to=exp.DataType.build("BOOLEAN"))


# ---------------------------------------------------------------------------
# Semi-structured
# ---------------------------------------------------------------------------


@_register("Semi-structured", "OBJECT_AGG")
def _object_agg(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    if len(args) != 2:
        return None
    return _call("json_group_object", *args)


@_register("Semi-structured", "OBJECT_PICK")
def _object_pick(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """OBJECT_PICK(obj, k...) keeps only the named keys."""
    if len(args) < 2:
        return None
    obj, keys = args[0], args[1:]
    pairs: list[exp.Expression] = []
    for key in keys:
        pairs.append(key)
        pairs.append(_call("json_extract", obj, _json_path(key)))
    # json_merge_patch onto {} drops keys whose value came back NULL, i.e. keys
    # that were not present on the source object.
    return _call("json_merge_patch", _str("{}"), _call("json_object", *pairs))


@_register("Semi-structured", "OBJECT_DELETE")
def _object_delete(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """OBJECT_DELETE(obj, k...) removes the named keys.

    RFC 7386 merge-patch gives a null value the meaning "delete this key", so
    patching with ``{"k": null}`` is exactly the operation wanted.
    """
    if len(args) < 2:
        return None
    obj, keys = args[0], args[1:]
    pairs: list[exp.Expression] = []
    for key in keys:
        pairs.append(key)
        pairs.append(exp.Null())
    return _call("json_merge_patch", obj, _call("json_object", *pairs))


@_register("Semi-structured", "ARRAY_FLATTEN")
def _array_flatten(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    if len(args) != 1:
        return None
    return _call("flatten", args[0])


@_register("Semi-structured", "IS_ARRAY")
def _is_array(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    return _json_type_is(args[0], "ARRAY") if len(args) == 1 else None


@_register("Semi-structured", "IS_OBJECT")
def _is_object(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    return _json_type_is(args[0], "OBJECT") if len(args) == 1 else None


@_register("Semi-structured", "IS_NULL_VALUE")
def _is_null_value(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """TRUE for a JSON null - distinct from a SQL NULL, which yields NULL."""
    if len(args) != 1:
        return None
    return exp.Case(
        ifs=[exp.If(this=exp.Is(this=args[0], expression=exp.Null()), true=exp.Null())],
        default=_json_type_is(args[0], "NULL"),
    )


@_register("Semi-structured", "IS_BOOLEAN")
def _is_boolean(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    return _json_type_is(args[0], "BOOLEAN") if len(args) == 1 else None


@_register("Semi-structured", "IS_VARCHAR", "IS_CHAR")
def _is_varchar(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    return _json_type_is(args[0], "VARCHAR") if len(args) == 1 else None


@_register("Semi-structured", "IS_INTEGER")
def _is_integer(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    if len(args) != 1:
        return None
    return _json_type_is(args[0], "BIGINT", "UBIGINT", "INTEGER")


@_register("Semi-structured", "IS_DOUBLE", "IS_DECIMAL", "IS_REAL")
def _is_double(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    if len(args) != 1:
        return None
    return _json_type_is(args[0], "DOUBLE", "BIGINT", "UBIGINT", "INTEGER")


def _as_type(type_name: str) -> Builder:
    def build(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
        if len(args) != 1:
            return None
        return exp.TryCast(this=args[0], to=exp.DataType.build(type_name))

    return build


def _cast_to(type_name: str, *, safe: bool = False) -> Builder:
    """TO_<type>(x) / TRY_TO_<type>(x) -> a cast.

    sqlglot's Snowflake parser only models the timestamp variants when the
    argument is a literal; anything else - `to_timestamp_ntz(convert_timezone(
    'UTC', current_timestamp()))`, which is how dbt stamps its snapshots -
    arrives here as an anonymous call.
    """

    def build(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
        if not args:
            return None
        node = exp.TryCast if safe else exp.Cast
        return node(this=args[0], to=exp.DataType.build(type_name))

    return build


for _name, _target, _safe in (
    ("TO_TIMESTAMP_NTZ", "TIMESTAMP", False),
    ("TO_TIMESTAMP_LTZ", "TIMESTAMPTZ", False),
    ("TO_TIMESTAMP_TZ", "TIMESTAMPTZ", False),
    ("TRY_TO_TIMESTAMP_NTZ", "TIMESTAMP", True),
    ("TRY_TO_TIMESTAMP_LTZ", "TIMESTAMPTZ", True),
    ("TRY_TO_TIMESTAMP_TZ", "TIMESTAMPTZ", True),
    # TO_TIME only casts; DuckDB has no TIMESTAMPTZ -> TIME cast, so a
    # timestamp argument is not covered here.
    ("TO_TIME", "TIME", False),
    ("TRY_TO_TIME", "TIME", True),
):
    _BUILDERS[_name] = _cast_to(_target, safe=_safe)
    _CATEGORIES[_name] = "Conversion"


_BUILDERS["AS_INTEGER"] = _as_type("BIGINT")
_BUILDERS["AS_DOUBLE"] = _as_type("DOUBLE")
_BUILDERS["AS_DECIMAL"] = _as_type("DECIMAL(38, 0)")
_BUILDERS["AS_NUMBER"] = _as_type("DECIMAL(38, 0)")
_BUILDERS["AS_BOOLEAN"] = _as_type("BOOLEAN")
_BUILDERS["AS_DATE"] = _as_type("DATE")
_BUILDERS["AS_TIMESTAMP_NTZ"] = _as_type("TIMESTAMP")
for _n in (
    "AS_INTEGER",
    "AS_DOUBLE",
    "AS_DECIMAL",
    "AS_NUMBER",
    "AS_BOOLEAN",
    "AS_DATE",
    "AS_TIMESTAMP_NTZ",
):
    _CATEGORIES[_n] = "Semi-structured"


@_register("Semi-structured", "AS_VARCHAR", "AS_CHAR")
def _as_varchar(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """Unwrap a VARIANT string without its JSON quotes."""
    if len(args) != 1:
        return None
    return _call("json_extract_string", args[0], _str("$"))


@_register("Semi-structured", "TO_OBJECT")
def _to_object(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    if len(args) != 1:
        return None
    return exp.Cast(this=args[0], to=exp.DataType.build("JSON"))


# ---------------------------------------------------------------------------
# String
# ---------------------------------------------------------------------------


@_register("String", "INSERT")
def _insert(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    """INSERT(base, pos, len, insert) splices `insert` into `base`."""
    if len(args) != 4:
        return None
    base, pos, length, ins = args
    head = _call("substr", base, _num(1), exp.Sub(this=pos, expression=_num(1)))
    tail = _call("substr", base, exp.Add(this=pos, expression=length))
    return exp.DPipe(this=exp.DPipe(this=head, expression=ins), expression=tail)


@_register("String", "RTRIMMED_LENGTH")
def _rtrimmed_length(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    if len(args) != 1:
        return None
    return _call("length", _call("rtrim", args[0]))


@_register("String", "JAROWINKLER_SIMILARITY")
def _jarowinkler(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """Snowflake reports similarity as an integer percentage (0-100)."""
    if len(args) != 2:
        return None
    ratio = _call("jaro_winkler_similarity", *args)
    scaled = exp.Mul(this=ratio, expression=_num(100))
    return exp.Cast(this=_call("round", scaled), to=exp.DataType.build("BIGINT"))


@_register("String", "TRY_BASE64_DECODE_STRING")
def _try_base64_decode(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    if not args:
        return None
    return _call("try", _call("decode", _call("from_base64", args[0])))


@_register("String", "TRY_HEX_DECODE_STRING")
def _try_hex_decode(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    if not args:
        return None
    return _call("try", _call("decode", _call("unhex", args[0])))


# Regexes for the URL components Snowflake's PARSE_URL reports.
_URL_PARTS = {
    "scheme": r"^([^:]+)://",
    "host": r"^[^:]+://([^/?#:]+)",
    "port": r"^[^:]+://[^/?#:]*:([0-9]+)",
    "path": r"^[^:]+://[^/?#]*/([^?#]*)",
    "query": r"\?([^#]*)",
    "fragment": r"#(.*)$",
}

# `parameters` is the query string split into an object. Empty pairs are
# dropped so a trailing '&' does not produce a blank key.
_URL_PARAMETERS_TEMPLATE = (
    "CAST('{' || coalesce(list_aggregate(list_filter("
    "  list_transform(str_split(_QUERY, '&'), p -> CASE WHEN p = '' THEN NULL ELSE"
    "    to_json(url_decode(split_part(p, '=', 1))) || ':' ||"
    "    to_json(url_decode(split_part(p, '=', 2))) END),"
    "  x -> x IS NOT NULL), 'string_agg', ','), '') || '}' AS JSON)"
)


@_register("String", "PARSE_URL")
def _parse_url(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """PARSE_URL(url) -> an object with the URL's components.

    Percent-escapes are decoded, and the query string is also exposed as a
    `parameters` object, as Snowflake does.
    """
    if not args:
        return None
    url = args[0]

    def part(pattern: str) -> exp.Expression:
        return _call(
            "url_decode", _call("regexp_extract", url.copy(), _str(pattern), _num(1))
        )

    query = _call("regexp_extract", url.copy(), _str(_URL_PARTS["query"]), _num(1))

    pairs: list[exp.Expression] = []
    for name, pattern in _URL_PARTS.items():
        pairs.append(_str(name))
        pairs.append(part(pattern))
    pairs.append(_str("parameters"))
    pairs.append(_template(_URL_PARAMETERS_TEMPLATE, _QUERY=query))

    return _call("json_object", *pairs)


# ---------------------------------------------------------------------------
# Date & time
# ---------------------------------------------------------------------------

# DuckDB's dayofweek(): Sunday = 0 ... Saturday = 6.
_WEEKDAYS = {
    "su": 0,
    "sun": 0,
    "sunday": 0,
    "mo": 1,
    "mon": 1,
    "monday": 1,
    "tu": 2,
    "tue": 2,
    "tuesday": 2,
    "we": 3,
    "wed": 3,
    "wednesday": 3,
    "th": 4,
    "thu": 4,
    "thursday": 4,
    "fr": 5,
    "fri": 5,
    "friday": 5,
    "sa": 6,
    "sat": 6,
    "saturday": 6,
}


def _weekday_index(arg: exp.Expression) -> exp.Expression | None:
    if isinstance(arg, exp.Literal) and arg.is_string:
        index = _WEEKDAYS.get(str(arg.this).strip().lower())
        return _num(index) if index is not None else None
    # Dynamic day name: look it up by comparing the first two letters.
    ifs = [
        exp.If(
            this=exp.EQ(
                this=_call("lower", _call("substr", arg, _num(1), _num(2))),
                expression=_str(abbrev),
            ),
            true=_num(index),
        )
        for abbrev, index in sorted(
            {k: v for k, v in _WEEKDAYS.items() if len(k) == 2}.items()
        )
    ]
    return exp.Case(ifs=ifs, default=exp.Null())


def _shift_to_weekday(
    date: exp.Expression, target: exp.Expression, forward: bool
) -> exp.Expression:
    """Days to the next/previous occurrence of a weekday, never zero."""
    today = _call("dayofweek", date)
    difference = (
        exp.Sub(this=target, expression=today)
        if forward
        else exp.Sub(this=today, expression=target)
    )
    # The parentheses matter: `a - b + 7 % 7` binds as `a - b + (7 % 7)`.
    raw = exp.Mod(
        this=exp.Paren(this=exp.Add(this=difference, expression=_num(7))),
        expression=_num(7),
    )
    # A delta of 0 means "same weekday": Snowflake moves a full week.
    delta = exp.Case(
        ifs=[exp.If(this=exp.EQ(this=raw, expression=_num(0)), true=_num(7))],
        default=raw,
    )
    interval = exp.Interval(this=delta, unit=exp.Var(this="DAY"))
    if forward:
        return exp.Cast(
            this=exp.Add(this=date, expression=interval), to=exp.DataType.build("DATE")
        )
    return exp.Cast(
        this=exp.Sub(this=date, expression=interval), to=exp.DataType.build("DATE")
    )


@_register("Date & time", "NEXT_DAY")
def _next_day(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    if len(args) != 2:
        return None
    target = _weekday_index(args[1])
    return _shift_to_weekday(args[0], target, forward=True) if target else None


@_register("Date & time", "PREVIOUS_DAY")
def _previous_day(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    if len(args) != 2:
        return None
    target = _weekday_index(args[1])
    return _shift_to_weekday(args[0], target, forward=False) if target else None


@_register("Date & time", "DAYNAME")
def _dayname(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    """Snowflake returns the three-letter abbreviation, e.g. 'Mon'."""
    if len(args) != 1:
        return None
    return _call("strftime", args[0], _str("%a"))


@_register("Date & time", "MONTHNAME")
def _monthname(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """Snowflake returns the three-letter abbreviation, e.g. 'Jan'."""
    if len(args) != 1:
        return None
    return _call("strftime", args[0], _str("%b"))


@_register("Date & time", "TIME_SLICE")
def _time_slice(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """TIME_SLICE(ts, n, unit[, 'START'|'END']) rounds down to an n-unit bucket."""
    if len(args) < 3:
        return None
    ts, count, unit = args[0], args[1], args[2]
    if not (isinstance(unit, exp.Literal) and unit.is_string):
        return None
    interval = exp.Interval(this=count, unit=exp.Var(this=str(unit.this).upper()))
    bucket = _call("time_bucket", interval, ts)
    if len(args) > 3 and isinstance(args[3], exp.Literal):
        if str(args[3].this).upper() == "END":
            return exp.Add(this=bucket, expression=interval)
    return bucket


@_register("Date & time", "GETDATE")
def _getdate(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    """GETDATE() is Snowflake's alias for CURRENT_TIMESTAMP."""
    return exp.CurrentTimestamp() if not args else None


@_register("Date & time", "SYSDATE")
def _sysdate(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    """SYSDATE() is the current time in UTC, without a timezone."""
    if args:
        return None
    return exp.Cast(
        this=_call("timezone", _str("UTC"), exp.CurrentTimestamp()),
        to=exp.DataType.build("TIMESTAMP"),
    )


@_register("Date & time", "YEAROFWEEK", "YEAROFWEEKISO")
def _yearofweek(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    if len(args) != 1:
        return None
    return _call("isoyear", args[0])


# ---------------------------------------------------------------------------
# Boolean & aggregate
# ---------------------------------------------------------------------------


@_register("Conditional", "BOOLAND")
def _booland(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    if len(args) != 2:
        return None
    return exp.And(this=_truthy(args[0]), expression=_truthy(args[1]))


@_register("Conditional", "BOOLOR")
def _boolor(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    if len(args) != 2:
        return None
    return exp.Or(this=_truthy(args[0]), expression=_truthy(args[1]))


@_register("Conditional", "BOOLXOR")
def _boolxor(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    if len(args) != 2:
        return None
    return exp.NEQ(this=_truthy(args[0]), expression=_truthy(args[1]))


@_register("Conditional", "BOOLNOT")
def _boolnot(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    if len(args) != 1:
        return None
    return exp.Not(this=exp.Paren(this=_truthy(args[0])))


@_register("String", "INITCAP")
def _initcap(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    """INITCAP(str, delimiters): capitalise each character that starts a word."""
    if len(args) != 2:
        return None
    return _template(
        "list_aggregate("
        "  list_transform("
        "    generate_series(1, length(_SUBJECT)),"
        "    i -> CASE WHEN i = 1 OR contains(_DELIMS, _SUBJECT[i - 1])"
        "              THEN upper(_SUBJECT[i]) ELSE lower(_SUBJECT[i]) END"
        "  ), 'string_agg', '')",
        _SUBJECT=args[0],
        _DELIMS=args[1],
    )


@_register("Aggregate", "HLL", "HLL_ESTIMATE")
def _hll(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    if not args:
        return None
    return _call("approx_count_distinct", args[0])


# ---------------------------------------------------------------------------
# Session context
# ---------------------------------------------------------------------------

SERVER_VERSION = "9.8.1"


def _context_literal(getter: Callable[[DialectContext], str]) -> Builder:
    def build(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
        return _str(getter(ctx))

    return build


_CONTEXT_FUNCTIONS: dict[str, Callable[[DialectContext], str]] = {
    "CURRENT_VERSION": lambda ctx: SERVER_VERSION,
    "CURRENT_ACCOUNT": lambda ctx: "SD4711",
    "CURRENT_ACCOUNT_NAME": lambda ctx: "SNOWDUCK",
    "CURRENT_ORGANIZATION_NAME": lambda ctx: "SNOWDUCK",
    "CURRENT_CLIENT": lambda ctx: f"SnowDuck {SERVER_VERSION}",
    "CURRENT_SESSION": lambda ctx: "4711",
    "CURRENT_REGION": lambda ctx: "AWS_US_EAST_1",
    "CURRENT_IP_ADDRESS": lambda ctx: "127.0.0.1",
    "CURRENT_TRANSACTION": lambda ctx: "4711",
    "CURRENT_STATEMENT": lambda ctx: "",
}

for _name, _getter in _CONTEXT_FUNCTIONS.items():
    _BUILDERS[_name] = _context_literal(_getter)
    _CATEGORIES[_name] = "Context"


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def preprocess_functions(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Rewrite registered Snowflake functions into DuckDB expressions."""
    if isinstance(expression, exp.Initcap) and expression.expression is not None:
        rewritten = _initcap([expression.this, expression.expression], context)
        return rewritten if rewritten is not None else expression

    if isinstance(expression, exp.Hll):
        counted = _hll(list(expression.expressions) or [expression.this], context)
        return counted if counted is not None else expression

    if isinstance(expression, exp.ParseUrl):
        parsed = _parse_url([expression.this], context)
        return parsed if parsed is not None else expression

    if isinstance(expression, exp.Stuff):
        # Snowflake INSERT(base, pos, len, insert) parses to exp.Stuff, which
        # DuckDB has no equivalent for.
        spliced = _insert(
            [
                expression.this,
                expression.args["start"],
                expression.args["length"],
                expression.expression,
            ],
            context,
        )
        return spliced if spliced is not None else expression

    if isinstance(expression, exp.Collate):
        return _collate(expression)

    if isinstance(expression, exp.Window):
        rewritten = _rewrite_window(expression)
        if rewritten is not None:
            return rewritten
        return expression

    if not isinstance(expression, exp.Anonymous) or not isinstance(
        expression.this, str
    ):
        return expression

    builder = _BUILDERS.get(expression.this.upper())
    if builder is None:
        return expression

    replacement = builder(list(expression.expressions), context)
    return replacement if replacement is not None else expression


def _collate(expression: exp.Collate) -> exp.Expression:
    """COLLATE(expr, spec) - only the case/accent-insensitive specifiers.

    Snowflake collation specifiers look like `en-ci` (case-insensitive) or
    `en-ci-ai` (also accent-insensitive). Case-insensitivity is emulated by
    folding to lower case, which gives the right answer for comparison,
    grouping and ordering. Any other specifier is dropped - locale-specific
    collation is not emulated.
    """
    spec = expression.expression
    if isinstance(spec, exp.Literal) and spec.is_string:
        parts = {p.strip().lower() for p in str(spec.this).split("-")}
        if "ci" in parts or "lower" in parts:
            return _call("lower", expression.this)
        if "upper" in parts:
            return _call("upper", expression.this)
    return cast(exp.Expression, expression.this)


def _rewrite_window(window: exp.Window) -> exp.Expression | None:
    """Snowflake window functions expressed as ordinary aggregates in DuckDB."""
    func = window.this
    if not isinstance(func, exp.Anonymous) or not isinstance(func.this, str):
        return None

    name = func.this.upper()
    args = list(func.expressions)

    if name == "RATIO_TO_REPORT" and len(args) == 1:
        # RATIO_TO_REPORT(x) OVER w  ->  x / SUM(x) OVER w
        total = window.copy()
        total.set("this", exp.Sum(this=args[0].copy()))
        return exp.Div(this=args[0], expression=exp.Paren(this=total))

    if name == "CONDITIONAL_TRUE_EVENT" and len(args) == 1:
        # Counts how many times the condition has been true up to this row.
        counter = exp.Sum(
            this=exp.Case(
                ifs=[exp.If(this=args[0], true=_num(1))],
                default=_num(0),
            )
        )
        rewritten = window.copy()
        rewritten.set("this", counter)
        return rewritten

    return None
