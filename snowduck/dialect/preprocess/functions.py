"""Snowflake scalar functions that have no direct DuckDB equivalent.

Each entry is a small builder that rewrites one Snowflake function call into
DuckDB expressions. Registering them in one table (rather than as branches of a
long ``elif`` chain) keeps the supported surface introspectable - see
:func:`supported_functions`, which the documentation is generated from.
"""

from typing import Any, Callable, cast

import sqlglot
from sqlglot import exp

from ..context import DialectContext

Builder = Callable[[list[exp.Expression], DialectContext], exp.Expression | None]

_BUILDERS: dict[str, Builder] = {}
_CATEGORIES: dict[str, str] = {}

# sqlglot promotes functions from a generic Anonymous call to a dedicated node
# as its Snowflake coverage grows, and a name-only registry silently stops
# firing when that happens. Registering the node class alongside the name keeps
# a builder working across those upgrades.
_NODE_BUILDERS: dict[type[exp.Func], Builder] = {}

#: Builders that need the node itself rather than its argument list, because
#: something other than an argument decides the translation - sqlglot folds
#: TO_BINARY and TRY_TO_BINARY into one node with a `safe` flag, for instance.
NodeBuilder = Callable[[Any, DialectContext], "exp.Expression | None"]
_TYPED_BUILDERS: dict[type[exp.Func], NodeBuilder] = {}


def _node_class(name: str) -> type[exp.Func] | None:
    """The sqlglot node a Snowflake function name maps to, if there is one.

    Only `exp.Func` subclasses are accepted: `INSERT` would otherwise derive to
    `exp.Insert`, the DML statement, and hijack every INSERT in the tree.
    """
    candidate = _NODE_ALIASES.get(
        name, "".join(part.capitalize() for part in name.split("_"))
    )
    node = getattr(exp, candidate, None)
    if isinstance(node, type) and issubclass(node, exp.Func):
        return node
    return None


def _positional_args(node: exp.Expression) -> list[exp.Expression]:
    """A function node's arguments in call order."""
    args: list[exp.Expression] = []
    for key in node.arg_types or ():
        value = node.args.get(key)
        if isinstance(value, list):
            args.extend(item for item in value if isinstance(item, exp.Expression))
        elif isinstance(value, exp.Expression):
            args.append(value)
    return args


def _add_builder(category: str, name: str, fn: Builder) -> None:
    """Register a builder under both its Snowflake name and its sqlglot node."""
    _BUILDERS[name] = fn
    _CATEGORIES[name] = category
    node = _node_class(name)
    if node is not None:
        _NODE_BUILDERS[node] = fn


#: Snowflake names whose sqlglot node is not the CamelCase of the name, so
#: `_node_class` cannot find it. Without this the builder only fires for a call
#: sqlglot left as Anonymous - which, for these, it never does.
_NODE_ALIASES: dict[str, str] = {
    "OCTET_LENGTH": "ByteLength",
}


def _register(category: str, *names: str) -> Callable[[Builder], Builder]:
    def decorate(fn: Builder) -> Builder:
        for name in names:
            _add_builder(category, name, fn)
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


#: JSON types that stand in for each Snowflake VARIANT type. DuckDB reports a
#: JSON value's type through `json_type`, and its vocabulary is narrower than
#: Snowflake's: every integer is UBIGINT/BIGINT and there is no DATE, TIME,
#: TIMESTAMP or BINARY at all - those arrive as strings.
_JSON_TYPES: dict[str, tuple[str, ...]] = {
    "ARRAY": ("ARRAY",),
    "OBJECT": ("OBJECT",),
    "BOOLEAN": ("BOOLEAN",),
    "VARCHAR": ("VARCHAR",),
    "INTEGER": ("BIGINT", "UBIGINT", "INTEGER"),
    "NUMERIC": ("DOUBLE", "BIGINT", "UBIGINT", "INTEGER"),
}


def _as_type(type_name: str, json_type: str | None = None) -> Builder:
    """AS_<type>(v) - the value if the VARIANT holds that type, else NULL.

    Snowflake's AS_ family *tests* the stored type rather than coercing to it:
    `AS_INTEGER(PARSE_JSON('"5"'))` is NULL, not 5. That test is only possible
    for the types JSON itself distinguishes; DATE, TIME, TIMESTAMP and BINARY
    are stored as strings, so those stay a plain TRY_CAST.
    """
    types = _JSON_TYPES.get(json_type or "")

    def build(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
        if not args:
            return None
        # AS_DECIMAL(v, precision [, scale]) states the target type in its
        # trailing arguments rather than in its name.
        target = _decimal_type(args[1:]) if type_name.startswith("DECIMAL") else None
        cast = exp.TryCast(this=args[0], to=exp.DataType.build(target or type_name))
        if types is None:
            return cast
        return exp.Case(
            ifs=[exp.If(this=_json_type_is(args[0].copy(), *types), true=cast)]
        )

    return build


def _decimal_type(arguments: list[exp.Expression]) -> str | None:
    """`DECIMAL(p, s)` from AS_DECIMAL's trailing precision/scale arguments."""
    digits: list[int] = []
    for argument in arguments[:2]:
        if not isinstance(argument, exp.Literal) or argument.is_string:
            return None
        try:
            digits.append(int(argument.this))
        except (TypeError, ValueError):
            return None
    if not digits:
        return None
    precision = digits[0]
    scale = digits[1] if len(digits) > 1 else 0
    return f"DECIMAL({precision}, {scale})"


def _is_type(json_type: str) -> Builder:
    """IS_<type>(v) - whether the VARIANT holds that type."""
    types = _JSON_TYPES[json_type]

    def build(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
        return _json_type_is(args[0], *types) if len(args) == 1 else None

    return build


def _is_castable_string(type_name: str) -> Builder:
    """IS_DATE/IS_TIME/IS_TIMESTAMP_*/IS_BINARY(v).

    JSON has no such types, so a VARIANT carrying one is a string. The nearest
    honest test is that it is a string *and* reads back as that type.
    """

    def build(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
        if len(args) != 1:
            return None
        text = _call("json_extract_string", args[0].copy(), _str("$"))
        return exp.And(
            this=_json_type_is(args[0], "VARCHAR"),
            expression=exp.Not(
                this=exp.Is(
                    this=exp.TryCast(this=text, to=exp.DataType.build(type_name)),
                    expression=exp.Null(),
                )
            ),
        )

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
    _add_builder("Conversion", _name, _cast_to(_target, safe=_safe))


# The VARIANT accessors. `json_type` is the storage's own type tag, so the ones
# JSON models are checked; the rest (DATE, TIME, TIMESTAMP, BINARY - all stored
# as strings) fall back to a TRY_CAST.
for _name, _target, _json in (
    ("AS_INTEGER", "BIGINT", "INTEGER"),
    ("AS_DOUBLE", "DOUBLE", "NUMERIC"),
    ("AS_REAL", "DOUBLE", "NUMERIC"),
    ("AS_DECIMAL", "DECIMAL(38, 0)", "NUMERIC"),
    ("AS_NUMBER", "DECIMAL(38, 0)", "NUMERIC"),
    ("AS_BOOLEAN", "BOOLEAN", "BOOLEAN"),
    ("AS_ARRAY", "JSON", "ARRAY"),
    ("AS_OBJECT", "JSON", "OBJECT"),
    ("AS_DATE", "DATE", None),
    ("AS_TIME", "TIME", None),
    ("AS_TIMESTAMP_NTZ", "TIMESTAMP", None),
    ("AS_TIMESTAMP_LTZ", "TIMESTAMPTZ", None),
    ("AS_TIMESTAMP_TZ", "TIMESTAMPTZ", None),
    ("AS_BINARY", "BLOB", None),
):
    _add_builder("Semi-structured", _name, _as_type(_target, _json))


for _name, _json in (
    ("IS_ARRAY", "ARRAY"),
    ("IS_OBJECT", "OBJECT"),
    ("IS_BOOLEAN", "BOOLEAN"),
    ("IS_VARCHAR", "VARCHAR"),
    ("IS_CHAR", "VARCHAR"),
    ("IS_INTEGER", "INTEGER"),
    ("IS_DOUBLE", "NUMERIC"),
    ("IS_DECIMAL", "NUMERIC"),
    ("IS_REAL", "NUMERIC"),
):
    _add_builder("Semi-structured", _name, _is_type(_json))


for _name, _target in (
    ("IS_DATE", "DATE"),
    ("IS_DATE_VALUE", "DATE"),
    ("IS_TIME", "TIME"),
    ("IS_TIMESTAMP_NTZ", "TIMESTAMP"),
    ("IS_TIMESTAMP_LTZ", "TIMESTAMPTZ"),
    ("IS_TIMESTAMP_TZ", "TIMESTAMPTZ"),
    ("IS_BINARY", "BLOB"),
):
    _add_builder("Semi-structured", _name, _is_castable_string(_target))


@_register("Semi-structured", "AS_VARCHAR", "AS_CHAR")
def _as_varchar(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """Unwrap a VARIANT string without its JSON quotes.

    Like the rest of the AS_ family this checks rather than coerces, so
    `AS_VARCHAR(PARSE_JSON('5'))` is NULL.
    """
    if len(args) != 1:
        return None
    return exp.Case(
        ifs=[
            exp.If(
                this=_json_type_is(args[0].copy(), "VARCHAR"),
                true=_call("json_extract_string", args[0], _str("$")),
            )
        ]
    )


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


@_register("String", "TRY_BASE64_DECODE_STRING")
def _try_base64_decode(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    if not args:
        return None
    return _call("try", _call("decode", _call("from_base64", args[0])))


@_register("String", "OCTET_LENGTH")
def _octet_length(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """OCTET_LENGTH(s) - the length in bytes, not characters.

    DuckDB's `octet_length` only takes a BLOB, and casting a string to one
    rejects any non-ASCII byte; `strlen` already counts bytes.
    """
    return _call("strlen", args[0]) if len(args) == 1 else None


@_register("String", "COLLATION")
def _collation(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """COLLATION(expr) - the collation in effect.

    SnowDuck emulates only the case-insensitive specifiers, by folding case, so
    nothing here ever carries a stored collation and the answer is always NULL -
    which is what Snowflake reports for an uncollated expression.
    """
    return exp.Null() if len(args) == 1 else None


@_register("String", "TRY_TO_BINARY")
def _try_to_binary(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """TRY_TO_BINARY(s [, format]) - HEX, BASE64 or UTF-8, NULL on bad input."""
    return _to_binary(args, safe=True) if args else None


@_register("Conversion", "TO_BINARY")
def _to_binary_function(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    return _to_binary(args) if args else None


def _to_binary_node(node: exp.ToBinary, ctx: DialectContext) -> exp.Expression | None:
    """TO_BINARY and TRY_TO_BINARY are one node, told apart by `safe`."""
    args = _rewritten_args(_positional_args(node), ctx)
    if not args:
        return None
    return _to_binary(args, safe=bool(node.args.get("safe")))


_TYPED_BUILDERS[exp.ToBinary] = _to_binary_node


def _time_node(node: exp.Time, ctx: DialectContext) -> exp.Expression | None:
    """TIME(expr) - Snowflake's one-argument alias for TO_TIME.

    sqlglot renders the node as `CAST(... AT TIME ZONE <zone> AS TIME)`, and
    with no zone to render that comes out as SQL DuckDB cannot parse. The
    two-argument form (a zone conversion) is left to sqlglot.
    """
    if node.args.get("zone") is not None:
        return None
    return exp.Cast(this=node.this, to=exp.DataType.build("TIME"))


_TYPED_BUILDERS[exp.Time] = _time_node


#: GET_DDL object kind -> (DuckDB catalog function, DDL column, name column).
_DDL_SOURCES: dict[str, tuple[str, str, str]] = {
    "TABLE": ("duckdb_tables()", "sql", "table_name"),
    "VIEW": ("duckdb_views()", "sql", "view_name"),
    "SEQUENCE": ("duckdb_sequences()", "sql", "sequence_name"),
    "FUNCTION": ("duckdb_functions()", "macro_definition", "function_name"),
}


@_register("Context", "GET_DDL")
def _get_ddl(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    """GET_DDL('<kind>', '<name>') - the statement that would recreate an object.

    The text comes from DuckDB's catalog, so it is *DuckDB's* rendering of the
    object rather than Snowflake DDL: types are DuckDB's spelling and Snowflake-
    only clauses the object never had locally are absent. It is read through a
    scalar subquery rather than resolved here, so the answer tracks the catalog
    instead of being frozen into the translated SQL. An object that does not
    exist reads back as NULL, where Snowflake raises.
    """
    if len(args) < 2:
        return None
    kind_arg, name_arg = args[0], args[1]
    if not (isinstance(kind_arg, exp.Literal) and kind_arg.is_string):
        return None
    if not (isinstance(name_arg, exp.Literal) and name_arg.is_string):
        return None

    source = _DDL_SOURCES.get(str(kind_arg.this).upper())
    if source is None:
        return None
    relation, ddl_column, name_column = source

    parts = [part.strip('"') for part in str(name_arg.this).split(".")]
    name = parts[-1]
    schema = parts[-2] if len(parts) > 1 else (ctx.current_schema or "")
    database = parts[-3] if len(parts) > 2 else (ctx.current_database or "")

    predicates = [f"upper({name_column}) = upper({_quote(name)})"]
    if schema:
        # DuckDB's internal `main` is Snowflake's MAIN.
        predicates.append(
            f"upper(schema_name) IN (upper({_quote(schema)}), "
            f"CASE WHEN upper({_quote(schema)}) = 'MAIN' THEN 'MAIN' ELSE '' END)"
        )
    if database:
        predicates.append(f"upper(database_name) = upper({_quote(database)})")

    return exp.Subquery(
        this=cast(
            exp.Expression,
            sqlglot.parse_one(
                f"SELECT {ddl_column} FROM {relation} WHERE {' AND '.join(predicates)}",
                read="duckdb",
            ),
        )
    )


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _to_binary(args: list[exp.Expression], *, safe: bool = False) -> exp.Expression:
    """The decoder a TO_BINARY format names; HEX is Snowflake's default.

    DuckDB's `try()` cannot rescue `unhex` on a constant - the folding happens
    before the guard - so the safe form tests the input against the encoding's
    alphabet instead.
    """
    encoding = "HEX"
    if len(args) > 1 and isinstance(args[1], exp.Literal) and args[1].is_string:
        encoding = str(args[1].this).upper()

    if encoding == "BASE64":
        decoded: exp.Expression = _call("from_base64", args[0])
        valid = r"^[A-Za-z0-9+/]*={0,2}$"
    elif encoding in ("UTF-8", "UTF8"):
        decoded = exp.Cast(this=args[0], to=exp.DataType.build("BLOB"))
        valid = ""
    else:
        decoded = _call("unhex", args[0])
        valid = "^([0-9a-fA-F][0-9a-fA-F])*$"

    if not safe:
        return decoded
    if not valid:
        return _call("try", decoded)
    return exp.Case(
        ifs=[
            exp.If(
                this=_call("regexp_matches", args[0].copy(), _str(valid)),
                true=decoded,
            )
        ]
    )


@_register("Semi-structured", "MAP_CAT")
def _map_cat(args: list[exp.Expression], ctx: DialectContext) -> exp.Expression | None:
    """MAP_CAT(a, b) - the two objects merged, b winning on a shared key."""
    if len(args) != 2:
        return None
    return _call("json_merge_patch", args[0], args[1])


@_register("Aggregate", "HLL_ACCUMULATE")
def _hll_accumulate(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """HLL_ACCUMULATE(x) - a sketch that HLL_ESTIMATE can read back.

    DuckDB has no serialisable HyperLogLog state, so the "sketch" is the set of
    distinct values. HLL_ESTIMATE then counts it, which gives the same answer
    the three-function form is asked for; only the intermediate representation
    differs, and it is not one Snowflake documents as portable either.
    """
    return (
        _call("list", exp.Distinct(expressions=[args[0]])) if len(args) == 1 else None
    )


@_register("Aggregate", "HLL_COMBINE")
def _hll_combine(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """HLL_COMBINE(sketch) - merge sketches across groups."""
    if len(args) != 1:
        return None
    return _call("list_distinct", _call("flatten", _call("list", args[0])))


@_register("Aggregate", "HLL_ESTIMATE")
def _hll_estimate(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """HLL_ESTIMATE(sketch) - the cardinality a sketch stands for."""
    if len(args) != 1:
        return None
    return _call("length", _call("list_distinct", args[0]))


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

# Snowflake reports an absent component as null, not as an empty string - the
# documented output for a bare `https://host/` has null for fragment,
# parameters, port and query. `path` is the exception: it comes back as "".
_URL_NULL_WHEN_ABSENT = frozenset({"scheme", "host", "port", "query", "fragment"})


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

    def part(name: str, pattern: str) -> exp.Expression:
        matched = _call(
            "url_decode", _call("regexp_extract", url.copy(), _str(pattern), _num(1))
        )
        if name in _URL_NULL_WHEN_ABSENT:
            # regexp_extract yields '' when the component is missing.
            return _call("nullif", matched, _str(""))
        return matched

    query = _call("regexp_extract", url.copy(), _str(_URL_PARTS["query"]), _num(1))

    pairs: list[exp.Expression] = []
    for name, pattern in _URL_PARTS.items():
        pairs.append(_str(name))
        pairs.append(part(name, pattern))

    # `parameters` is null when there is no query string at all.
    pairs.append(_str("parameters"))
    pairs.append(
        exp.Case(
            ifs=[
                exp.If(
                    this=exp.EQ(this=query.copy(), expression=_str("")),
                    true=exp.Null(),
                )
            ],
            default=_template(_URL_PARAMETERS_TEMPLATE, _QUERY=query),
        )
    )

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


# ---------------------------------------------------------------------------
# Boolean & aggregate
# ---------------------------------------------------------------------------


@_register("Context", "LAST_QUERY_ID")
def _last_query_id(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """The id of the session's previous statement.

    Snowflake's optional argument selects an older statement; only the most
    recent one is tracked here, so any argument reports the same id.
    """
    return _str(ctx.last_query_id or "00000000-0000-0000-0000-000000000000")


@_register("Context", "SYSTEM$TYPEOF")
def _system_typeof(
    args: list[exp.Expression], ctx: DialectContext
) -> exp.Expression | None:
    """SYSTEM$TYPEOF(x) reports the runtime type of an expression."""
    if len(args) != 1:
        return None
    return _call("typeof", args[0])


@_register("Aggregate", "HLL")
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
    _add_builder("Context", _name, _context_literal(_getter))


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def preprocess_functions(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Rewrite registered Snowflake functions into DuckDB expressions.

    Typed nodes are matched first: sqlglot models more of Snowflake's function
    surface with every release, and a name-only lookup silently stops firing
    when a function graduates from a generic Anonymous call to its own node.
    """
    if isinstance(expression, exp.Collate):
        return _collate(expression)

    if isinstance(expression, exp.Window):
        rewritten = _rewrite_window(expression)
        if rewritten is not None:
            return rewritten
        return expression

    if isinstance(expression, exp.Func):
        typed_builder = _TYPED_BUILDERS.get(type(expression))
        if typed_builder is not None:
            replacement = typed_builder(expression, context)
            if replacement is not None:
                return replacement

    node_builder = (
        _NODE_BUILDERS.get(type(expression))
        if isinstance(expression, exp.Func)
        else None
    )
    if node_builder is not None:
        replacement = node_builder(
            _rewritten_args(_positional_args(expression), context), context
        )
        return replacement if replacement is not None else expression

    if not isinstance(expression, exp.Anonymous) or not isinstance(
        expression.this, str
    ):
        return expression

    builder = _BUILDERS.get(expression.this.upper())
    if builder is None:
        return expression

    replacement = builder(
        _rewritten_args(list(expression.expressions), context), context
    )
    return replacement if replacement is not None else expression


def _rewritten_args(
    args: list[exp.Expression], context: DialectContext
) -> list[exp.Expression]:
    """Translate a call's arguments before its builder consumes them.

    sqlglot's `transform` walks parents first and does not descend into a node
    it has just replaced, so a builder that embeds its arguments in new DuckDB
    nodes would carry any *nested* Snowflake call through untranslated -
    `HLL_ESTIMATE(HLL_ACCUMULATE(x))` reached DuckDB with the inner call still
    spelled the Snowflake way. Each argument is therefore rewritten first;
    recursion terminates because every step is strictly smaller.
    """
    return [arg.transform(preprocess_functions, context=context) for arg in args]


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
