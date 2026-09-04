"""Snowflake-specific syntax that has no DuckDB spelling.

These are shapes the parser accepts but the DuckDB generator cannot emit
usefully, so they are rewritten into equivalent standard SQL before generation.
"""

from typing import cast

import sqlglot
from sqlglot import exp

from ..context import DialectContext


def _call(name: str, *args: exp.Expression) -> exp.Anonymous:
    return exp.Anonymous(this=name, expressions=list(args))


_SNOWFLAKE_CONTAINERS = (
    exp.DataType.Type.ARRAY,
    exp.DataType.Type.VARIANT,
    exp.DataType.Type.OBJECT,
)


def preprocess_syntax(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Rewrite Snowflake-only syntax into DuckDB equivalents."""
    if (
        isinstance(expression, exp.DataType)
        and expression.this in _SNOWFLAKE_CONTAINERS
        # A parameterised array such as JSON[] is a real DuckDB list type, not
        # Snowflake's untyped ARRAY - rewriting it would undo the coercions
        # applied by `preprocess_arrays`.
        and not expression.expressions
    ):
        # Snowflake's ARRAY / VARIANT / OBJECT are all modelled as DuckDB JSON.
        return exp.DataType.build("JSON")

    if isinstance(expression, exp.Lateral):
        return _lateral(expression)

    if isinstance(expression, exp.TableFromRows):
        return _table_from_rows(expression)

    if isinstance(expression, (exp.Like, exp.ILike)):
        # sqlglot renders `x LIKE ANY (...)` as Like(expression=Any(Tuple(...)))
        # and `LIKE ALL` as Like(expression=All(...)). DuckDB has neither.
        quantifier = expression.expression
        if isinstance(quantifier, (exp.Any, exp.All)):
            return _like_list(
                expression.this,
                _quantified_patterns(quantifier),
                insensitive=isinstance(expression, exp.ILike),
                require_all=isinstance(quantifier, exp.All),
            )

    if isinstance(expression, exp.Explode):
        _cast_flatten_input(expression)
        return expression

    if isinstance(expression, exp.Merge):
        _unqualify_merge_assignments(expression)
        return expression

    if isinstance(expression, exp.Column):
        rewritten = _sequence_value(expression)
        if rewritten is not None:
            return rewritten

    return expression


# Snowflake's FLATTEN exposes six columns. DuckDB has no equivalent table
# function, so they are synthesised: a struct is built per element and
# expanded with `recursive := TRUE`.
_FLATTEN_COLUMNS = ("SEQ", "KEY", "PATH", "INDEX", "VALUE", "THIS")

# `recursive := TRUE` expands the struct into separate columns, but DuckDB only
# accepts it in a SELECT list - not as a FROM-clause table function - so the
# UNNEST is wrapped in a (LATERAL) subquery.
_FLATTEN_TEMPLATE = (
    "SELECT UNNEST(list_transform(generate_series(1, len(_INPUT)), i -> {"
    "'SEQ': 1, "
    "'KEY': NULL, "
    "'PATH': '[' || (i - 1) || ']', "
    "'INDEX': i - 1, "
    "'VALUE': _INPUT[i], "
    "'THIS': _INPUT"
    "}), recursive := TRUE)"
)


def _flatten_unnest(target: exp.Expression) -> exp.Subquery:
    """Build the subquery that gives FLATTEN its full Snowflake column set."""
    tree = cast(exp.Expression, sqlglot.parse_one(_FLATTEN_TEMPLATE, read="duckdb"))
    source = flatten_input(target)
    for node in list(tree.find_all(exp.Column)):
        if node.name == "_INPUT":
            node.replace(source.copy())
    return exp.Subquery(this=tree)


def _table_alias(
    alias: exp.Expression | None,
    default: str,
    columns: tuple[str, ...] = _FLATTEN_COLUMNS,
) -> exp.TableAlias:
    """Alias for an unnested table function, with FLATTEN's column names."""
    if isinstance(alias, exp.TableAlias):
        if not alias.columns:
            alias = alias.copy()
            alias.set("columns", [exp.to_identifier(c) for c in columns])
        return alias
    name = alias.name if isinstance(alias, exp.Expression) and alias.name else default
    return exp.TableAlias(
        this=exp.to_identifier(name),
        columns=[exp.to_identifier(c) for c in columns],
    )


def _explode_input(explode: exp.Expression) -> exp.Expression | None:
    """The `input =>` argument of a FLATTEN call."""
    kwarg = explode.args.get("this")
    if isinstance(kwarg, exp.Kwarg):
        value = kwarg.expression
        return value if isinstance(value, exp.Expression) else None
    return kwarg if isinstance(kwarg, exp.Expression) else None


def _lateral(lateral: exp.Lateral) -> exp.Expression:
    """LATERAL FLATTEN(input => x) -> LATERAL UNNEST(x) AS alias(VALUE)."""
    if not isinstance(lateral.this, exp.Explode):
        return lateral

    target = _explode_input(lateral.this)
    if target is None:
        return lateral

    return exp.Lateral(
        this=_flatten_unnest(target),
        alias=_table_alias(lateral.args.get("alias"), "_flattened"),
    )


def _table_from_rows(node: exp.TableFromRows) -> exp.Expression:
    """Snowflake wraps table functions in `TABLE(...)`; DuckDB calls them directly."""
    inner = node.this
    alias = node.args.get("alias")

    if isinstance(inner, exp.Explode):
        target = _explode_input(inner)
        if target is not None:
            unnest = _flatten_unnest(target)
            unnest.set("alias", _table_alias(alias, "_flattened"))
            return unnest

    if isinstance(inner, exp.Anonymous) and isinstance(inner.this, str):
        if inner.this.upper() in ("SPLIT_TO_TABLE", "STRTOK_SPLIT_TO_TABLE"):
            args = list(inner.expressions)
            if len(args) >= 2:
                return exp.Unnest(
                    expressions=[_call("str_split", args[0], args[1])],
                    alias=_table_alias(alias, "_split", columns=("VALUE",)),
                )

    if isinstance(inner, exp.Expression):
        replacement = inner.copy()
        if alias is not None:
            replacement.set("alias", alias)
        return replacement

    return node


def _quantified_patterns(quantifier: exp.Expression) -> list[exp.Expression]:
    """The pattern list inside an ANY (...) / ALL (...) quantifier."""
    inner = quantifier.this
    if isinstance(inner, exp.Tuple):
        return list(inner.expressions)
    if isinstance(inner, exp.Paren):
        return [inner.this]
    return [inner] if isinstance(inner, exp.Expression) else []


def _like_list(
    subject: exp.Expression,
    patterns: list[exp.Expression],
    insensitive: bool,
    require_all: bool,
) -> exp.Expression:
    """`x LIKE ANY (a, b)` -> `x LIKE a OR x LIKE b`; ALL joins with AND."""
    if not patterns:
        return subject

    comparison = exp.ILike if insensitive else exp.Like
    join = exp.And if require_all else exp.Or
    terms = [
        comparison(this=subject.copy(), expression=pattern.copy())
        for pattern in patterns
    ]

    combined: exp.Expression = terms[0]
    for term in terms[1:]:
        combined = join(this=combined, expression=term)
    return exp.Paren(this=combined)


def _unqualify_merge_assignments(merge: exp.Merge) -> None:
    """Drop the table qualifier from MERGE ... UPDATE SET targets.

    Snowflake writes `SET tgt.name = src.name`; DuckDB rejects a qualified
    column on the left of SET ("Qualified column names in UPDATE .. SET not
    supported"). The target table is unambiguous there, so the qualifier is
    simply dropped - only on the left-hand side, since the right-hand side
    still has to distinguish source from target.
    """
    for when in merge.find_all(exp.When):
        update = when.args.get("then")
        if not isinstance(update, exp.Update):
            continue
        for assignment in update.expressions:
            target = assignment.this if isinstance(assignment, exp.EQ) else None
            if isinstance(target, exp.Column) and target.args.get("table"):
                assignment.set("this", exp.column(target.this))


def _sequence_value(column: exp.Column) -> exp.Expression | None:
    """`seq.NEXTVAL` -> `nextval('seq')`, and the same for CURRVAL."""
    name = column.name.upper() if column.name else ""
    if name not in ("NEXTVAL", "CURRVAL"):
        return None

    table = column.args.get("table")
    if table is None:
        return None

    parts = [
        part.name
        for part in (column.args.get("catalog"), column.args.get("db"), table)
        if part is not None
    ]
    return _call(name.lower(), exp.Literal.string(".".join(parts)))


_JSON_LIST = exp.DataType.build("JSON[]")


def flatten_input(value: exp.Expression) -> exp.Expression:
    """A FLATTEN input in a form UNNEST accepts.

    SnowDuck models Snowflake ARRAY/VARIANT as DuckDB JSON, which UNNEST
    rejects, so a JSON value must be cast to JSON[] first. An expression that is
    already a native list (ARRAY_CONSTRUCT, an array literal) is left alone, so
    its elements keep their own types instead of each becoming a JSON value.
    """
    if isinstance(value, (exp.Array, exp.Unnest)) or (
        isinstance(value, exp.Cast) and value.to == _JSON_LIST
    ):
        return value
    return exp.Cast(this=value, to=_JSON_LIST.copy())


def _cast_flatten_input(explode: exp.Expression) -> None:
    """Apply :func:`flatten_input` in place.

    Done on the AST so it survives sqlglot's own rewriting of
    ``TABLE(FLATTEN(...))`` into an UNNEST subquery.
    """
    kwarg = explode.args.get("this")
    target = kwarg.expression if isinstance(kwarg, exp.Kwarg) else kwarg
    if target is None:
        return
    wrapped = flatten_input(target)
    if wrapped is not target:
        target.replace(wrapped)
