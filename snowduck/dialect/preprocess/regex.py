"""Snowflake regular-expression semantics not covered by sqlglot.

sqlglot's Snowflake -> DuckDB translation handles most of this family
correctly: ``REGEXP_LIKE``/``RLIKE`` anchor at both ends (via
``regexp_full_match``), ``REGEXP_REPLACE`` replaces every occurrence by
default and splits on ``position``, and
``REGEXP_COUNT``/``REGEXP_INSTR``/``REGEXP_SUBSTR_ALL`` all map across with
their position and occurrence arguments intact.

One divergence remains: **Snowflake returns NULL when nothing matches**, where
DuckDB's ``regexp_extract`` returns an empty string. Snowflake's docs are
explicit - "The function returns NULL in the following cases: No match is
found." Routing REGEXP_SUBSTR through ``regexp_extract_all`` and indexing the
result gives NULL for free, and emulates ``occurrence`` at the same time.
"""

from sqlglot import exp

from ..context import DialectContext

# Snowflake regex flags that map 1:1 onto DuckDB regex options. 'e' is
# deliberately excluded - it is a Snowflake-only "extract group" marker, and
# DuckDB rejects it as an unknown option.
_PASSTHROUGH_FLAGS = "cims"


def _call(name: str, *args: exp.Expression | None) -> exp.Anonymous:
    return exp.Anonymous(this=name, expressions=[a for a in args if a is not None])


def _flags(parameters: exp.Expression | None) -> tuple[bool, exp.Expression | None]:
    """Split Snowflake regex parameters into (wants_group, duckdb_options)."""
    if not (isinstance(parameters, exp.Literal) and parameters.is_string):
        return False, parameters

    raw = str(parameters.this)
    wants_group = "e" in raw.lower()
    kept = "".join(c for c in raw if c.lower() in _PASSTHROUGH_FLAGS)
    return wants_group, exp.Literal.string(kept) if kept else None


def _literal_int(arg: exp.Expression | None) -> int | None:
    if isinstance(arg, exp.Literal) and not arg.is_string:
        try:
            return int(arg.this)
        except (TypeError, ValueError):
            return None
    return None


def _is_one(arg: exp.Expression | None) -> bool:
    """True when an optional position/occurrence argument is its default of 1."""
    return arg is None or _literal_int(arg) == 1


def _resolve_group(group: exp.Expression | None, wants_group: bool) -> exp.Expression:
    """Pick the capture group to extract.

    Without the ``e`` parameter Snowflake returns the whole match whatever
    group_num says. With ``e`` but no explicit group_num it returns the first
    capture group - and an unset group slot arrives as 0, so an explicit 0 has
    to be read as "not specified" here too.
    """
    if not wants_group:
        return exp.Literal.number(0)
    if group is None or _literal_int(group) == 0:
        return exp.Literal.number(1)
    return group


def preprocess_regexp(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Give REGEXP_SUBSTR Snowflake's NULL-on-no-match behaviour."""
    if not isinstance(expression, exp.RegexpExtract):
        return expression

    subject = expression.this
    position = expression.args.get("position")
    if not _is_one(position):
        # Snowflake's `position` is a 1-based start offset.
        subject = _call("substr", subject, position)

    wants_group, options = _flags(expression.args.get("parameters"))
    group = _resolve_group(expression.args.get("group"), wants_group)

    matches = _call(
        "regexp_extract_all", subject, expression.expression, group, options
    )
    occurrence = expression.args.get("occurrence") or exp.Literal.number(1)
    # list_extract rather than a Bracket: the generator rewrites bracket indices
    # for dialect index-offset differences, which would shift the occurrence by
    # one. An out-of-range index yields NULL, matching Snowflake's "no match".
    return _call("list_extract", matches, occurrence)
