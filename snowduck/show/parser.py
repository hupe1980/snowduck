"""Parsing of Snowflake ``SHOW`` commands.

sqlglot models only part of Snowflake's ``SHOW`` grammar: ``SHOW OBJECTS`` and
``SHOW VIEWS`` become :class:`sqlglot.exp.Show`, while ``SHOW USER FUNCTIONS``,
``SHOW PARAMETERS`` and ``SHOW DYNAMIC TABLES`` fall back to an opaque
:class:`sqlglot.exp.Command`. Both render back to Snowflake SQL faithfully, so
SnowDuck re-reads that text with a single scanner instead of maintaining two
half-parsers whose behaviour would drift.

The scanner produces a :class:`ShowRequest`, which
:mod:`snowduck.info_schema.manager` turns into a DuckDB query with the column
shape Snowflake documents for that object type.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace

from sqlglot import exp

# Object types are multi-word ("USER FUNCTIONS", "DYNAMIC TABLES"), so the
# object type is everything between the leading SHOW/TERSE and the first clause
# keyword.
_CLAUSE_KEYWORDS = frozenset(
    {"LIKE", "IN", "STARTS", "LIMIT", "FROM", "HISTORY", "WITH", "FOR", "ON"}
)

# Scope keywords that may follow IN. Anything else after IN is the scope name.
_SCOPE_KEYWORDS = frozenset(
    {
        "ACCOUNT",
        "APPLICATION",
        "CLASS",
        "DATABASE",
        "SCHEMA",
        "SESSION",
        "TABLE",
        "VIEW",
        "WAREHOUSE",
    }
)

# Object types whose scope name is a table/view, not a schema.
_TABLE_SCOPED_KINDS = frozenset(
    {"COLUMNS", "PRIMARY KEYS", "IMPORTED KEYS", "UNIQUE KEYS"}
)

# Synonyms Snowflake accepts for the same output shape.
_KIND_ALIASES = {
    "TERSE OBJECTS": "OBJECTS",
    "MATERIALIZED VIEWS": "VIEWS",
    "USER PROCEDURES": "PROCEDURES",
}

_TOKEN_RE = re.compile(
    r"""
    '(?:[^']|'')*'          # single-quoted string literal
    | "(?:[^"]|"")*"        # double-quoted identifier
    | [A-Za-z_$][\w$]*      # bare word
    | \d+                   # number
    | \.                    # name separator
    | \S                    # anything else, one char at a time
    """,
    re.VERBOSE,
)


def _unquote_literal(token: str) -> str:
    return token[1:-1].replace("''", "'")


def _unquote_identifier(token: str) -> str:
    if token.startswith('"') and token.endswith('"') and len(token) >= 2:
        return token[1:-1].replace('""', '"')
    return token


@dataclass(frozen=True)
class ShowRequest:
    """One parsed ``SHOW`` statement, independent of how sqlglot modelled it."""

    kind: str
    terse: bool = False
    history: bool = False
    scope_kind: str | None = None
    #: Dotted scope name, already unquoted, outermost first.
    scope_parts: tuple[str, ...] = field(default_factory=tuple)
    like: str | None = None
    starts_with: str | None = None
    limit: int | None = None
    from_: str | None = None

    def resolved(
        self, *, database: str | None, schema: str | None
    ) -> "ResolvedShowRequest":
        """Fill the scope in from the session context.

        Snowflake resolves an omitted scope against the session's current
        database/schema, and sqlglot mislabels ``IN <schema>`` as ``scope_kind
        TABLE`` for several object types - so the number of dotted name parts,
        not the keyword alone, decides what the name refers to.
        """
        parts = list(self.scope_parts)
        scope_kind = (self.scope_kind or "").upper() or None

        catalog: str | None = None
        target_schema: str | None = None
        table: str | None = None

        if scope_kind == "ACCOUNT":
            pass
        elif scope_kind == "DATABASE":
            catalog = parts[-1] if parts else database
        elif self.kind in _TABLE_SCOPED_KINDS and (
            scope_kind in ("TABLE", "VIEW") or len(parts) == 3
        ):
            if parts:
                table = parts[-1]
                target_schema = parts[-2] if len(parts) >= 2 else schema
                catalog = parts[-3] if len(parts) >= 3 else database
        elif self.kind == "SCHEMAS":
            # SHOW SCHEMAS IN <db> - a bare name is a database, never a schema.
            catalog = parts[-1] if parts else database
        elif parts:
            target_schema = parts[-1]
            catalog = parts[-2] if len(parts) >= 2 else database
        else:
            # No scope at all: account-wide for SCHEMAS/DATABASES, otherwise the
            # session's current schema.
            catalog = database
            target_schema = schema

        return ResolvedShowRequest(
            request=self,
            database=catalog,
            schema=target_schema,
            table=table,
        )


@dataclass(frozen=True)
class ResolvedShowRequest:
    """A :class:`ShowRequest` with its scope resolved against the session."""

    request: ShowRequest
    database: str | None
    schema: str | None
    table: str | None

    @property
    def kind(self) -> str:
        return self.request.kind

    @property
    def terse(self) -> bool:
        return self.request.terse

    @property
    def like(self) -> str | None:
        return self.request.like

    @property
    def starts_with(self) -> str | None:
        return self.request.starts_with

    @property
    def limit(self) -> int | None:
        return self.request.limit

    @property
    def from_(self) -> str | None:
        return self.request.from_


def parse_show(expression: exp.Expression) -> ShowRequest | None:
    """Parse a ``SHOW`` statement, whether sqlglot modelled it or gave up.

    Returns ``None`` for anything that is not a ``SHOW``.
    """
    if isinstance(expression, exp.Show):
        text = expression.sql(dialect="snowflake")
    elif isinstance(expression, exp.Command):
        head = expression.this
        head_str = head.name if isinstance(head, exp.Identifier) else str(head)
        if head_str.upper() != "SHOW":
            return None
        tail = expression.expression
        tail_sql = tail if isinstance(tail, str) else tail.sql(dialect="snowflake")
        text = f"SHOW {tail_sql}"
    else:
        return None

    return parse_show_text(text)


def parse_show_text(text: str) -> ShowRequest | None:
    """Scan ``SHOW ...`` SQL text into a :class:`ShowRequest`."""
    tokens = _TOKEN_RE.findall(text.strip().rstrip(";"))
    if not tokens or tokens[0].upper() != "SHOW":
        return None

    index = 1
    terse = False
    if index < len(tokens) and tokens[index].upper() == "TERSE":
        terse = True
        index += 1

    # Object type: every word up to the first clause keyword.
    kind_words: list[str] = []
    while index < len(tokens):
        word = tokens[index].upper()
        if word in _CLAUSE_KEYWORDS and kind_words:
            break
        if not word.isalpha() and word != "_":
            break
        kind_words.append(word)
        index += 1

    if not kind_words:
        return None

    request = ShowRequest(kind=" ".join(kind_words), terse=terse)

    while index < len(tokens):
        word = tokens[index].upper()
        index += 1

        if word == "HISTORY":
            request = replace(request, history=True)
        elif word == "LIKE" and index < len(tokens):
            request = replace(request, like=_unquote_literal(tokens[index]))
            index += 1
        elif word == "STARTS" and index + 1 < len(tokens):
            # STARTS WITH '<prefix>'
            index += 1  # WITH
            request = replace(request, starts_with=_unquote_literal(tokens[index]))
            index += 1
        elif word == "LIMIT" and index < len(tokens):
            try:
                request = replace(request, limit=int(tokens[index]))
            except ValueError:
                pass
            index += 1
        elif word == "FROM" and index < len(tokens):
            request = replace(request, from_=_unquote_literal(tokens[index]))
            index += 1
        elif word == "IN":
            scope_kind = None
            if index < len(tokens) and tokens[index].upper() in _SCOPE_KEYWORDS:
                scope_kind = tokens[index].upper()
                index += 1
            parts, index = _scan_name(tokens, index)
            request = replace(
                request,
                scope_kind=scope_kind,
                scope_parts=tuple(parts),
            )

    kind = _KIND_ALIASES.get(request.kind, request.kind)
    return replace(request, kind=kind)


def _scan_name(tokens: list[str], index: int) -> tuple[list[str], int]:
    """Read a possibly dotted, possibly quoted object name."""
    parts: list[str] = []
    while index < len(tokens):
        token = tokens[index]
        if token == ".":
            index += 1
            continue
        if token.upper() in _CLAUSE_KEYWORDS and parts:
            break
        if not (token[0].isalpha() or token[0] in '_$"'):
            break
        parts.append(_unquote_identifier(token))
        index += 1
        if index >= len(tokens) or tokens[index] != ".":
            break
    return parts, index
