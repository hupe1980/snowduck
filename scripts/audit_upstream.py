"""Report where SnowDuck's translation and plain sqlglot's now agree.

SnowDuck carries local rewrites for Snowflake behaviour sqlglot did not model.
sqlglot keeps catching up, and a local rewrite that upstream has since
implemented is worse than useless: it is unmaintained duplicate code that
overrides a tested implementation, and it silently stops matching upstream's
behaviour as Snowflake's semantics are refined.

This walks the conformance corpus, runs every statement twice - once through
SnowDuck's dialect, once through sqlglot's stock Snowflake -> DuckDB path - and
reports which ones now agree. Anything listed as AGREE is a candidate for
deletion; anything listed as DIFFERS is either load-bearing or a bug.

    python scripts/audit_upstream.py [-v]
"""

from __future__ import annotations

import argparse
import ast
import pathlib
import sys

import duckdb
import sqlglot

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from snowduck.dialect import Dialect  # noqa: E402
from snowduck.dialect.context import DialectContext  # noqa: E402
from snowduck.info_schema import InfoSchemaManager  # noqa: E402

CONFORMANCE = pathlib.Path(__file__).resolve().parent.parent / "tests" / "conformance"


def statements() -> list[str]:
    """Every SELECT the conformance suite exercises."""
    found: list[str] = []
    for path in sorted(CONFORMANCE.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                text = node.value.strip()
                if text.upper().startswith("SELECT ") and "\n" not in text:
                    found.append(text)
    return sorted(set(found))


def _run(connection: duckdb.DuckDBPyConnection, sql: str) -> str:
    try:
        return repr(connection.execute(sql).fetchall())
    except Exception as exc:  # noqa: BLE001 - the message is the result
        return f"error: {str(exc).splitlines()[0][:70]}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true", help="list agreements")
    args = parser.parse_args()

    connection = duckdb.connect()
    dialect = Dialect(
        context=DialectContext(info_schema_manager=InfoSchemaManager(connection))
    )

    agree: list[str] = []
    differs: list[tuple[str, str, str]] = []
    untranslatable: list[str] = []

    for sql in statements():
        try:
            ours = sqlglot.parse_one(sql, read="snowflake").sql(dialect=dialect)
            theirs = sqlglot.transpile(sql, read="snowflake", write="duckdb")[0]
        except Exception:  # noqa: BLE001
            untranslatable.append(sql)
            continue

        if ours == theirs:
            agree.append(sql)
            continue

        ours_result = _run(connection, ours)
        theirs_result = _run(connection, theirs)
        if ours_result == theirs_result:
            agree.append(sql)
        else:
            differs.append((sql, ours_result, theirs_result))

    total = len(agree) + len(differs)
    print(f"statements compared: {total}")
    print(f"  same result as plain sqlglot : {len(agree)}")
    print(f"  SnowDuck-specific behaviour  : {len(differs)}")
    if untranslatable:
        print(f"  not translatable standalone  : {len(untranslatable)}")

    if args.verbose and agree:
        print("\nAGREE - upstream now produces the same answer:")
        for sql in agree:
            print(f"  {sql[:100]}")

    print("\nDIFFERS - SnowDuck's own behaviour (verify each is deliberate):")
    for sql, ours_result, theirs_result in differs:
        print(f"  {sql[:96]}")
        print(f"      snowduck: {ours_result[:76]}")
        print(f"      sqlglot : {theirs_result[:76]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
