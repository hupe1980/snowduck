"""DuckDB macros to emulate Snowflake functions not native to DuckDB."""

from duckdb import DuckDBPyConnection

# SQL macros to emulate Snowflake functions not native to DuckDB
# These macros are database-scoped in DuckDB, so they need to be created in each database
# The macros are created in the 'main' schema of each database so they're accessible
# from any schema within that database.
_MACRO_DEFINITIONS = [
    # INITCAP - capitalize first letter of each word
    # Uses list_transform to process each space-separated word
    (
        "INITCAP",
        """
        CREATE OR REPLACE MACRO {schema}INITCAP(str) AS (
            list_aggregate(
                list_transform(
                    string_split(lower(str), ' '),
                    x -> upper(x[1]) || lower(x[2:])
                ),
                'string_agg',
                ' '
            )
        )
        """,
    ),
    # SOUNDEX - phonetic algorithm (simplified implementation)
    # Standard Soundex: keep first letter, map consonants to digits 1-6, remove vowels/duplicates
    (
        "SOUNDEX",
        """
        CREATE OR REPLACE MACRO {schema}SOUNDEX(str) AS (
            upper(str[1]) || 
            lpad(
                replace(replace(replace(replace(replace(replace(
                    regexp_replace(
                        regexp_replace(
                            translate(upper(str[2:]), 'AEIOUYHW', ''),
                            '([BFPV])+', '1', 'g'
                        ),
                        '([CGJKQSXZ])+', '2', 'g'
                    ),
                    'D', '3'),
                    'T', '3'),
                    'L', '4'),
                    'M', '5'),
                    'N', '5'),
                    'R', '6'
                ),
                3, '0'
            )[1:4]
        )
        """,
    ),
]


def register_macros(
    duck_conn: DuckDBPyConnection, database: str | None = None
) -> None:
    """Register Snowflake-compatible macros in the DuckDB connection.

    Args:
        duck_conn: DuckDB connection to register macros on
        database: If provided, creates macros in the 'main' schema of this database.
                  Otherwise creates in the current schema.
    """
    schema_prefix = f"{database}.main." if database else ""

    for _name, macro_template in _MACRO_DEFINITIONS:
        try:
            macro_sql = macro_template.format(schema=schema_prefix)
            duck_conn.execute(macro_sql)
        except Exception:
            # Macro may already exist or fail for other reasons - continue
            pass
