"""String functions with behaviour that is easy to get subtly wrong."""

import json


def test_parse_url_components(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT PARSE_URL('https://ex.com:8080/a%20b/c?q=hello%20world&n=2#frag')"
        )
        parsed = json.loads(cur.fetchone()[0])

    assert parsed["scheme"] == "https"
    assert parsed["host"] == "ex.com"
    assert parsed["port"] == "8080"
    assert parsed["fragment"] == "frag"
    # Percent-escapes are decoded.
    assert parsed["path"] == "a b/c"


def test_parse_url_parameters_are_an_object(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT PARSE_URL('https://ex.com/p?q=hello%20world&n=2')")
        parsed = json.loads(cur.fetchone()[0])

    assert parsed["parameters"] == {"q": "hello world", "n": "2"}


def test_parse_url_matches_the_documented_example(conn):
    """Snowflake's own example output for a bare URL.

    Absent components are null, not empty strings - except `path`, which the
    documentation shows as "".
    """
    with conn.cursor() as cur:
        cur.execute("SELECT PARSE_URL('https://www.snowflake.com/')")
        parsed = json.loads(cur.fetchone()[0])

    assert parsed == {
        "fragment": None,
        "host": "www.snowflake.com",
        "parameters": None,
        "path": "",
        "port": None,
        "query": None,
        "scheme": "https",
    }


def test_collate_case_insensitive(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COLLATE('ABC', 'en-ci') = 'abc', COLLATE('ABC', 'en') = 'abc'"
        )
        insensitive, plain = cur.fetchone()

    assert insensitive is True
    assert plain is False


def test_to_number_format_model_strips_decoration(conn):
    """A format model describes the decoration around the digits."""
    with conn.cursor() as cur:
        # Scale still defaults to 0 even with a format model, so this rounds.
        cur.execute("SELECT TO_NUMBER('$1,234.56', '$9,999.99')")
        assert cur.fetchone()[0] == 1235

        # The documented example from Snowflake's TO_DECIMAL page.
        cur.execute("SELECT TO_DECIMAL('$3,741.72', '$9,999.99', 6, 2)")
        assert str(cur.fetchone()[0]) == "3741.72"
