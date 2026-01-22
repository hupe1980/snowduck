"""Tests for additional Snowflake-compatible features - TDD approach."""

import snowflake.connector

from snowduck import mock_snowflake

# =============================================================================
# SPLIT function tests
# =============================================================================

@mock_snowflake
def test_split_basic():
    """Test SPLIT function splits string into array."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT SPLIT('a,b,c', ',')")
    result = cur.fetchone()[0]
    
    assert result == ['a', 'b', 'c']


@mock_snowflake
def test_split_with_multi_char_delimiter():
    """Test SPLIT with multi-character delimiter."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT SPLIT('a::b::c', '::')")
    result = cur.fetchone()[0]
    
    assert result == ['a', 'b', 'c']


# =============================================================================
# POSITION function tests
# =============================================================================

@mock_snowflake
def test_position_basic():
    """Test POSITION function finds substring position."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT POSITION('world' IN 'hello world')")
    result = cur.fetchone()[0]
    
    assert result == 7  # 1-based index


@mock_snowflake
def test_position_not_found():
    """Test POSITION returns 0 when not found."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT POSITION('xyz' IN 'hello world')")
    result = cur.fetchone()[0]
    
    assert result == 0


# =============================================================================
# CONCAT_WS function tests
# =============================================================================

@mock_snowflake
def test_concat_ws_basic():
    """Test CONCAT_WS concatenates with separator."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT CONCAT_WS('-', 'a', 'b', 'c')")
    result = cur.fetchone()[0]
    
    assert result == 'a-b-c'


@mock_snowflake
def test_concat_ws_with_nulls():
    """Test CONCAT_WS skips NULL values."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT CONCAT_WS('-', 'a', NULL, 'c')")
    result = cur.fetchone()[0]
    
    assert result == 'a-c'


# =============================================================================
# COALESCE extended tests
# =============================================================================

@mock_snowflake
def test_coalesce_multiple_values():
    """Test COALESCE with multiple NULL values."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT COALESCE(NULL, NULL, 'third', 'fourth')")
    result = cur.fetchone()[0]
    
    assert result == 'third'


# =============================================================================
# GREATEST/LEAST function tests
# =============================================================================

@mock_snowflake
def test_greatest_basic():
    """Test GREATEST returns maximum value."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT GREATEST(1, 5, 3, 9, 2)")
    result = cur.fetchone()[0]
    
    assert result == 9


@mock_snowflake
def test_least_basic():
    """Test LEAST returns minimum value."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT LEAST(1, 5, 3, 9, 2)")
    result = cur.fetchone()[0]
    
    assert result == 1


@mock_snowflake
def test_greatest_with_strings():
    """Test GREATEST with string values."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT GREATEST('apple', 'banana', 'cherry')")
    result = cur.fetchone()[0]
    
    assert result == 'cherry'


# =============================================================================
# LEN/LENGTH function tests
# =============================================================================

@mock_snowflake
def test_len_function():
    """Test LEN returns string length."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT LEN('hello')")
    result = cur.fetchone()[0]
    
    assert result == 5


@mock_snowflake
def test_length_function():
    """Test LENGTH returns string length."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT LENGTH('hello world')")
    result = cur.fetchone()[0]
    
    assert result == 11


# =============================================================================
# UPPER/LOWER function tests
# =============================================================================

@mock_snowflake
def test_upper_function():
    """Test UPPER converts to uppercase."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT UPPER('Hello World')")
    result = cur.fetchone()[0]
    
    assert result == 'HELLO WORLD'


@mock_snowflake
def test_lower_function():
    """Test LOWER converts to lowercase."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT LOWER('Hello World')")
    result = cur.fetchone()[0]
    
    assert result == 'hello world'


# =============================================================================
# DATE_PART function tests
# =============================================================================

@mock_snowflake
def test_date_part_year():
    """Test DATE_PART extracts year."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT DATE_PART('year', '2024-06-15'::DATE)")
    result = cur.fetchone()[0]
    
    assert result == 2024


@mock_snowflake
def test_date_part_month():
    """Test DATE_PART extracts month."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT DATE_PART('month', '2024-06-15'::DATE)")
    result = cur.fetchone()[0]
    
    assert result == 6


@mock_snowflake
def test_date_part_day():
    """Test DATE_PART extracts day."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT DATE_PART('day', '2024-06-15'::DATE)")
    result = cur.fetchone()[0]
    
    assert result == 15


# =============================================================================
# TO_DATE / TO_TIMESTAMP with format tests
# =============================================================================

@mock_snowflake
def test_to_date_basic():
    """Test TO_DATE converts string to date."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT TO_DATE('2024-06-15')")
    result = cur.fetchone()[0]
    
    assert '2024-06-15' in str(result)


@mock_snowflake
def test_to_timestamp_basic():
    """Test TO_TIMESTAMP converts string to timestamp."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT TO_TIMESTAMP('2024-06-15 14:30:00')")
    result = cur.fetchone()[0]
    
    assert '2024-06-15' in str(result)


# =============================================================================
# NULLIF function tests
# =============================================================================

@mock_snowflake
def test_nullif_equal():
    """Test NULLIF returns NULL when values equal."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT NULLIF(5, 5)")
    result = cur.fetchone()[0]
    
    assert result is None


@mock_snowflake
def test_nullif_not_equal():
    """Test NULLIF returns first value when not equal."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT NULLIF(5, 10)")
    result = cur.fetchone()[0]
    
    assert result == 5


# =============================================================================
# SIGN function tests
# =============================================================================

@mock_snowflake
def test_sign_positive():
    """Test SIGN returns 1 for positive numbers."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT SIGN(42)")
    result = cur.fetchone()[0]
    
    assert result == 1


@mock_snowflake
def test_sign_negative():
    """Test SIGN returns -1 for negative numbers."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT SIGN(-42)")
    result = cur.fetchone()[0]
    
    assert result == -1


@mock_snowflake
def test_sign_zero():
    """Test SIGN returns 0 for zero."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT SIGN(0)")
    result = cur.fetchone()[0]
    
    assert result == 0


# =============================================================================
# ABS function tests
# =============================================================================

@mock_snowflake
def test_abs_positive():
    """Test ABS returns absolute value of positive."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT ABS(42)")
    result = cur.fetchone()[0]
    
    assert result == 42


@mock_snowflake
def test_abs_negative():
    """Test ABS returns absolute value of negative."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT ABS(-42)")
    result = cur.fetchone()[0]
    
    assert result == 42


# =============================================================================
# EXP / LN / LOG function tests
# =============================================================================

@mock_snowflake
def test_exp_function():
    """Test EXP returns e^x."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT ROUND(EXP(1), 5)")
    result = cur.fetchone()[0]
    
    assert abs(result - 2.71828) < 0.001


@mock_snowflake
def test_ln_function():
    """Test LN returns natural logarithm."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT ROUND(LN(2.71828), 2)")
    result = cur.fetchone()[0]
    
    assert abs(result - 1.0) < 0.01


@mock_snowflake
def test_log_base10():
    """Test LOG returns base-10 logarithm."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT LOG(10, 100)")
    result = cur.fetchone()[0]
    
    assert result == 2


# =============================================================================
# RANDOM function tests
# =============================================================================

@mock_snowflake
def test_random_returns_value():
    """Test RANDOM returns a numeric value."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT RANDOM()")
    result = cur.fetchone()[0]
    
    assert isinstance(result, (int, float))


# =============================================================================
# TRIM variants tests
# =============================================================================

@mock_snowflake
def test_ltrim_basic():
    """Test LTRIM removes leading spaces."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT LTRIM('  hello  ')")
    result = cur.fetchone()[0]
    
    assert result == 'hello  '


@mock_snowflake
def test_rtrim_basic():
    """Test RTRIM removes trailing spaces."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT RTRIM('  hello  ')")
    result = cur.fetchone()[0]
    
    assert result == '  hello'


@mock_snowflake
def test_trim_both():
    """Test TRIM removes both leading and trailing spaces."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT TRIM('  hello  ')")
    result = cur.fetchone()[0]
    
    assert result == 'hello'


# =============================================================================
# SUBSTR/SUBSTRING function tests
# =============================================================================

@mock_snowflake
def test_substr_basic():
    """Test SUBSTR extracts substring."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT SUBSTR('hello world', 7)")
    result = cur.fetchone()[0]
    
    assert result == 'world'


@mock_snowflake
def test_substr_with_length():
    """Test SUBSTR with length parameter."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT SUBSTR('hello world', 1, 5)")
    result = cur.fetchone()[0]
    
    assert result == 'hello'


@mock_snowflake
def test_substring_basic():
    """Test SUBSTRING extracts substring."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT SUBSTRING('hello world', 7, 5)")
    result = cur.fetchone()[0]
    
    assert result == 'world'


# =============================================================================
# INSTR function tests
# =============================================================================

@mock_snowflake
def test_instr_basic():
    """Test INSTR finds substring position."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT INSTR('hello world', 'world')")
    result = cur.fetchone()[0]
    
    assert result == 7


@mock_snowflake
def test_instr_not_found():
    """Test INSTR returns 0 when not found."""
    conn = snowflake.connector.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT INSTR('hello world', 'xyz')")
    result = cur.fetchone()[0]
    
    assert result == 0
