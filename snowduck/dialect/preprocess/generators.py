"""GENERATOR and sequence functions preprocessing."""

from sqlglot import exp

from ..context import DialectContext


def _numeric_type(
    precision: exp.Expression | None, scale: exp.Expression | None
) -> exp.DataType:
    """Build the DuckDB target type for a Snowflake numeric conversion.

    Snowflake's TO_NUMBER/TO_DECIMAL take (precision, scale) and return an exact
    NUMBER. Dropping them and casting to DOUBLE turns exact decimal arithmetic
    into binary floating point, which silently changes results for things like
    check-digit and money calculations.
    """
    # Snowflake's default is NUMBER(38, 0): a bare TO_NUMBER('123.45') yields
    # 123, because reducing the scale rounds rather than truncates.
    if precision is None:
        return exp.DataType.build("DECIMAL(38, 0)")

    def _int(node: exp.Expression | None, default: int) -> int:
        if isinstance(node, exp.Literal) and not node.is_string:
            try:
                return int(node.this)
            except (TypeError, ValueError):
                return default
        return default

    # DuckDB's DECIMAL tops out at precision 38, same as Snowflake's NUMBER.
    prec = min(_int(precision, 38), 38)
    return exp.DataType.build(f"DECIMAL({prec}, {_int(scale, 0)})")


def _numeric_args_from_list(
    args: list[exp.Expression],
) -> tuple[exp.Expression | None, exp.Expression | None]:
    """Pull (precision, scale) out of the trailing args of a TO_DECIMAL-style call.

    A leading string argument is a format model, not a precision.
    """
    numeric = [a for a in args if not (isinstance(a, exp.Literal) and a.is_string)]
    precision = numeric[0] if len(numeric) >= 1 else None
    scale = numeric[1] if len(numeric) >= 2 else None
    return precision, scale


def _apply_number_format(
    value: exp.Expression, fmt: exp.Expression | None
) -> exp.Expression:
    """Strip the decoration a TO_NUMBER format model describes.

    Snowflake format models such as `$9,999.99` or `999,999` describe currency
    symbols and group separators around the digits. Rather than interpreting
    the model, the characters it allows for decoration are removed before the
    cast, which is what the model means for parsing: `TO_NUMBER('$1,234.56',
    '$9,999.99')` -> 1234.56.
    """
    if not (isinstance(fmt, exp.Literal) and fmt.is_string):
        return value
    return exp.Anonymous(
        this="regexp_replace",
        expressions=[
            value,
            exp.Literal.string(r"[^0-9.\-]"),
            exp.Literal.string(""),
            exp.Literal.string("g"),
        ],
    )


def _width_bucket(expression: exp.WidthBucket) -> exp.Expression:
    """WIDTH_BUCKET(value, min, max, count) -> the 1-based bucket index.

    Values below `min` fall in bucket 0 and values at or above `max` in
    bucket count+1, matching Snowflake.
    """
    value = expression.this
    low = expression.args["min_value"]
    high = expression.args["max_value"]
    buckets = expression.args["num_buckets"]

    scaled = exp.Div(
        this=exp.Paren(this=exp.Sub(this=value.copy(), expression=low.copy())),
        expression=exp.Paren(this=exp.Sub(this=high.copy(), expression=low.copy())),
    )
    index = exp.Add(
        this=exp.Cast(
            this=exp.Anonymous(
                this="floor",
                expressions=[
                    exp.Mul(this=exp.Paren(this=scaled), expression=buckets.copy())
                ],
            ),
            to=exp.DataType.build("BIGINT"),
        ),
        expression=exp.Literal.number(1),
    )
    return exp.Case(
        ifs=[
            exp.If(
                this=exp.LT(this=value.copy(), expression=low.copy()),
                true=exp.Literal.number(0),
            ),
            exp.If(
                this=exp.GTE(this=value.copy(), expression=high.copy()),
                true=exp.Add(this=buckets.copy(), expression=exp.Literal.number(1)),
            ),
        ],
        default=index,
    )


# Snowflake: "If date_or_time_part is day or larger (for example, month, year),
# the function returns a DATE value" - but only when the input is itself a DATE.
# Anything smaller returns TIMESTAMP_NTZ.
_DATE_PRESERVING_UNITS = frozenset(
    {"DAY", "DAYOFMONTH", "WEEK", "WEEKISO", "MONTH", "QUARTER", "YEAR", "YEAROFWEEK"}
)


def _is_date_valued(node: exp.Expression | None) -> bool:
    """Whether an expression is statically known to be a DATE.

    Only the forms that say so on their face are recognised; the type of a bare
    column is not known at translation time, so `DATEADD` over one keeps
    DuckDB's TIMESTAMP result.
    """
    if isinstance(node, (exp.Cast, exp.TryCast)):
        return bool(node.to and node.to.this == exp.DataType.Type.DATE)
    return isinstance(node, (exp.CurrentDate, exp.Date, exp.DateStrToDate))


def _date_add_result(expression: exp.DateAdd) -> exp.Expression:
    """Keep DATEADD's result a DATE when Snowflake would.

    DuckDB promotes `DATE + INTERVAL` to TIMESTAMP, so a model column that
    should be a DATE came back as a timestamp.
    """
    unit = expression.args.get("unit")
    unit_name = (
        unit.name if isinstance(unit, exp.Expression) else str(unit or "")
    ).upper()
    if unit_name not in _DATE_PRESERVING_UNITS or not _is_date_valued(expression.this):
        return expression
    if isinstance(expression.parent, exp.Cast):
        return expression
    return exp.Cast(this=expression, to=exp.DataType.build("DATE"))


def preprocess_special_expressions(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Transform special expression types that aren't Anonymous functions."""

    if (
        isinstance(expression, exp.Drop)
        and str(expression.args.get("kind")).upper() == "FUNCTION"
    ):
        # Snowflake identifies a UDF by its signature (DROP FUNCTION f(VARCHAR));
        # a DuckDB macro is identified by name alone, and the signature is a
        # syntax error there.
        dropped = expression.copy()
        dropped.set("kind", "MACRO")
        dropped.set("expressions", None)
        return dropped

    if isinstance(expression, exp.ToNumber):
        # TO_NUMBER / TO_DECIMAL / TO_NUMERIC and their TRY_ variants, which
        # arrive as the same node with `safe` set.
        target = _numeric_type(
            expression.args.get("precision"), expression.args.get("scale")
        )
        value = _apply_number_format(expression.this, expression.args.get("format"))
        if expression.args.get("safe"):
            return exp.TryCast(this=value, to=target)
        return exp.Cast(this=value, to=target)

    if isinstance(expression, exp.DateAdd):
        return _date_add_result(expression)

    if isinstance(expression, exp.ToVariant):
        # Snowflake VARIANT is modelled as DuckDB JSON.
        return exp.Cast(this=expression.this, to=exp.DataType.build("JSON"))

    if isinstance(expression, exp.WidthBucket):
        return _width_bucket(expression)

    if isinstance(expression, exp.Space):
        # SPACE(n) -> REPEAT(' ', n) in DuckDB
        return exp.Anonymous(
            this="repeat",
            expressions=[exp.Literal.string(" "), expression.this],
        )

    if isinstance(expression, exp.ObjectInsert):
        # OBJECT_INSERT(obj, key, value) -> json_merge_patch(obj, json_object(key, value))
        obj_expr = expression.this
        key_expr = expression.args.get("key")
        val_expr = expression.args.get("value")
        if obj_expr and key_expr and val_expr:
            # Create json_object(key, value)
            new_obj = exp.Anonymous(
                this="json_object", expressions=[key_expr, val_expr]
            )
            # Merge with original using json_merge_patch
            return exp.Anonymous(
                this="json_merge_patch", expressions=[obj_expr, new_obj]
            )

    return expression


def preprocess_bitwise(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Transform bitwise operations for DuckDB compatibility.

    DuckDB uses `xor()` function instead of `^` operator for XOR
    (because `^` is power in DuckDB).
    """
    if isinstance(expression, exp.BitwiseXor):
        # Convert BitwiseXor to xor(a, b) function call
        return exp.Anonymous(
            this="xor", expressions=[expression.this, expression.expression]
        )
    return expression


def preprocess_generator(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Pre-process GENERATOR table functions into DuckDB generate_series."""

    # sqlglot parses TABLE(GENERATOR(...)) as TableFromRows containing the Anonymous function.
    if isinstance(expression, exp.TableFromRows):
        if (
            isinstance(expression.this, exp.Anonymous)
            and expression.this.this.upper() == "GENERATOR"
        ):
            gen_expr = expression.this
            rowcount = None

            # Extract ROWCOUNT
            for arg in gen_expr.expressions:
                if isinstance(arg, exp.Kwarg) and arg.this.name.upper() == "ROWCOUNT":
                    rowcount = arg.expression
                elif (
                    isinstance(arg, exp.PropertyEQ)
                    and arg.this.name.upper() == "ROWCOUNT"
                ):
                    rowcount = arg.expression

            if rowcount:
                # DuckDB uses generate_series(start, stop)
                return exp.Anonymous(
                    this="generate_series",
                    expressions=[exp.Literal.number(1), rowcount],
                )

    return expression


def preprocess_seq_functions(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Transform sequence generation functions like SEQ4() to DuckDB equivalents."""

    if isinstance(expression, exp.Anonymous):
        fname = expression.this.upper()
        if fname == "EQUAL_NULL":
            # EQUAL_NULL(a, b) -> (a IS NOT DISTINCT FROM b)
            # This is Snowflake's NULL-safe equality comparison
            # DuckDB supports IS NOT DISTINCT FROM via NullSafeEQ
            args = expression.expressions
            if len(args) == 2:
                return exp.NullSafeEQ(this=args[0], expression=args[1])

        elif fname == "DIV0NULL":
            # DIV0NULL(a, b) -> CASE WHEN b = 0 THEN NULL ELSE a / b END
            args = expression.expressions
            if len(args) == 2:
                return exp.Case(
                    ifs=[
                        exp.If(
                            this=exp.EQ(this=args[1], expression=exp.Literal.number(0)),
                            true=exp.Null(),
                        )
                    ],
                    default=exp.Div(this=args[0], expression=args[1]),
                )

        elif fname == "BITAND":
            # BITAND(a, b) -> (a & b) in DuckDB
            args = expression.expressions
            if len(args) == 2:
                return exp.BitwiseAnd(this=args[0], expression=args[1])

        elif fname == "BITOR":
            # BITOR(a, b) -> (a | b) in DuckDB
            args = expression.expressions
            if len(args) == 2:
                return exp.BitwiseOr(this=args[0], expression=args[1])

        elif fname == "BITXOR":
            # BITXOR(a, b) -> xor(a, b) in DuckDB
            args = expression.expressions
            if len(args) == 2:
                return exp.Anonymous(this="xor", expressions=args)

        elif fname == "BITNOT":
            # BITNOT(a) -> ~a in DuckDB
            args = expression.expressions
            if len(args) == 1:
                return exp.BitwiseNot(this=args[0])

        elif fname == "TO_BOOLEAN":
            # TO_BOOLEAN(x) -> CAST(x AS BOOLEAN) in DuckDB
            args = expression.expressions
            if len(args) == 1:
                return exp.Cast(this=args[0], to=exp.DataType.build("BOOLEAN"))

        elif fname == "WIDTH_BUCKET":
            # WIDTH_BUCKET(expr, min, max, num_buckets) ->
            # CASE
            #   WHEN expr < min THEN 0
            #   WHEN expr >= max THEN num_buckets + 1
            #   ELSE FLOOR((expr - min) / (max - min) * num_buckets) + 1
            # END
            args = expression.expressions
            if len(args) == 4:
                expr, min_val, max_val, num_buckets = args
                # Calculate: FLOOR((expr - min) * num_buckets / (max - min)) + 1
                # This is equivalent to assigning to buckets 1..num_buckets
                # Use Paren for grouping to ensure correct operator precedence
                expr_minus_min = exp.Paren(this=exp.Sub(this=expr, expression=min_val))
                max_minus_min = exp.Paren(
                    this=exp.Sub(this=max_val, expression=min_val)
                )
                # (expr - min) * num_buckets
                numerator = exp.Mul(this=expr_minus_min, expression=num_buckets)
                # (expr - min) * num_buckets / (max - min)
                bucket_position = exp.Div(this=numerator, expression=max_minus_min)
                # Normal bucket: FLOOR(...) + 1
                normal_bucket = exp.Add(
                    this=exp.Floor(this=bucket_position),
                    expression=exp.Literal.number(1),
                )
                return exp.Case(
                    ifs=[
                        exp.If(
                            this=exp.LT(this=expr, expression=min_val),
                            true=exp.Literal.number(0),
                        ),
                        exp.If(
                            this=exp.GTE(this=expr, expression=max_val),
                            true=exp.Add(
                                this=num_buckets, expression=exp.Literal.number(1)
                            ),
                        ),
                    ],
                    default=normal_bucket,
                )

        elif fname == "TRUNCATE":
            # TRUNCATE(x, p) -> TRUNC(x, p) in DuckDB
            args = expression.expressions
            return exp.Anonymous(this="trunc", expressions=args)

        elif fname == "STRTOK":
            # STRTOK(string, delimiters, partNumber) -> split_part(string, delimiters, partNumber)
            # Snowflake: STRTOK('a,b,c', ',', 2) returns 'b'
            # DuckDB: split_part('a,b,c', ',', 2) returns 'b'
            args = expression.expressions
            if len(args) >= 2:
                return exp.Anonymous(this="split_part", expressions=args)

        elif fname == "ADD_MONTHS":
            # ADD_MONTHS(date, months) -> date + INTERVAL months MONTH
            args = expression.expressions
            if len(args) == 2:
                date_expr = args[0]
                months = args[1]
                # Create INTERVAL expression
                interval = exp.Interval(this=months, unit=exp.Var(this="MONTH"))
                return exp.Add(this=date_expr, expression=interval)

        elif fname in ("SEQ1", "SEQ2", "SEQ4", "SEQ8"):
            # Transform to ROW_NUMBER() OVER () - 1
            window = exp.Window(
                this=exp.Anonymous(this="row_number"), over=exp.WindowSpec()
            )
            return exp.Sub(this=window, expression=exp.Literal.number(1))

        elif fname == "UNIFORM":
            # UNIFORM(min, max, seed) -> floor(random() * (max - min + 1) + min)
            args = expression.expressions
            if len(args) >= 2:
                min_val = args[0]
                max_val = args[1]
                range_len = exp.Add(
                    this=exp.Sub(this=max_val, expression=min_val),
                    expression=exp.Literal.number(1),
                )
                rand_scaled = exp.Mul(
                    this=exp.Anonymous(this="random"), expression=range_len
                )
                shifted = exp.Add(this=exp.Floor(this=rand_scaled), expression=min_val)
                return exp.Cast(this=shifted, to=exp.DataType.build("int"))

        # =========================================================================
        # STRING FUNCTIONS
        # =========================================================================
        elif fname == "INITCAP":
            # INITCAP(str) - handled by DuckDB macro registered in connector
            # Just pass through - macro will handle it
            pass

        elif fname == "SOUNDEX":
            # SOUNDEX(str) - handled by DuckDB macro registered in connector
            # Just pass through - macro will handle it
            pass

        elif fname == "CHARINDEX":
            # CHARINDEX(substr, str) -> strpos(str, substr)
            # CHARINDEX(substr, str, start) -> strpos(str[start:], substr) + start - 1
            # Note: Snowflake CHARINDEX is 1-based, DuckDB strpos is 1-based
            args = expression.expressions
            if len(args) == 2:
                substr = args[0]
                string = args[1]
                return exp.Anonymous(this="strpos", expressions=[string, substr])
            elif len(args) == 3:
                # With start position - DuckDB strpos doesn't have start, use locate
                substr = args[0]
                string = args[1]
                start = args[2]
                return exp.Anonymous(this="instr", expressions=[string, substr, start])

        elif fname == "TRANSLATE":
            # TRANSLATE(str, from, to) -> translate(str, from, to) - DuckDB has this
            args = expression.expressions
            if len(args) == 3:
                return exp.Anonymous(this="translate", expressions=args)

        elif fname == "SOUNDEX":
            # SOUNDEX(str) - DuckDB doesn't have this natively
            # Pass through - may work with extensions
            pass

        elif fname == "REVERSE":
            # REVERSE(str) -> reverse(str) - DuckDB has this
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="reverse", expressions=args)

        elif fname == "REPEAT":
            # REPEAT(str, n) -> repeat(str, n) - DuckDB has this
            args = expression.expressions
            if len(args) == 2:
                return exp.Anonymous(this="repeat", expressions=args)

        elif fname == "ASCII":
            # ASCII(str) -> ascii(str) - DuckDB has this
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="ascii", expressions=args)

        elif fname == "CHR":
            # CHR(n) -> chr(n) - DuckDB has this
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="chr", expressions=args)

        elif fname == "UNICODE":
            # UNICODE(str) -> unicode(str) - DuckDB has this
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="unicode", expressions=args)

        elif fname == "STARTSWITH":
            # STARTSWITH(str, prefix) -> starts_with(str, prefix) - DuckDB has this
            args = expression.expressions
            if len(args) == 2:
                return exp.Anonymous(this="starts_with", expressions=args)

        elif fname == "ENDSWITH":
            # ENDSWITH(str, suffix) -> ends_with(str, suffix) - DuckDB has this
            args = expression.expressions
            if len(args) == 2:
                return exp.Anonymous(this="ends_with", expressions=args)

        # =========================================================================
        # DATE/TIME CONSTRUCTION FUNCTIONS
        # =========================================================================
        elif fname == "DATE_FROM_PARTS":
            # DATE_FROM_PARTS(year, month, day) -> make_date(year, month, day)
            args = expression.expressions
            if len(args) == 3:
                return exp.Anonymous(this="make_date", expressions=args)

        elif fname == "TIME_FROM_PARTS":
            # TIME_FROM_PARTS(hour, minute, second[, nanosecond]) -> make_time(hour, minute, second)
            args = expression.expressions
            if len(args) >= 3:
                # DuckDB make_time takes (hour, minute, second) - ignore nanoseconds
                return exp.Anonymous(this="make_time", expressions=args[:3])

        elif fname == "TIMESTAMP_FROM_PARTS":
            # TIMESTAMP_FROM_PARTS(year, month, day, hour, minute, second[, nanosecond])
            # -> make_timestamp(year, month, day, hour, minute, second)
            args = expression.expressions
            if len(args) >= 6:
                return exp.Anonymous(this="make_timestamp", expressions=args[:6])

        elif fname in ("TIMESTAMPADD", "TIMEADD"):
            # TIMESTAMPADD(unit, amount, date) -> date + INTERVAL amount unit
            # Same as DATEADD but with different argument order in some dialects
            args = expression.expressions
            if len(args) == 3:
                unit = args[0]
                amount = args[1]
                date_expr = args[2]
                # Get unit name
                if isinstance(unit, exp.Literal):
                    unit_str = unit.this.upper()
                elif isinstance(unit, exp.Var):
                    unit_str = unit.name.upper()
                else:
                    unit_str = "DAY"
                interval = exp.Interval(this=amount, unit=exp.Var(this=unit_str))
                return exp.Add(this=date_expr, expression=interval)

        elif fname == "TIMEDIFF":
            # TIMEDIFF(unit, time1, time2) -> date_diff(unit, time1, time2)
            args = expression.expressions
            if len(args) == 3:
                return exp.DateDiff(unit=args[0], this=args[2], expression=args[1])

        # =========================================================================
        # NUMERIC FUNCTIONS
        # =========================================================================
        elif fname == "CBRT":
            # CBRT(x) -> cbrt(x) - DuckDB has this
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="cbrt", expressions=args)

        elif fname == "FACTORIAL":
            # FACTORIAL(n) -> factorial(n) - DuckDB has this
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="factorial", expressions=args)

        elif fname == "DEGREES":
            # DEGREES(radians) -> degrees(radians) - DuckDB has this
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="degrees", expressions=args)

        elif fname == "RADIANS":
            # RADIANS(degrees) -> radians(degrees) - DuckDB has this
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="radians", expressions=args)

        elif fname == "PI":
            # PI() -> pi() - DuckDB has this
            return exp.Anonymous(this="pi", expressions=[])

        # =========================================================================
        # ARRAY FUNCTIONS
        # =========================================================================
        elif fname == "ARRAY_CAT":
            # ARRAY_CAT(array1, array2) -> list_concat(array1, array2)
            args = expression.expressions
            if len(args) == 2:
                return exp.Anonymous(this="list_concat", expressions=args)

        elif fname == "ARRAY_APPEND":
            # ARRAY_APPEND(array, elem) -> list_append(array, elem)
            args = expression.expressions
            if len(args) == 2:
                return exp.Anonymous(this="list_append", expressions=args)

        elif fname == "ARRAY_PREPEND":
            # ARRAY_PREPEND(array, elem) -> list_prepend(elem, array)
            # Note: DuckDB list_prepend has (element, array) order!
            args = expression.expressions
            if len(args) == 2:
                array = args[0]
                elem = args[1]
                return exp.Anonymous(this="list_prepend", expressions=[elem, array])

        elif fname == "ARRAY_SORT":
            # ARRAY_SORT(array) -> list_sort(array)
            args = expression.expressions
            if len(args) >= 1:
                return exp.Anonymous(this="list_sort", expressions=[args[0]])

        elif fname == "ARRAY_REVERSE":
            # ARRAY_REVERSE(array) -> list_reverse(array)
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="list_reverse", expressions=args)

        elif fname == "ARRAY_MIN":
            # ARRAY_MIN(array) -> list_min(array)
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="list_min", expressions=args)

        elif fname == "ARRAY_MAX":
            # ARRAY_MAX(array) -> list_max(array)
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="list_max", expressions=args)

        elif fname in ("ARRAY_SUM", "ARRAY_AGG_SUM"):
            # ARRAY_SUM(array) -> list_sum(array)
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="list_sum", expressions=args)

        elif fname in ("ARRAY_AVG", "ARRAY_AGG_AVG"):
            # ARRAY_AVG(array) -> list_avg(array)
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="list_avg", expressions=args)

        elif fname == "ARRAYS_OVERLAP":
            # ARRAYS_OVERLAP(array1, array2) -> list_has_any(array1, array2)
            args = expression.expressions
            if len(args) == 2:
                return exp.Anonymous(this="list_has_any", expressions=args)

        # =========================================================================
        # JSON / OBJECT FUNCTIONS
        # =========================================================================
        elif fname == "OBJECT_KEYS":
            # OBJECT_KEYS(obj) -> json_keys(obj)
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="json_keys", expressions=args)

        elif fname == "TO_JSON":
            # TO_JSON(value) -> to_json(value) - DuckDB has this
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="to_json", expressions=args)

        elif fname == "TO_VARIANT":
            # TO_VARIANT(value) -> to_json(value) - VARIANT is JSON in DuckDB
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="to_json", expressions=args)

        elif fname == "CHECK_JSON":
            # CHECK_JSON(str) -> NULL if valid, error message if invalid
            # DuckDB: json_valid returns true/false
            # Return NULL if valid, else error message
            args = expression.expressions
            if len(args) == 1:
                json_valid = exp.Anonymous(this="json_valid", expressions=args)
                return exp.Case(
                    ifs=[
                        exp.If(
                            this=json_valid,
                            true=exp.Null(),
                        )
                    ],
                    default=exp.Literal.string("Invalid JSON"),
                )

        # =========================================================================
        # AGGREGATE FUNCTIONS
        # =========================================================================
        elif fname == "ANY_VALUE":
            # ANY_VALUE(x) -> any_value(x) - DuckDB has this
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="any_value", expressions=args)

        elif fname == "BITAND_AGG":
            # BITAND_AGG(x) -> bit_and(x)
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="bit_and", expressions=args)

        elif fname == "BITOR_AGG":
            # BITOR_AGG(x) -> bit_or(x)
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="bit_or", expressions=args)

        elif fname == "BITXOR_AGG":
            # BITXOR_AGG(x) -> bit_xor(x)
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="bit_xor", expressions=args)

        elif fname == "BOOLAND_AGG":
            # BOOLAND_AGG(x) -> bool_and(x)
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="bool_and", expressions=args)

        elif fname == "BOOLOR_AGG":
            # BOOLOR_AGG(x) -> bool_or(x)
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="bool_or", expressions=args)

        elif fname == "KURTOSIS":
            # KURTOSIS(x) -> kurtosis(x) - DuckDB has this
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="kurtosis", expressions=args)

        elif fname == "SKEW":
            # SKEW(x) -> skewness(x) - DuckDB calls it skewness
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="skewness", expressions=args)

        elif fname == "COVAR_POP":
            # COVAR_POP(y, x) -> covar_pop(y, x) - DuckDB has this
            args = expression.expressions
            if len(args) == 2:
                return exp.Anonymous(this="covar_pop", expressions=args)

        elif fname == "COVAR_SAMP":
            # COVAR_SAMP(y, x) -> covar_samp(y, x) - DuckDB has this
            args = expression.expressions
            if len(args) == 2:
                return exp.Anonymous(this="covar_samp", expressions=args)

        # =========================================================================
        # HASH FUNCTIONS
        # =========================================================================
        elif fname == "HASH":
            # HASH(x) -> hash(x) - DuckDB has this
            args = expression.expressions
            return exp.Anonymous(this="hash", expressions=args)

        elif fname == "MD5_NUMBER_LOWER64":
            # MD5_NUMBER_LOWER64(x) -> hash(x) as approximation
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="hash", expressions=args)

        # =========================================================================
        # MISCELLANEOUS FUNCTIONS
        # =========================================================================
        elif fname == "IFF":
            # IFF(condition, true_val, false_val) -> IF(condition, true_val, false_val)
            # DuckDB supports IF function
            args = expression.expressions
            if len(args) == 3:
                return exp.If(this=args[0], true=args[1], false=args[2])

        elif fname == "NVL":
            # NVL(expr, default) -> COALESCE(expr, default)
            args = expression.expressions
            if len(args) == 2:
                return exp.Coalesce(this=args[0], expressions=[args[1]])

        elif fname == "NVL2":
            # NVL2(expr, not_null_val, null_val) -> IF(expr IS NOT NULL, not_null_val, null_val)
            args = expression.expressions
            if len(args) == 3:
                condition = exp.Not(this=exp.Is(this=args[0], expression=exp.Null()))
                return exp.If(this=condition, true=args[1], false=args[2])

        elif fname == "ZEROIFNULL":
            # ZEROIFNULL(expr) -> COALESCE(expr, 0)
            args = expression.expressions
            if len(args) == 1:
                return exp.Coalesce(this=args[0], expressions=[exp.Literal.number(0)])

        elif fname == "NULLIFZERO":
            # NULLIFZERO(expr) -> NULLIF(expr, 0)
            args = expression.expressions
            if len(args) == 1:
                return exp.Nullif(this=args[0], expression=exp.Literal.number(0))

        elif fname in ("TO_DECIMAL", "TO_NUMERIC"):
            # TO_DECIMAL/TO_NUMERIC are synonyms of TO_NUMBER. Snowflake parses
            # TO_NUMBER into exp.ToNumber but leaves these as Anonymous, so
            # without this branch they reach DuckDB verbatim and fail with
            # "Scalar Function with name to_decimal does not exist".
            args = expression.expressions
            if len(args) >= 1:
                precision, scale = _numeric_args_from_list(args[1:])
                return exp.Cast(this=args[0], to=_numeric_type(precision, scale))

        elif fname in ("TRY_TO_DECIMAL", "TRY_TO_NUMERIC"):
            args = expression.expressions
            if len(args) >= 1:
                precision, scale = _numeric_args_from_list(args[1:])
                return exp.TryCast(this=args[0], to=_numeric_type(precision, scale))

        elif fname == "TRY_TO_NUMBER":
            # TRY_TO_NUMBER(str [, precision [, scale]]) -> TRY_CAST(str AS ...)
            args = expression.expressions
            if len(args) >= 1:
                precision, scale = _numeric_args_from_list(args[1:])
                return exp.TryCast(this=args[0], to=_numeric_type(precision, scale))

        elif fname == "TRY_TO_DATE":
            # TRY_TO_DATE(str) -> TRY_CAST(str AS DATE)
            args = expression.expressions
            if len(args) >= 1:
                return exp.TryCast(this=args[0], to=exp.DataType.build("DATE"))

        elif fname == "TRY_TO_TIMESTAMP":
            # TRY_TO_TIMESTAMP(str) -> TRY_CAST(str AS TIMESTAMP)
            args = expression.expressions
            if len(args) >= 1:
                return exp.TryCast(this=args[0], to=exp.DataType.build("TIMESTAMP"))

        elif fname == "TRY_TO_BOOLEAN":
            # TRY_TO_BOOLEAN(x) -> TRY_CAST(x AS BOOLEAN)
            args = expression.expressions
            if len(args) == 1:
                return exp.TryCast(this=args[0], to=exp.DataType.build("BOOLEAN"))

        # =========================================================================
        # BASE64 ENCODING FUNCTIONS
        # =========================================================================
        elif fname == "BASE64_ENCODE":
            # BASE64_ENCODE(str) -> base64(encode(str))
            args = expression.expressions
            if len(args) == 1:
                encode_call = exp.Anonymous(this="encode", expressions=args)
                return exp.Anonymous(this="base64", expressions=[encode_call])

        elif fname == "BASE64_DECODE_STRING":
            # BASE64_DECODE_STRING(str) -> decode(from_base64(str))
            args = expression.expressions
            if len(args) == 1:
                from_base64_call = exp.Anonymous(this="from_base64", expressions=args)
                return exp.Anonymous(this="decode", expressions=[from_base64_call])

        elif fname == "BASE64_DECODE_BINARY":
            # BASE64_DECODE_BINARY(str) -> from_base64(str) (returns BLOB)
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="from_base64", expressions=args)

        # =========================================================================
        # ADDITIONAL ARRAY FUNCTIONS
        # =========================================================================
        elif fname == "ARRAY_EXCEPT":
            # ARRAY_EXCEPT(arr1, arr2) -> list_filter(arr1, x -> NOT list_contains(arr2, x))
            args = expression.expressions
            if len(args) == 2:
                arr1 = args[0]
                arr2 = args[1]
                # Build: list_filter(arr1, x -> NOT list_contains(arr2, x))
                lambda_param = exp.Identifier(this="x")
                list_contains_call = exp.Anonymous(
                    this="list_contains", expressions=[arr2, lambda_param]
                )
                negated = exp.Not(this=list_contains_call)
                lambda_expr = exp.Lambda(this=negated, expressions=[lambda_param])
                return exp.Anonymous(
                    this="list_filter", expressions=[arr1, lambda_expr]
                )

        elif fname == "ARRAY_INTERSECTION":
            # ARRAY_INTERSECTION(arr1, arr2) -> list_filter(arr1, x -> list_contains(arr2, x))
            args = expression.expressions
            if len(args) == 2:
                arr1 = args[0]
                arr2 = args[1]
                # Build: list_filter(arr1, x -> list_contains(arr2, x))
                lambda_param = exp.Identifier(this="x")
                list_contains_call = exp.Anonymous(
                    this="list_contains", expressions=[arr2, lambda_param]
                )
                lambda_expr = exp.Lambda(
                    this=list_contains_call, expressions=[lambda_param]
                )
                return exp.Anonymous(
                    this="list_filter", expressions=[arr1, lambda_expr]
                )

        elif fname == "ARRAY_DISTINCT":
            # ARRAY_DISTINCT(arr) -> list_distinct(arr)
            args = expression.expressions
            if len(args) == 1:
                return exp.Anonymous(this="list_distinct", expressions=args)

        # =========================================================================
        # TIMEZONE CONVERSION FUNCTIONS
        # =========================================================================
        elif fname == "CONVERT_TIMEZONE":
            # CONVERT_TIMEZONE(target_tz, ts) -> timezone(target_tz, ts)
            # CONVERT_TIMEZONE(source_tz, target_tz, ts) -> timezone(target_tz, timezone(source_tz, ts)::TIMESTAMPTZ)
            args = expression.expressions
            if len(args) == 2:
                # Two args: (target_tz, timestamp) - assume source is UTC
                target_tz = args[0]
                ts = args[1]
                return exp.Anonymous(this="timezone", expressions=[target_tz, ts])
            elif len(args) == 3:
                # Three args: (source_tz, target_tz, timestamp)
                source_tz = args[0]
                target_tz = args[1]
                ts = args[2]
                # First convert to UTC using source timezone, then to target
                # timezone(target_tz, timezone('UTC', timezone(source_tz, ts)))
                in_source = exp.Anonymous(this="timezone", expressions=[source_tz, ts])
                in_utc = exp.Anonymous(
                    this="timezone", expressions=[exp.Literal.string("UTC"), in_source]
                )
                return exp.Anonymous(this="timezone", expressions=[target_tz, in_utc])

        # =========================================================================
        # SHA HASH FUNCTIONS
        # =========================================================================
        elif fname == "SHA2":
            # SHA2(str) or SHA2(str, bits) -> sha256(str::BLOB) by default
            # Snowflake defaults to SHA-256 (256 bits)
            args = expression.expressions
            if len(args) >= 1:
                str_expr = args[0]
                # Cast to BLOB and apply sha256
                blob_cast = exp.Cast(this=str_expr, to=exp.DataType.build("BLOB"))
                return exp.Anonymous(this="sha256", expressions=[blob_cast])

        elif fname == "SHA1":
            # SHA1(str) -> sha1(str::BLOB)
            args = expression.expressions
            if len(args) == 1:
                str_expr = args[0]
                blob_cast = exp.Cast(this=str_expr, to=exp.DataType.build("BLOB"))
                return exp.Anonymous(this="sha1", expressions=[blob_cast])

        # =========================================================================
        # HEX ENCODING FUNCTIONS
        # =========================================================================
        elif fname == "HEX_ENCODE":
            # HEX_ENCODE(str) -> hex(str::BLOB)
            args = expression.expressions
            if len(args) >= 1:
                str_expr = args[0]
                blob_cast = exp.Cast(this=str_expr, to=exp.DataType.build("BLOB"))
                return exp.Anonymous(this="hex", expressions=[blob_cast])

        elif fname == "HEX_DECODE_STRING":
            # HEX_DECODE_STRING(hex_str) -> unhex(hex_str)::VARCHAR
            args = expression.expressions
            if len(args) >= 1:
                hex_expr = args[0]
                unhex_result = exp.Anonymous(this="unhex", expressions=[hex_expr])
                return exp.Cast(this=unhex_result, to=exp.DataType.build("VARCHAR"))

        elif fname == "HEX_DECODE_BINARY":
            # HEX_DECODE_BINARY(hex_str) -> unhex(hex_str)
            args = expression.expressions
            if len(args) >= 1:
                return exp.Anonymous(this="unhex", expressions=[args[0]])

    return expression
