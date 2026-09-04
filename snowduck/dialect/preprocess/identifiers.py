"""Identifier and OBJECT/ARRAY_CONSTRUCT preprocessing."""

from typing import cast

import sqlglot
from sqlglot import exp

from ..context import DialectContext


def preprocess_identifier(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Convert identifier function to an identifier.

    See https://docs.snowflake.com/en/sql-reference/identifier-literal
    """
    if (
        isinstance(expression, exp.Anonymous)
        and isinstance(expression.this, str)
        and expression.this.upper() == "IDENTIFIER"
    ):
        expression = exp.Identifier(this=expression.expressions[0].this, quoted=False)

    return expression


_OBJECT_CONSTRUCT_TEMPLATE = (
    "CAST('{' || coalesce(list_aggregate("
    "list_filter(_PAIRS, x -> x IS NOT NULL), 'string_agg', ','"
    "), '') || '}' AS JSON)"
)


def _object_construct(args: list[exp.Expression], keep_null: bool) -> exp.Expression:
    """Build a DuckDB JSON object with Snowflake's NULL semantics.

    OBJECT_CONSTRUCT drops key/value pairs whose value is NULL, while
    OBJECT_CONSTRUCT_KEEP_NULL retains them as JSON nulls.

    The dropping variant renders each pair as JSON text and filters out the
    ones whose value is NULL, then splices the survivors together. A merge-patch
    would be shorter, but RFC 7386 gives null the meaning "delete this key"
    *recursively* - so a NULL nested inside an object-valued argument would be
    removed too, where Snowflake only drops at the top level.
    """
    obj = exp.Anonymous(this="json_object", expressions=args)
    if keep_null:
        return obj

    pairs: list[exp.Expression] = []
    for index in range(0, len(args) - 1, 2):
        key, value = args[index], args[index + 1]
        pair_text = exp.DPipe(
            this=exp.DPipe(
                this=exp.Anonymous(this="to_json", expressions=[key]),
                expression=exp.Literal.string(":"),
            ),
            expression=exp.Anonymous(this="to_json", expressions=[value]),
        )
        pairs.append(
            exp.Case(
                ifs=[
                    exp.If(
                        this=exp.Is(this=value.copy(), expression=exp.Null()),
                        true=exp.Null(),
                    )
                ],
                default=pair_text,
            )
        )

    if not pairs:
        return exp.Cast(this=exp.Literal.string("{}"), to=exp.DataType.build("JSON"))

    tree = cast(
        exp.Expression, sqlglot.parse_one(_OBJECT_CONSTRUCT_TEMPLATE, read="duckdb")
    )
    for node in list(tree.find_all(exp.Column)):
        if node.name == "_PAIRS":
            node.replace(exp.Array(expressions=pairs))
    return tree


def _row_source_name(select: exp.Select) -> str | None:
    """The name a DuckDB query uses to refer to its row as a struct.

    That is the FROM clause's alias, or the table's own name when it has none.
    Only a single source can be named this way; over a join, Snowflake's
    ``OBJECT_CONSTRUCT(*)`` spans both sides and DuckDB has no equivalent.
    """
    # sqlglot renamed the argument to `from_`; accept both spellings so the
    # lookup does not silently start returning None on an upgrade.
    from_clause = select.args.get("from_") or select.args.get("from")
    if not isinstance(from_clause, exp.From) or select.args.get("joins"):
        return None
    source = from_clause.this
    alias = source.args.get("alias") if isinstance(source, exp.Expression) else None
    if isinstance(alias, exp.TableAlias) and alias.this:
        return str(alias.this.name)
    if isinstance(alias, exp.Identifier):
        return str(alias.name)
    if isinstance(source, exp.Table):
        return str(source.name)
    if isinstance(source, exp.Subquery):
        # An unaliased subquery has no name to refer to, so give it one.
        name = "_snowduck_row"
        source.set("alias", exp.TableAlias(this=exp.to_identifier(name)))
        return name
    return None


def _expand_star_map(select: exp.Select) -> None:
    """Rewrite every ``OBJECT_CONSTRUCT(*)`` in a SELECT onto its row struct."""
    star_maps = list(select.find_all(exp.StarMap))
    if not star_maps:
        return

    name = _row_source_name(select)
    if name is None:
        raise ValueError(
            "OBJECT_CONSTRUCT(*) needs exactly one FROM source to read the row "
            "from; name the columns explicitly instead"
        )

    for star_map in star_maps:
        star_map.replace(
            exp.Anonymous(
                this="to_json", expressions=[exp.column(exp.to_identifier(name))]
            )
        )


def preprocess_semi_structured(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Pre-process expression to transform OBJECT_CONSTRUCT/ARRAY_CONSTRUCT and strip Time Travel."""

    # Snowflake's GET is two functions sharing a name: GET(array, index) reads
    # an element (0-based), GET(object, key) reads a member. sqlglot parses
    # both into GetExtract, whose DuckDB form is a bracket - and a bracket
    # applied to an object raises "Expected ARRAY, but got OBJECT", so a string
    # key becomes a JSON path instead.
    if isinstance(expression, exp.GetExtract):
        key = expression.expression
        if isinstance(key, exp.Literal) and key.is_string:
            return exp.Anonymous(
                this="json_extract",
                expressions=[expression.this, exp.Literal.string(f'$."{key.this}"')],
            )
        return exp.Bracket(this=expression.this, expressions=[key])

    # Handle JSONExtract - Snowflake parser converts GET_PATH to JSONExtract
    # We need to convert it to json_extract_string to get unquoted results
    if isinstance(expression, exp.JSONExtract):
        json_obj = expression.this
        path = expression.expression
        # Convert JSONPath to string literal
        if isinstance(path, exp.JSONPath):
            # Build the JSONPath string from its components
            parts = []
            for node in path.expressions:
                if isinstance(node, exp.JSONPathRoot):
                    parts.append("$")
                elif isinstance(node, exp.JSONPathKey):
                    parts.append(f".{node.this}")
                elif isinstance(node, exp.JSONPathSubscript):
                    parts.append(f"[{node.this}]")
                else:
                    parts.append(str(node))
            path_str = "".join(parts)
            path = exp.Literal.string(path_str)
        return exp.Anonymous(this="json_extract_string", expressions=[json_obj, path])

    # Handle JSONExtractScalar - Snowflake parser converts JSON_EXTRACT_PATH_TEXT to this
    # but loses the multiple key arguments. We need to check if there were multiple keys
    # and build the full path
    if isinstance(expression, exp.JSONExtractScalar):
        json_obj = expression.this
        path = expression.expression
        # Convert JSONPath to string literal
        if isinstance(path, exp.JSONPath):
            # Build the JSONPath string from its components
            parts = []
            for node in path.expressions:
                if isinstance(node, exp.JSONPathRoot):
                    parts.append("$")
                elif isinstance(node, exp.JSONPathKey):
                    parts.append(f".{node.this}")
                elif isinstance(node, exp.JSONPathSubscript):
                    parts.append(f"[{node.this}]")
                else:
                    parts.append(str(node))
            path_str = "".join(parts)
            path = exp.Literal.string(path_str)
        return exp.Anonymous(this="json_extract_string", expressions=[json_obj, path])

    # Handle ParseJSON - convert to CAST AS JSON or TRY_CAST for safe mode
    if isinstance(expression, exp.ParseJSON):
        str_expr = expression.this
        if expression.args.get("safe"):
            # TRY_PARSE_JSON -> TRY_CAST(str AS JSON)
            return exp.TryCast(this=str_expr, to=exp.DataType.build("JSON"))
        else:
            # PARSE_JSON -> CAST(str AS JSON)
            return exp.Cast(this=str_expr, to=exp.DataType.build("JSON"))

    # Handle OBJECT_CONSTRUCT parsed as Struct (Snowflake dialect does this)
    if isinstance(expression, exp.Struct):
        # Convert Struct(PropertyEQ(k, v), ...) to json_object(k, v, ...)
        new_args = []
        possible = True
        for e in expression.expressions:
            if isinstance(e, exp.PropertyEQ):
                new_args.append(e.this)
                new_args.append(e.expression)
            else:
                possible = False
                break

        if possible:
            # An empty OBJECT_CONSTRUCT() is still an (empty) object, not a
            # DuckDB struct literal - which would not even parse.
            return _object_construct(new_args, keep_null=False)

    # OBJECT_CONSTRUCT(*) - every column of the row as one object. sqlglot
    # parses it to StarMap. DuckDB has no expression that expands to the whole
    # row, but naming the source in a value position produces its row struct,
    # so the rewrite has to happen where the FROM clause is visible.
    if isinstance(expression, exp.Select) and expression.find(exp.StarMap):
        _expand_star_map(expression)

    # Handle explicit function calls (e.g. ARRAY_CONSTRUCT is parsed as exp.Array usually?)
    if isinstance(expression, exp.Anonymous):
        fname = expression.this.upper()

        # JSON functions
        if fname == "PARSE_JSON":
            # PARSE_JSON(str) -> CAST(str AS JSON)
            if len(expression.expressions) == 1:
                str_expr = expression.expressions[0]
                return exp.Cast(this=str_expr, to=exp.DataType.build("JSON"))
        elif fname == "OBJECT_CONSTRUCT":
            return _object_construct(expression.expressions, keep_null=False)
        elif fname == "OBJECT_CONSTRUCT_KEEP_NULL":
            return _object_construct(expression.expressions, keep_null=True)
        elif fname == "GET_PATH":
            # GET_PATH(json, 'path') -> json_extract_string(json, '$.path')
            # json_extract_string returns unquoted strings (unlike json_extract)
            if len(expression.expressions) >= 2:
                json_obj = expression.expressions[0]
                path = expression.expressions[1]
                # Convert path to JSONPath format (add $. prefix if not present)
                if isinstance(path, exp.Literal) and isinstance(path.this, str):
                    path_str = path.this
                    if not path_str.startswith("$"):
                        path_str = "$." + path_str
                    path = exp.Literal.string(path_str)
                return exp.Anonymous(
                    this="json_extract_string", expressions=[json_obj, path]
                )
        elif fname == "JSON_EXTRACT_PATH_TEXT":
            # JSON_EXTRACT_PATH_TEXT(json, 'key1', 'key2') -> json_extract_string(json, '$.key1.key2')
            # json_extract_string returns unquoted text values
            if len(expression.expressions) >= 2:
                json_obj = expression.expressions[0]
                keys = expression.expressions[1:]
                # Build path from keys
                key_strs = []
                for key in keys:
                    if isinstance(key, exp.Literal):
                        key_strs.append(str(key.this))
                path = "$." + ".".join(key_strs)
                return exp.Anonymous(
                    this="json_extract_string",
                    expressions=[json_obj, exp.Literal.string(path)],
                )

        # ARRAY functions
        elif fname == "ARRAY_CONSTRUCT":
            return exp.Array(expressions=expression.expressions)
        elif fname == "ARRAY_POSITION":
            # ARRAY_POSITION(value, array) -> CASE WHEN list_indexof(array, value) = 0 THEN NULL ELSE list_indexof(array, value) - 1 END
            # NOTE: Snowflake has (value, array) and uses 0-based indexing, returns NULL if not found
            # DuckDB list_indexof returns 1-based index, 0 if not found
            if len(expression.expressions) == 2:
                value = expression.expressions[0]
                array = expression.expressions[1]
                # Get 1-based index from DuckDB
                indexof_call = exp.Anonymous(
                    this="list_indexof", expressions=[array, value]
                )
                # Return NULL if not found (index = 0), else return index - 1 for 0-based
                return exp.Case(
                    ifs=[
                        exp.If(
                            this=exp.EQ(
                                this=indexof_call.copy(),
                                expression=exp.Literal.number(0),
                            ),
                            true=exp.Null(),
                        )
                    ],
                    default=exp.Sub(
                        this=indexof_call, expression=exp.Literal.number(1)
                    ),
                )
        elif fname == "GET" and len(expression.expressions) == 2:
            # sqlglot parses `GET(...)` into GetExtract, handled above; this
            # only catches a GET that reached here as an anonymous call.
            subject, key = expression.expressions
            if isinstance(key, exp.Literal) and key.is_string:
                return exp.Anonymous(
                    this="json_extract",
                    expressions=[subject, exp.Literal.string(f'$."{key.this}"')],
                )
            return exp.Bracket(this=subject, expressions=[key])
        elif fname == "ARRAY_SLICE":
            # ARRAY_SLICE(array, start, end) -> list_slice(array, start+1, end+1)
            # Snowflake uses 0-based indexing, DuckDB list_slice uses 1-based
            # Convert by adding 1 to both start and end indices
            if len(expression.expressions) == 3:
                array = expression.expressions[0]
                start = expression.expressions[1]
                end = expression.expressions[2]
                # Add 1 to convert from 0-based (Snowflake) to 1-based (DuckDB)
                start_plus_1 = exp.Add(this=start, expression=exp.Literal.number(1))
                end_plus_1 = exp.Add(this=end, expression=exp.Literal.number(1))
                return exp.Anonymous(
                    this="list_slice", expressions=[array, start_plus_1, end_plus_1]
                )
        elif fname == "ARRAY_COMPACT":
            # ARRAY_COMPACT(array) -> list_filter(array, x -> x IS NOT NULL)
            if len(expression.expressions) == 1:
                array = expression.expressions[0]
                # Use list_filter with lambda: keep elements that are NOT NULL
                lambda_expr = exp.Lambda(
                    this=exp.Not(
                        this=exp.Is(
                            this=exp.Identifier(this="x"), expression=exp.Null()
                        )
                    ),
                    expressions=[exp.Identifier(this="x")],
                )
                return exp.Anonymous(
                    this="list_filter", expressions=[array, lambda_expr]
                )
        elif fname == "FLATTEN" or fname == "TABLE":
            # FLATTEN or TABLE(FLATTEN(...)) -> unnest
            # This is tricky because it's a table function
            pass

    # Handle Time Travel: FROM table AT(...) -> stored in 'when' arg as HistoricalData
    if isinstance(expression, exp.Table):
        if expression.args.get("when"):
            expression.set("when", None)

    # Handle ArraySlice - sqlglot parses this as exp.ArraySlice, not Anonymous
    if isinstance(expression, exp.ArraySlice):
        # ARRAY_SLICE(array, start, end) -> list_slice(array, start+1, end+1)
        # Snowflake uses 0-based indexing, DuckDB list_slice uses 1-based
        # Convert by adding 1 to both start and end indices
        array = expression.this
        start = expression.args.get("start")
        end = expression.args.get("end")
        if array and start is not None and end is not None:
            # Add 1 to convert from 0-based (Snowflake) to 1-based (DuckDB)
            start_plus_1 = exp.Add(this=start, expression=exp.Literal.number(1))
            end_plus_1 = exp.Add(this=end, expression=exp.Literal.number(1))
            return exp.Anonymous(
                this="list_slice", expressions=[array, start_plus_1, end_plus_1]
            )

    return expression
