"""Identifier and OBJECT/ARRAY_CONSTRUCT preprocessing."""

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


def preprocess_semi_structured(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Pre-process expression to transform OBJECT_CONSTRUCT/ARRAY_CONSTRUCT and strip Time Travel."""

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
                    parts.append('$')
                elif isinstance(node, exp.JSONPathKey):
                    parts.append(f'.{node.this}')
                elif isinstance(node, exp.JSONPathSubscript):
                    parts.append(f'[{node.this}]')
                else:
                    parts.append(str(node))
            path_str = ''.join(parts)
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
                    parts.append('$')
                elif isinstance(node, exp.JSONPathKey):
                    parts.append(f'.{node.this}')
                elif isinstance(node, exp.JSONPathSubscript):
                    parts.append(f'[{node.this}]')
                else:
                    parts.append(str(node))
            path_str = ''.join(parts)
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

        if possible and new_args:
            return exp.Anonymous(this="json_object", expressions=new_args)

    # Handle OBJECT_CONSTRUCT(*) parsed as StarMap
    if isinstance(expression, exp.StarMap):
        # map(*) equivalent -> to_json(row(*))
        # row(*) creates a struct with all cols
        return exp.Anonymous(
            this="to_json",
            expressions=[exp.Anonymous(this="row", expressions=[exp.Star()])],
        )

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
            return exp.Anonymous(this="json_object", expressions=expression.expressions)
        elif fname == "GET_PATH":
            # GET_PATH(json, 'path') -> json_extract_string(json, '$.path')
            # json_extract_string returns unquoted strings (unlike json_extract)
            if len(expression.expressions) >= 2:
                json_obj = expression.expressions[0]
                path = expression.expressions[1]
                # Convert path to JSONPath format (add $. prefix if not present)
                if isinstance(path, exp.Literal) and isinstance(path.this, str):
                    path_str = path.this
                    if not path_str.startswith('$'):
                        path_str = '$.' + path_str
                    path = exp.Literal.string(path_str)
                return exp.Anonymous(this="json_extract_string", expressions=[json_obj, path])
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
                path = '$.' + '.'.join(key_strs)
                return exp.Anonymous(this="json_extract_string", expressions=[json_obj, exp.Literal.string(path)])
        
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
                indexof_call = exp.Anonymous(this="list_indexof", expressions=[array, value])
                # Return NULL if not found (index = 0), else return index - 1 for 0-based
                return exp.Case(
                    ifs=[
                        exp.If(
                            this=exp.EQ(this=indexof_call.copy(), expression=exp.Literal.number(0)),
                            true=exp.Null(),
                        )
                    ],
                    default=exp.Sub(this=indexof_call, expression=exp.Literal.number(1)),
                )
        elif fname == "GET" and len(expression.expressions) == 2:
            # GET(array, index) -> array[index]
            # Snowflake GET uses 0-based indexing
            # sqlglot's Bracket auto-converts 0-based to 1-based for DuckDB
            # So we just pass the index directly
            array = expression.expressions[0]
            index = expression.expressions[1]
            return exp.Bracket(this=array, expressions=[index])
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
                return exp.Anonymous(this="list_slice", expressions=[array, start_plus_1, end_plus_1])
        elif fname == "ARRAY_COMPACT":
            # ARRAY_COMPACT(array) -> list_filter(array, x -> x IS NOT NULL)
            if len(expression.expressions) == 1:
                array = expression.expressions[0]
                # Use list_filter with lambda: keep elements that are NOT NULL
                lambda_expr = exp.Lambda(
                    this=exp.Not(
                        this=exp.Is(
                            this=exp.Identifier(this="x"),
                            expression=exp.Null()
                        )
                    ),
                    expressions=[exp.Identifier(this="x")]
                )
                return exp.Anonymous(this="list_filter", expressions=[array, lambda_expr])
        elif fname == "FLATTEN" or fname == "TABLE":
            # FLATTEN or TABLE(FLATTEN(...)) -> unnest
            # This is tricky because it's a table function
            pass

    # Handle Time Travel: FROM table AT(...) -> stored in 'when' arg as HistoricalData
    if isinstance(expression, exp.Table):
        if expression.args.get("when"):
            expression.set("when", None)

    # Handle TABLE(FLATTEN/EXPLODE(...)) -> UNNEST for DuckDB
    # Snowflake: SELECT value FROM TABLE(FLATTEN(INPUT => array))
    # DuckDB:    SELECT value FROM (SELECT UNNEST(array) AS value)
    if isinstance(expression, exp.TableFromRows):
        inner = expression.this
        if isinstance(inner, exp.Explode):
            # Extract the array from EXPLODE
            # Could be direct array or Kwarg(INPUT => array)
            array_expr = inner.this
            if isinstance(array_expr, exp.Kwarg):
                # INPUT => array - extract the array part
                array_expr = array_expr.expression
            # Transform to subquery with UNNEST aliased as 'value'
            # DuckDB: (SELECT UNNEST([1,2,3]) AS value) AS _flatten
            subquery = exp.Select(expressions=[
                exp.Alias(
                    this=exp.Unnest(expressions=[array_expr]),
                    alias=exp.Identifier(this="value")
                )
            ])
            return exp.Subquery(this=subquery, alias=exp.Identifier(this="_flatten"))

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
            return exp.Anonymous(this="list_slice", expressions=[array, start_plus_1, end_plus_1])

    # Handle Snowflake semi-structured types in DDL -> Convert appropriately
    # Snowflake: ARRAY, VARIANT, OBJECT are semi-structured types
    # DuckDB:
    #   - ARRAY without element type -> INT[] (most common case, arrays of integers)
    #   - VARIANT -> JSON (flexible container)
    #   - OBJECT -> JSON (key-value container)
    if isinstance(expression, exp.DataType):
        if expression.this == exp.DataType.Type.ARRAY:
            # Check if it has an element type specified
            if not expression.expressions:
                # No element type - use INT[] as default
                return exp.DataType.build("INT[]")
        elif expression.this in (exp.DataType.Type.VARIANT, exp.DataType.Type.OBJECT):
            # Replace with JSON type
            return exp.DataType.build("JSON")

    return expression
