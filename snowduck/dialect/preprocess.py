import json
from datetime import datetime, timedelta

from sqlglot import exp

from .context import DialectContext


def preprocess_variables(expression: exp.Expression, context: DialectContext) -> exp.Expression:
    """
    Substitute $var placeholders with their values from session variables.
    
    Snowflake: SELECT $my_var WHERE id = $filter_id
    DuckDB: SELECT 'hello' WHERE id = 1
    """
    # Check if this is a Parameter node (SQLGlot uses this for $var in Snowflake)
    if isinstance(expression, exp.Parameter):
        # Parameter has this=Var(this=var_name)
        if isinstance(expression.this, exp.Var):
            var_name = expression.this.this.upper() if isinstance(expression.this.this, str) else expression.this.name.upper()
            if var_name in context.session_variables:
                value = context.session_variables[var_name]
                # Try to determine if it's a number or string
                try:
                    # Try integer
                    int_val = int(value)
                    return exp.Literal.number(int_val)
                except ValueError:
                    try:
                        # Try float
                        float_val = float(value)
                        return exp.Literal.number(float_val)
                    except ValueError:
                        # It's a string
                        return exp.Literal.string(value)
            else:
                raise ValueError(f"Undefined session variable: ${var_name}")
    
    # Also check Placeholder (in case syntax varies)
    if isinstance(expression, exp.Placeholder):
        var_name = expression.name.upper()
        if var_name in context.session_variables:
            value = context.session_variables[var_name]
            try:
                int_val = int(value)
                return exp.Literal.number(int_val)
            except ValueError:
                try:
                    float_val = float(value)
                    return exp.Literal.number(float_val)
                except ValueError:
                    return exp.Literal.string(value)
        else:
            raise ValueError(f"Undefined session variable: ${var_name}")
    
    return expression


def preprocess_identifier(expression: exp.Expression, context: DialectContext) -> exp.Expression:
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

def preprocess_info_schema(expression: exp.Expression, context: DialectContext) -> exp.Expression:
    if (
        isinstance(expression, exp.Table)
        and (
            expression.db.upper() == "INFORMATION_SCHEMA"
            or (context.current_schema and context.current_schema.upper() == "INFORMATION_SCHEMA")
        )
        and expression.name.upper() == "DATABASES"
    ):  
        return exp.Table(
            this=exp.Identifier(this="_DATABASES", quoted=False),
            db=exp.Identifier(this=f"{context.current_database}.{context.info_schema_manager.info_schema_name}", quoted=False),
        )
    
    return expression

def preprocess_current_schema(expression: exp.Expression, context: DialectContext) -> exp.Expression:
    """Convert current schema to the correct format."""
    if isinstance(expression, exp.CurrentSchema):
        return exp.Literal.string(context.current_schema or "INFORMATION_SCHEMA")
    return expression

def preprocess_system_calls(expression: exp.Expression, context: DialectContext) -> exp.Expression:
    """Convert system calls to the correct format."""
    if isinstance(expression, exp.Func):
        if expression.name == "SYSTEM$BOOTSTRAP_DATA_REQUEST":
            ten_days_ago = datetime.now() - timedelta(days=10)
            three_days_ago = datetime.now() - timedelta(days=3)
            ten_days_ago_ts = int(ten_days_ago.timestamp() * 1000)
            three_days_ago_ts = int(three_days_ago.timestamp() * 1000)

            current_database = context.current_database or "SNOWFLAKE"
            current_schema = context.current_schema or "INFORMATION_SCHEMA"
            current_role = context.current_role or "SYSADMIN"
            current_warehouse = context.current_warehouse or "DEFAULT_WAREHOUSE"
            literal = exp.Literal.string(json.dumps({
                "serverVersion": "9.8.1",
                "currentSession": {
                    "id": 4711, # numeric 14 digits long
                    "idAsString": "4711",
                    "isActive": True,
                    "accountName": "SD4711",
                    "currentWarehouse": current_warehouse,
                    "currentDatabase": current_database,
                    "currentSchema": current_schema,
                },
                "accountInfo": {
                    "availableRegions": {
                        "PUBLIC.AWS_US_EAST_1": {
                            "snowflakeRegion": "AWS_US_EAST_1",
                            "regionGroup": "PUBLIC",
                            "cloud": "aws",
                            "cloudRegion": "us-east-1",
                            "cloudRegionName": "US East (N. Virginia)",
                            "regionGroupType": "PUBLIC",
                        },
                    },
                    "currentDeploymentLocation": "PUBLIC.AWS_US_EAST_1",
                    "accountAlias": "SNOWDUCK",
                    "region": "us-east-1",
                },
                "userInfo": {
                    "loginName": "USER@SNOWDUCK.ORG",
                    "firstName": "FirstName",
                    "lastName": "LastName",
                    "email": "user@snowduck.org",
                    "createdOn": ten_days_ago_ts,
                    "defaultRole": current_role,
                    "defaultNameSapce": None,
                    "defaultWarehouse": current_warehouse,
                    "validationState": "VALIDATED",
                    "lastSucLogin": three_days_ago_ts,
                },
            }))
            alias = exp.Identifier(
                this="SYSTEM$BOOTSTRAP_DATA_REQUEST('ACCOUNT','CURRENT_SESSION','USER')",
                quoted=True
            )
            return exp.Alias(this=literal, alias=alias)
    return expression

def preprocess_semi_structured(expression: exp.Expression, context: DialectContext) -> exp.Expression:
    """Pre-process expression to transform OBJECT_CONSTRUCT/ARRAY_CONSTRUCT and strip Time Travel."""
    
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
            expressions=[exp.Anonymous(this="row", expressions=[exp.Star()])]
        )

    # Handle explicit function calls (e.g. ARRAY_CONSTRUCT is parsed as exp.Array usually?)
    # Let's check ARRAY_CONSTRUCT parsing too?
    if isinstance(expression, exp.Anonymous):
        fname = expression.this.upper()
        if fname == "OBJECT_CONSTRUCT":
            return exp.Anonymous(this="json_object", expressions=expression.expressions)
        elif fname == "ARRAY_CONSTRUCT":
            return exp.Array(expressions=expression.expressions)

    # Handle Time Travel: FROM table AT(...) -> stored in 'when' arg as HistoricalData
    if isinstance(expression, exp.Table):
        if expression.args.get("when"):
             expression.set("when", None)
             
    return expression


def preprocess_generator(expression: exp.Expression, context: DialectContext) -> exp.Expression:
    """Pre-process GENERATOR table functions into DuckDB generate_series."""

    # sqlglot parses TABLE(GENERATOR(...)) as TableFromRows containing the Anonymous function.
    # We need to catch this specific expression type and transform it.
    
    if isinstance(expression, exp.TableFromRows):
        if isinstance(expression.this, exp.Anonymous) and expression.this.this.upper() == "GENERATOR":
             gen_expr = expression.this
             rowcount = None
             
             # Extract ROWCOUNT
             for arg in gen_expr.expressions:
                 if isinstance(arg, exp.Kwarg) and arg.this.name.upper() == "ROWCOUNT":
                     rowcount = arg.expression
                 elif isinstance(arg, exp.PropertyEQ) and arg.this.name.upper() == "ROWCOUNT":
                     rowcount = arg.expression
             
             if rowcount:
                 # DuckDB uses generate_series(start, stop)
                 # We replace TableFromRows with a Table wrapping the function, or just the function if possible.
                 # But FROM clause expects a Table-like thing usually?
                 # DuckDB `FROM generate_series(...)` works.
                 # sqlglot `generate_series` is likely a Func.
                 
                 # NOTE: We can't return a Func directly inside a FROM if sqlglot expects a Table.
                 # But wait, DuckDB FROM can take a function call. 
                 # Let's try returning just the function call.
                 # Must use exp.Anonymous or exp.Func with explicit name if using generic Func class
                 return exp.Anonymous(this="generate_series", expressions=[exp.Literal.number(1), rowcount])

    return expression


def preprocess_seq_functions(expression: exp.Expression, context: DialectContext) -> exp.Expression:
    """Transform sequence generation functions like SEQ4() to DuckDB equivalents."""
    
    # SEQ4(), SEQ8() -> ROW_NUMBER() OVER () (usually 0-indexed in Snowflake but 1-based in DuckDB window functions)
    # Actually Snowflake SEQ4 is "Returns a sequence of 4-byte integers... values are not necessarily unique or gap-free"
    # But for testing  it's usually used for simple IDs.
    # We can map it to ROW_NUMBER() OVER () - 1 if we want strict 0-start, or just ROW_NUMBER().
    
    # Note: exp.Func instances are handled by exp.Anonymous check below

    if isinstance(expression, exp.Anonymous):
        fname = expression.this.upper()
        if fname in ("SEQ1", "SEQ2", "SEQ4", "SEQ8"):
             # Transform to ROW_NUMBER() OVER () - 1
             # row_number() is 1 based.
             # Let's just use row_number() over (order by null) - 1
             
             # Create: (ROW_NUMBER() OVER ()) - 1
             window = exp.Window(this=exp.Anonymous(this="row_number"), over=exp.WindowSpec())
             return exp.Sub(this=window, expression=exp.Literal.number(1))
             
        elif fname == "UNIFORM":
             # UNIFORM(min, max, seed)
             # -> floor(random() * (max - min + 1) + min)
             # arguments: min=args[0], max=args[1], seed=args[2]
             args = expression.expressions
             if len(args) >= 2:
                 min_val = args[0]
                 max_val = args[1]
                 # Seed is ignored in DuckDB random() usually unless using setseed
                 
                 # Logic: (random() * (max - min + 1)) + min
                 # Then cast to int or floor? Snowflake UNIFORM returns number.
                 # Let's use CAST(FLOOR(...) AS INT)
                 
                 # (max - min + 1)
                 range_len = exp.Add(this=exp.Sub(this=max_val, expression=min_val), expression=exp.Literal.number(1))
                 
                 # random() * range_len
                 rand_scaled = exp.Mul(this=exp.Anonymous(this="random"), expression=range_len)
                 
                 # + min
                 shifted = exp.Add(this=exp.Floor(this=rand_scaled), expression=min_val)
                 
                 return exp.Cast(this=shifted, to=exp.DataType.build("int"))

    return expression

