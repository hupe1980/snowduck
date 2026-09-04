"""System functions and current schema preprocessing."""

import json
from datetime import datetime, timedelta

from sqlglot import exp

from ..context import DialectContext


def preprocess_current_schema(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Convert current schema to the correct format."""
    if isinstance(expression, exp.CurrentSchema):
        return _named(
            exp.Literal.string(context.current_schema or "INFORMATION_SCHEMA"),
            "CURRENT_SCHEMA()",
            expression,
        )
    return expression


def _named(
    value: exp.Expression, column_name: str, original: exp.Expression
) -> exp.Expression:
    """Alias a substituted session value the way Snowflake names the column.

    Only bare projections get an alias - an expression nested inside a larger
    one must stay an expression.
    """
    if isinstance(original.parent, exp.Select):
        return exp.Alias(
            this=value, alias=exp.Identifier(this=column_name, quoted=True)
        )
    return value


# sqlglot models these as dedicated nodes rather than Anonymous calls.
_TYPED_SESSION_FUNCTIONS: dict[type[exp.Expression], str] = {
    node: name
    for node, name in (
        (getattr(exp, "CurrentRole", None), "CURRENT_ROLE"),
        (getattr(exp, "CurrentDatabase", None), "CURRENT_DATABASE"),
        (getattr(exp, "CurrentWarehouse", None), "CURRENT_WAREHOUSE"),
        (getattr(exp, "CurrentSecondaryRoles", None), "CURRENT_SECONDARY_ROLES"),
    )
    if node is not None
}


def preprocess_session_info(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
    """Replace session context functions with their configured values.

    This runs as an AST rewrite rather than a generator transform on
    ``exp.Select``: a generator transform has to re-render the whole statement
    as a string, which silently dropped everything after the projection list
    (so ``SELECT CURRENT_DATABASE(), COUNT(*) FROM t`` lost its FROM and
    counted one row).
    """
    typed = _TYPED_SESSION_FUNCTIONS.get(type(expression))
    if typed is not None:
        name = typed
    elif isinstance(expression, exp.Anonymous) and isinstance(expression.this, str):
        name = expression.this.upper()
    else:
        return expression

    if name == "CURRENT_ROLE":
        value = context.current_role or "SYSADMIN"
    elif name == "CURRENT_DATABASE":
        value = context.current_database or ""
    elif name == "CURRENT_WAREHOUSE":
        value = context.current_warehouse or "DEFAULT_WAREHOUSE"
    elif name == "CURRENT_SECONDARY_ROLES":
        value = json.dumps({"roles": "", "value": "ALL"})
    else:
        return expression

    return _named(exp.Literal.string(value), f"{name}()", expression)


def preprocess_system_calls(
    expression: exp.Expression, context: DialectContext
) -> exp.Expression:
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
            literal = exp.Literal.string(
                json.dumps(
                    {
                        "serverVersion": "9.8.1",
                        "currentSession": {
                            "id": 4711,
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
                    }
                )
            )
            alias = exp.Identifier(
                this="SYSTEM$BOOTSTRAP_DATA_REQUEST('ACCOUNT','CURRENT_SESSION','USER')",
                quoted=True,
            )
            return exp.Alias(this=literal, alias=alias)
    return expression
