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
        return exp.Literal.string(context.current_schema or "INFORMATION_SCHEMA")
    return expression


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
