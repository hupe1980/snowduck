from starlette.testclient import TestClient

from snowduck.server import app


def test_query_format_inferred_from_user_agent():
    with TestClient(app) as client:
        login_resp = client.post(
            "/session/v1/login-request?databaseName=db&schemaName=schema",
            json={},
        )
        token = login_resp.json()["data"]["token"]

        headers = {
            "Authorization": f'Snowflake Token="{token}"',
            "User-Agent": "snowflake-connector-nodejs/1.11.0",
        }

        query_resp = client.post(
            "/queries/v1/query-request",
            headers=headers,
            json={"sqlText": "SELECT 1"},
        )

        assert query_resp.status_code == 200
        data = query_resp.json()["data"]
        assert data["queryResultFormat"] == "json"
