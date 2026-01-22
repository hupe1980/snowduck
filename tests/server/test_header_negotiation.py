from starlette.testclient import TestClient

from snowduck.server import app


def test_query_format_inferred_from_accept_header():
    with TestClient(app) as client:
        login_resp = client.post(
            "/session/v1/login-request?databaseName=db&schemaName=schema",
            json={},
        )
        token = login_resp.json()["data"]["token"]

        headers = {
            "Authorization": f'Snowflake Token="{token}"',
            "Accept": "application/json",
        }

        query_resp = client.post(
            "/queries/v1/query-request",
            headers=headers,
            json={"sqlText": "SELECT 1"},
        )

        assert query_resp.status_code == 200
        data = query_resp.json()["data"]
        assert data["queryResultFormat"] == "json"
