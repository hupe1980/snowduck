
from starlette.testclient import TestClient

from snowduck.server import app


def test_query_chunking(monkeypatch):
    monkeypatch.setenv("SNOWDUCK_CHUNK_SIZE", "2")

    with TestClient(app) as client:
        login_resp = client.post(
            "/session/v1/login-request?databaseName=db&schemaName=schema",
            json={},
        )
        assert login_resp.status_code == 200
        token = login_resp.json()["data"]["token"]

        headers = {"Authorization": f"Snowflake Token=\"{token}\""}
        query_resp = client.post(
            "/queries/v1/query-request",
            headers=headers,
            json={"sqlText": "SELECT * FROM range(5)", "queryResultFormat": "json"},
        )

        assert query_resp.status_code == 200
        data = query_resp.json()["data"]

        assert data["total"] == 5
        assert data["returned"] == 2
        assert data["chunkHeaders"]["chunkCount"] == 3
        assert len(data["chunks"]) == 2
        assert data["rowset"] == [[0], [1]]
        assert data["chunks"][0]["rowset"] == [[2], [3]]
        assert data["chunks"][1]["rowset"] == [[4]]

    monkeypatch.delenv("SNOWDUCK_CHUNK_SIZE", raising=False)
