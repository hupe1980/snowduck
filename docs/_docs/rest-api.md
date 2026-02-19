---
layout: default
title: REST API
parent: Documentation
nav_order: 5
---

# REST API Server

SnowDuck includes a full REST API server that emulates Snowflake's SQL REST API and Snowpipe Streaming API.

## Starting the Server

### Command Line

```bash
# Install with server dependencies
pip install snowduck[server]

# Start the server
snowduck --host 0.0.0.0 --port 8000
```

### Docker

```bash
docker run -p 8000:8000 ghcr.io/hupe1980/snowduck:latest
```

### Programmatic

```python
from snowduck.server import create_app
import uvicorn

app = create_app(debug=True)
uvicorn.run(app, host="0.0.0.0", port=8000)
```

## SQL REST API

The SQL REST API allows executing SQL statements via HTTP.

### Submit a Statement

```bash
curl -X POST http://localhost:8000/api/v2/statements \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer dummy-token" \
  -d '{
    "statement": "SELECT 1 + 1 AS result",
    "database": "TESTDB",
    "schema": "PUBLIC"
  }'
```

**Response:**

```json
{
  "statementHandle": "01abc123-0000-0000-0000-000000000001",
  "message": "Statement executed successfully.",
  "statementStatusUrl": "/api/v2/statements/01abc123-0000-0000-0000-000000000001",
  "code": "090001",
  "resultSetMetaData": {
    "rowType": [
      {"name": "RESULT", "type": "fixed", "nullable": true}
    ],
    "numRows": 1
  },
  "data": [[2]]
}
```

### Get Statement Status

```bash
curl http://localhost:8000/api/v2/statements/{statementHandle} \
  -H "Authorization: Bearer dummy-token"
```

### Cancel a Statement

```bash
curl -X POST http://localhost:8000/api/v2/statements/{statementHandle}/cancel \
  -H "Authorization: Bearer dummy-token"
```

### Query with Parameters

```bash
curl -X POST http://localhost:8000/api/v2/statements \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer dummy-token" \
  -d '{
    "statement": "SELECT * FROM users WHERE id = ?",
    "bindings": {
      "1": {"type": "FIXED", "value": "123"}
    }
  }'
```

## Snowpipe Streaming API

SnowDuck fully emulates the Snowpipe Streaming API for testing streaming data applications.

### Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Your Streaming │────▶│    SnowDuck     │────▶│     DuckDB      │
│   Application   │     │     Server      │     │    (Storage)    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v2/streaming/hostname` | GET | Get streaming hostname |
| `/oauth/token` | POST | Exchange JWT for token |
| `/v2/streaming/.../pipes/{pipe}:pipe-info` | GET/POST | Get pipe metadata |
| `/v2/streaming/.../pipes/{pipe}/channels/{channel}` | PUT | Open channel |
| `/v2/streaming/.../pipes/{pipe}/channels/{channel}` | DELETE | Close channel |
| `/v2/streaming/data/.../channels/{channel}/rows` | POST | Append rows |
| `/v2/streaming/.../pipes/{pipe}:bulk-channel-status` | POST | Bulk status check |

### Using with Snowflake SDK

```python
# Note: Requires snowpipe-streaming package
from snowflake.streaming import StreamingIngestClient

client = StreamingIngestClient(
    scheme="http",      # Use HTTP for local testing
    host="127.0.0.1",
    port=8000,
    account="test",
    user="test",
    private_key="...",  # Can be dummy for SnowDuck
)

# Open a channel
channel = client.open_channel(
    database="MYDB",
    schema="PUBLIC", 
    table="EVENTS",
    channel_name="my_channel"
)

# Insert rows
channel.insert_rows([
    {"event_id": 1, "event_type": "click", "timestamp": "2024-01-15T10:00:00Z"},
    {"event_id": 2, "event_type": "view", "timestamp": "2024-01-15T10:01:00Z"},
])

# Close the channel
channel.close()
```

### Manual Channel Operations

**Open Channel:**
```bash
curl -X PUT http://localhost:8000/v2/streaming/databases/MYDB/schemas/PUBLIC/pipes/EVENTS-STREAMING/channels/my_channel \
  -H "Authorization: Bearer dummy-token" \
  -H "Content-Type: application/json" \
  -d '{}'
```

**Append Rows:**
```bash
curl -X POST http://localhost:8000/v2/streaming/data/databases/MYDB/schemas/PUBLIC/pipes/EVENTS-STREAMING/channels/my_channel/rows \
  -H "Authorization: Bearer dummy-token" \
  -H "Content-Type: application/json" \
  -d '[
    {"event_id": 1, "event_type": "click"},
    {"event_id": 2, "event_type": "view"}
  ]'
```

**Get Channel Status:**
```bash
curl http://localhost:8000/v2/streaming/databases/MYDB/schemas/PUBLIC/pipes/EVENTS-STREAMING/channels/my_channel/status \
  -H "Authorization: Bearer dummy-token"
```

## Connector API

Internal endpoints used by `snowflake-connector-python`:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/session/v1/login-request` | POST | Login authentication |
| `/queries/v1/query-request` | POST | Execute query |
| `/session/heartbeat-request` | POST | Keep session alive |

{: .note }
> These endpoints are automatically used when you connect via the Python connector. You don't need to call them directly.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SNOWDUCK_HOST` | `0.0.0.0` | Server bind address |
| `SNOWDUCK_PORT` | `8000` | Server port |
| `SNOWDUCK_STAGE_DIR` | `/tmp/snowduck_stage` | Stage file directory |
| `SNOWDUCK_STREAMING_HOSTNAME` | `localhost` | Streaming API hostname |

### Docker Configuration

```bash
docker run -p 9000:9000 \
  -e SNOWDUCK_PORT=9000 \
  -v /my/data:/data \
  ghcr.io/hupe1980/snowduck:latest \
  --port 9000 --db-file /data/snowduck.duckdb
```
