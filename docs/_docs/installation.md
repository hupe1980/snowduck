---
layout: default
title: Installation
parent: Documentation
nav_order: 1
---

# Installation

## Requirements

- Python 3.11 or higher
- pip or uv package manager

## Basic Installation

Install SnowDuck from PyPI:

```bash
pip install snowduck
```

Or using uv:

```bash
uv add snowduck
```

## Server Installation

To use the REST API server (for Snowpipe Streaming or SQL REST API), install with the server extras:

```bash
pip install snowduck[server]
```

This includes:
- `starlette` - ASGI framework
- `uvicorn` - ASGI server
- `zstandard` - Compression for streaming data

## Development Installation

For contributing or running from source:

```bash
git clone https://github.com/hupe1980/snowduck.git
cd snowduck
uv sync
```

## Docker

Pull the pre-built Docker image:

```bash
docker pull ghcr.io/hupe1980/snowduck:latest
```

Run the server:

```bash
docker run -p 8000:8000 ghcr.io/hupe1980/snowduck:latest
```

## Verifying Installation

```python
import snowduck
from snowduck import start_patch_snowflake

# Should work without errors
start_patch_snowflake()
print("SnowDuck installed successfully!")
```
