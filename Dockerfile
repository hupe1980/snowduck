# SnowDuck Server Docker Image
# Runs the Snowflake-compatible REST API backed by DuckDB

FROM python:3.11-slim AS builder

WORKDIR /app

# Install build dependencies
RUN pip install --no-cache-dir uv

# Copy project files
COPY pyproject.toml README.md ./
COPY snowduck/ ./snowduck/

# Build the wheel
RUN uv build --wheel

# Runtime stage
FROM python:3.11-slim

WORKDIR /app

# Install the package with server dependencies
COPY --from=builder /app/dist/ /tmp/dist/
RUN pip install --no-cache-dir "$(ls /tmp/dist/*.whl)[server]" && rm -rf /tmp/dist

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash snowduck
USER snowduck

# Default environment variables
ENV SNOWDUCK_HOST=0.0.0.0
ENV SNOWDUCK_PORT=8000

# Expose the default port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:${SNOWDUCK_PORT}/v2/streaming/hostname')" || exit 1

# Run the server
ENTRYPOINT ["snowduck"]
CMD ["--host", "0.0.0.0", "--port", "8000"]
