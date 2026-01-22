# SnowDuck - Modern Task Runner
# Requires: just (https://github.com/casey/just)
# Usage: just <recipe>

# Default recipe (shown when running just with no arguments)
default:
    @just --list

# Variables
image_name := "snowduck"
container_name := "snowduck-container"

# Run all tests
test:
    pytest --maxfail=1 --disable-warnings -sv tests/

# Run tests with coverage
test-cov:
    pytest --cov=snowduck --cov-report=html --cov-report=term tests/

# Run tests in parallel
test-parallel:
    pytest -n auto tests/

# Run specific test file
test-file file:
    pytest -sv tests/{{file}}

# Run tests matching a pattern
test-match pattern:
    pytest -k "{{pattern}}" tests/

# Run linter
lint:
    ruff check .

# Run linter and fix issues
lint-fix:
    ruff check --fix .

# Format code
format:
    ruff format .

# Run mypy for type checking
mypy:
    mypy --strict snowduck

# Run all quality checks (lint, format check, mypy, test)
check: lint mypy test

# Install dependencies
install:
    uv sync

# Update dependencies
update:
    uv lock --upgrade

# Add a new dependency
add package:
    uv add {{package}}

# Add a new dev dependency
add-dev package:
    uv add --dev {{package}}

# Build Docker image
docker-build:
    docker build --tag {{image_name}} .

# Run Docker container
docker-run:
    docker run --rm -it -p 8000:8000 --name {{container_name}} {{image_name}}

# Stop & remove running container
docker-stop:
    -docker stop {{container_name}}
    -docker rm {{container_name}}

# Clean up untagged Docker images
docker-clean:
    docker image prune -f

# Full Docker rebuild (stop, clean, build, run)
docker-rebuild: docker-stop docker-clean docker-build docker-run

# Start the FastAPI server
serve:
    uvicorn snowduck.server:app --reload --host 0.0.0.0 --port 8000

# Run the server in production mode
serve-prod:
    uvicorn snowduck.server:app --host 0.0.0.0 --port 8000 --workers 4

# Clean Python cache files
clean:
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find . -type f -name "*.pyc" -delete
    find . -type f -name "*.pyo" -delete
    find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
    find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
    find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
    find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
    rm -rf htmlcov/ .coverage

# Show project statistics
stats:
    @echo "=== Project Statistics ==="
    @echo "Lines of code (Python):"
    @find snowduck -name "*.py" | xargs wc -l | tail -1
    @echo "\nTest files:"
    @find tests -name "test_*.py" | wc -l
    @echo "\nTotal tests:"
    @pytest --collect-only -q | tail -1

# Run benchmarks (if any)
bench:
    pytest tests/ -m benchmark --benchmark-only

# Generate documentation
docs:
    @echo "Documentation generation not yet configured"

# Initialize pre-commit hooks
init-hooks:
    @echo "Setting up git hooks..."
    @echo "#!/bin/sh\njust lint\njust mypy" > .git/hooks/pre-commit
    @chmod +x .git/hooks/pre-commit
    @echo "Pre-commit hook installed!"
