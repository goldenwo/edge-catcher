# Edge Catcher — production Dockerfile.
#
# Builds a single image that can run any of the entry points:
#   - python -m edge_catcher paper-trade --config config.local/paper-trader.yaml
#   - python -m edge_catcher.reporting --db data/paper_trades.db --notify ...
#   - uvicorn api.main:app --host 0.0.0.0 --port 8000
#
# Override CMD at compose time per service. See docker-compose.yml + the
# deployment-docker doc for the canonical service shapes.
#
# Multi-stage build keeps the runtime image small by separating the
# pip-install layer from the source-copy layer.

# ---------------------------------------------------------------------------
# Stage 1: builder — installs deps in a venv we'll copy into the runtime stage
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

# Build deps for pandas/numpy/scipy wheels and httpx's Rust deps.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /build
# Copy only pyproject.toml first so dep-install layer caches when code changes.
COPY pyproject.toml ./
COPY edge_catcher/__init__.py ./edge_catcher/__init__.py
COPY api/__init__.py ./api/__init__.py
RUN pip install --upgrade pip && \
    pip install -e ".[live,ai,ui]"

# Now copy the rest of the source.
COPY . .
# Re-install in editable mode now that all sources are present so console
# scripts wire up correctly.
RUN pip install -e ".[live,ai,ui]"


# ---------------------------------------------------------------------------
# Stage 2: runtime — slim image with venv + source copied over
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

# Runtime deps only (curl for healthchecks; no compilers here).
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for the paper trader process. Don't run as root in prod.
RUN groupadd --system edge && useradd --system --gid edge --create-home edge

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder --chown=edge:edge /build /app

WORKDIR /app
USER edge

# Default to the paper trader; compose overrides per-service.
CMD ["python", "-m", "edge_catcher", "paper-trade", "--config", "config.local/paper-trader.yaml"]
