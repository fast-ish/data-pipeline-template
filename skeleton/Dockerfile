# Build stage
FROM python:3.12-slim as builder

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-dev

# Runtime stage
FROM python:3.12-slim

WORKDIR /app

# OpenTelemetry configuration
ARG OTEL_SERVICE_VERSION="0.1.0"
ENV OTEL_SERVICE_VERSION=${OTEL_SERVICE_VERSION}
ENV OTEL_TRACES_EXPORTER=otlp
ENV OTEL_METRICS_EXPORTER=otlp

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy application code
COPY src/ ./src/

# Set Python path
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

# Create non-root user
RUN useradd --create-home --shell /bin/bash pipeline
USER pipeline

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import src; print('healthy')" || exit 1

ENTRYPOINT ["python", "-m", "src.cli"]
CMD ["run"]
