"""Structured logging configuration."""

import logging
import sys
from typing import Any

import structlog
from opentelemetry import trace

from src.core.config import settings


def add_otel_context(
    logger: logging.Logger, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Add OpenTelemetry trace context to log events."""
    span = trace.get_current_span()
    if span and span.is_recording():
        ctx = span.get_span_context()
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
        event_dict["service.name"] = settings.otel_service_name
        event_dict["service.version"] = settings.otel_service_version
    return event_dict


def configure_logging() -> None:
    """Configure structured logging with OpenTelemetry integration."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.TimeStamper(fmt="iso"),
            add_otel_context,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a logger instance."""
    return structlog.get_logger(name)


# Configure on import
configure_logging()
