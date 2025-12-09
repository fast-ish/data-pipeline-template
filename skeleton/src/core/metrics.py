"""Pipeline metrics with OpenTelemetry."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from opentelemetry import metrics, trace

from src.core.config import settings
from src.core.logging import get_logger

logger = get_logger(__name__)

# Get meter for metrics
meter = metrics.get_meter(__name__)
tracer = trace.get_tracer(__name__)


@dataclass
class PipelineMetrics:
    """Container for pipeline execution metrics."""

    pipeline_name: str
    run_id: str
    started_at: datetime
    ended_at: datetime | None = None
    records_read: int = 0
    records_written: int = 0
    records_failed: int = 0
    bytes_processed: int = 0
    status: str = "running"
    error_message: str | None = None

    @property
    def duration_seconds(self) -> float | None:
        if self.ended_at:
            return (self.ended_at - self.started_at).total_seconds()
        return None

    @property
    def success_rate(self) -> float:
        total = self.records_read
        if total == 0:
            return 1.0
        return (total - self.records_failed) / total

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "duration_seconds": self.duration_seconds,
            "records_read": self.records_read,
            "records_written": self.records_written,
            "records_failed": self.records_failed,
            "bytes_processed": self.bytes_processed,
            "success_rate": self.success_rate,
            "status": self.status,
            "error_message": self.error_message,
        }


# Create metrics instruments
duration_histogram = meter.create_histogram(
    name="pipeline.duration_seconds",
    description="Pipeline execution duration",
    unit="s",
)
records_counter = meter.create_counter(
    name="pipeline.records",
    description="Records processed by pipeline",
    unit="1",
)
bytes_counter = meter.create_counter(
    name="pipeline.bytes_processed",
    description="Bytes processed by pipeline",
    unit="By",
)


def emit_metric(name: str, value: float, tags: dict[str, str] | None = None) -> None:
    """Emit a metric via OpenTelemetry."""
    all_tags = {
        "pipeline": settings.pipeline_name,
        "env": settings.environment,
        **(tags or {}),
    }

    # Add metric to current span if active
    span = trace.get_current_span()
    if span and span.is_recording():
        span.set_attribute(name, value)

    tag_list = [f"{k}:{v}" for k, v in all_tags.items()]
    logger.info(
        "metric_emitted",
        metric_name=name,
        metric_value=value,
        tags=tag_list,
    )


def track_pipeline_run(metrics: PipelineMetrics) -> None:
    """Track pipeline run metrics."""
    attributes = {
        "pipeline.name": metrics.pipeline_name,
        "pipeline.status": metrics.status,
        "service.name": settings.otel_service_name,
    }

    if metrics.duration_seconds:
        duration_histogram.record(metrics.duration_seconds, attributes)

    records_counter.add(metrics.records_read, {**attributes, "type": "read"})
    records_counter.add(metrics.records_written, {**attributes, "type": "written"})
    records_counter.add(metrics.records_failed, {**attributes, "type": "failed"})
    bytes_counter.add(metrics.bytes_processed, attributes)

    logger.info("pipeline_metrics", **metrics.to_dict())
