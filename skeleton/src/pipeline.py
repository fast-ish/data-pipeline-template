"""Pipeline runner."""

from collections.abc import Iterator
from datetime import datetime
from typing import Any
from uuid import uuid4

from ddtrace import tracer

from src.core.config import settings
from src.core.logging import get_logger
from src.core.metrics import PipelineMetrics, track_pipeline_run
from src.core.notifications import get_notification_service
from src.extractors.base import BaseExtractor
from src.loaders.base import BaseLoader
from src.quality.base import BaseValidator
from src.transformers.base import BaseTransformer

logger = get_logger(__name__)


class Pipeline:
    """ETL Pipeline runner."""

    def __init__(
        self,
        name: str | None = None,
        extractor: BaseExtractor | None = None,
        transformers: list[BaseTransformer] | None = None,
        validators: list[BaseValidator] | None = None,
        loader: BaseLoader | None = None,
    ) -> None:
        self.name = name or settings.pipeline_name
        self.extractor = extractor
        self.transformers = transformers or []
        self.validators = validators or []
        self.loader = loader

        self.notification_service = get_notification_service()

    def _apply_transformers(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply all transformers in sequence."""
        result = data
        for transformer in self.transformers:
            result = transformer(result)
        return result

    def _apply_validators(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply all validators."""
        for validator in self.validators:
            data = validator(data)
        return data

    @tracer.wrap(name="pipeline.run")
    def run(self) -> PipelineMetrics:
        """Execute the pipeline."""
        run_id = str(uuid4())
        metrics = PipelineMetrics(
            pipeline_name=self.name,
            run_id=run_id,
            started_at=datetime.utcnow(),
        )

        logger.info("pipeline_started", run_id=run_id, pipeline=self.name)

        try:
            if self.extractor is None:
                raise ValueError("No extractor configured")

            for batch in self.extractor:
                # Transform
                transformed = self._apply_transformers(batch)

                # Validate
                validated = self._apply_validators(transformed)

                # Load
                if self.loader:
                    self.loader(validated)

                # Update metrics
                metrics.records_read += len(batch)
                metrics.records_written += len(validated)

            # Finalize metrics
            metrics.ended_at = datetime.utcnow()
            metrics.status = "success"

            if self.extractor:
                metrics.records_read = self.extractor.records_read
            if self.loader:
                metrics.records_written = self.loader.records_written
                metrics.bytes_processed = self.loader.bytes_written

            # Track and notify
            track_pipeline_run(metrics)
            self.notification_service.send_success(metrics)

            logger.info(
                "pipeline_completed",
                run_id=run_id,
                duration=metrics.duration_seconds,
                records_read=metrics.records_read,
                records_written=metrics.records_written,
            )

            return metrics

        except Exception as e:
            metrics.ended_at = datetime.utcnow()
            metrics.status = "failed"
            metrics.error_message = str(e)

            track_pipeline_run(metrics)
            self.notification_service.send_failure(metrics, e)

            logger.error(
                "pipeline_failed",
                run_id=run_id,
                error=str(e),
                duration=metrics.duration_seconds,
            )
            raise

        finally:
            if self.loader:
                self.loader.close()


class StreamingPipeline(Pipeline):
    """Streaming pipeline with continuous processing."""

    def __init__(
        self,
        name: str | None = None,
        extractor: BaseExtractor | None = None,
        transformers: list[BaseTransformer] | None = None,
        validators: list[BaseValidator] | None = None,
        loader: BaseLoader | None = None,
        checkpoint_interval: int = 100,
    ) -> None:
        super().__init__(
            name=name,
            extractor=extractor,
            transformers=transformers,
            validators=validators,
            loader=loader,
        )
        self.checkpoint_interval = checkpoint_interval
        self._batches_processed = 0

    def _checkpoint(self, metrics: PipelineMetrics) -> None:
        """Save checkpoint for recovery."""
        self._batches_processed += 1
        if self._batches_processed % self.checkpoint_interval == 0:
            logger.info(
                "checkpoint_saved",
                batches=self._batches_processed,
                records=metrics.records_written,
            )

    @tracer.wrap(name="streaming_pipeline.run")
    def run(self) -> PipelineMetrics:
        """Execute streaming pipeline with continuous processing."""
        run_id = str(uuid4())
        metrics = PipelineMetrics(
            pipeline_name=self.name,
            run_id=run_id,
            started_at=datetime.utcnow(),
        )

        logger.info("streaming_pipeline_started", run_id=run_id, pipeline=self.name)

        try:
            if self.extractor is None:
                raise ValueError("No extractor configured")

            for batch in self.extractor:
                with tracer.trace("process_batch"):
                    # Transform
                    transformed = self._apply_transformers(batch)

                    # Validate
                    validated = self._apply_validators(transformed)

                    # Load
                    if self.loader:
                        self.loader(validated)

                    # Update metrics
                    metrics.records_read += len(batch)
                    metrics.records_written += len(validated)

                    # Checkpoint
                    self._checkpoint(metrics)

            # Finalize
            metrics.ended_at = datetime.utcnow()
            metrics.status = "success"

            track_pipeline_run(metrics)
            self.notification_service.send_success(metrics)

            return metrics

        except Exception as e:
            metrics.ended_at = datetime.utcnow()
            metrics.status = "failed"
            metrics.error_message = str(e)

            track_pipeline_run(metrics)
            self.notification_service.send_failure(metrics, e)

            logger.error("streaming_pipeline_failed", run_id=run_id, error=str(e))
            raise

        finally:
            if self.loader:
                self.loader.close()
