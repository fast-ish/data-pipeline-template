"""Tests for pipeline runner."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.pipeline import Pipeline, StreamingPipeline
from src.transformers.common import FilterTransformer
from src.quality.validators import NotNullValidator


class TestPipeline:
    def test_runs_pipeline(
        self,
        mock_extractor: Any,
        mock_loader: Any,
        sample_records: list[dict[str, Any]],
    ) -> None:
        with patch("src.pipeline.get_notification_service") as mock_notif:
            mock_notif.return_value = MagicMock()

            pipeline = Pipeline(
                name="test-pipeline",
                extractor=mock_extractor,
                loader=mock_loader,
            )

            metrics = pipeline.run()

            assert metrics.status == "success"
            assert metrics.records_read == len(sample_records)
            assert len(mock_loader.loaded_data) == len(sample_records)

    def test_applies_transformers(
        self,
        mock_extractor: Any,
        mock_loader: Any,
    ) -> None:
        with patch("src.pipeline.get_notification_service") as mock_notif:
            mock_notif.return_value = MagicMock()

            pipeline = Pipeline(
                name="test-pipeline",
                extractor=mock_extractor,
                transformers=[FilterTransformer(lambda r: r["status"] == "active")],
                loader=mock_loader,
            )

            metrics = pipeline.run()

            assert metrics.status == "success"
            # Only active records should be loaded (4 out of 5)
            assert len(mock_loader.loaded_data) == 4

    def test_applies_validators(
        self,
        mock_extractor: Any,
        mock_loader: Any,
    ) -> None:
        with patch("src.pipeline.get_notification_service") as mock_notif:
            mock_notif.return_value = MagicMock()

            pipeline = Pipeline(
                name="test-pipeline",
                extractor=mock_extractor,
                validators=[NotNullValidator(fields=["id"], fail_on_error=True)],
                loader=mock_loader,
            )

            metrics = pipeline.run()

            assert metrics.status == "success"

    def test_handles_failure(self, mock_loader: Any) -> None:
        with patch("src.pipeline.get_notification_service") as mock_notif:
            mock_notif.return_value = MagicMock()

            pipeline = Pipeline(
                name="test-pipeline",
                extractor=None,  # Will cause failure
                loader=mock_loader,
            )

            with pytest.raises(ValueError, match="No extractor configured"):
                pipeline.run()

    def test_sends_success_notification(
        self,
        mock_extractor: Any,
        mock_loader: Any,
    ) -> None:
        with patch("src.pipeline.get_notification_service") as mock_notif:
            mock_service = MagicMock()
            mock_notif.return_value = mock_service

            pipeline = Pipeline(
                name="test-pipeline",
                extractor=mock_extractor,
                loader=mock_loader,
            )

            pipeline.run()

            mock_service.send_success.assert_called_once()

    def test_sends_failure_notification(self, mock_loader: Any) -> None:
        with patch("src.pipeline.get_notification_service") as mock_notif:
            mock_service = MagicMock()
            mock_notif.return_value = mock_service

            pipeline = Pipeline(
                name="test-pipeline",
                extractor=None,
                loader=mock_loader,
            )

            with pytest.raises(ValueError):
                pipeline.run()

            mock_service.send_failure.assert_called_once()


class TestStreamingPipeline:
    def test_runs_streaming_pipeline(
        self,
        mock_extractor: Any,
        mock_loader: Any,
        sample_records: list[dict[str, Any]],
    ) -> None:
        with patch("src.pipeline.get_notification_service") as mock_notif:
            mock_notif.return_value = MagicMock()

            pipeline = StreamingPipeline(
                name="test-streaming-pipeline",
                extractor=mock_extractor,
                loader=mock_loader,
                checkpoint_interval=2,
            )

            metrics = pipeline.run()

            assert metrics.status == "success"
            assert metrics.records_read == len(sample_records)
