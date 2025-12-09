"""Pipeline notifications."""

from abc import ABC, abstractmethod
from typing import Any

{%- if values.notifications == "slack" %}
from slack_sdk.webhook import WebhookClient
{%- endif %}
{%- if values.notifications == "sns" %}
import boto3
{%- endif %}

from src.core.config import settings
from src.core.logging import get_logger
from src.core.metrics import PipelineMetrics

logger = get_logger(__name__)


class NotificationService(ABC):
    """Base notification service."""

    @abstractmethod
    def send_success(self, metrics: PipelineMetrics) -> None:
        """Send success notification."""
        pass

    @abstractmethod
    def send_failure(self, metrics: PipelineMetrics, error: Exception) -> None:
        """Send failure notification."""
        pass


{%- if values.notifications == "slack" %}


class SlackNotificationService(NotificationService):
    """Slack notification service."""

    def __init__(self) -> None:
        self.client = WebhookClient(settings.slack_webhook_url)
        self.channel = settings.slack_channel

    def send_success(self, metrics: PipelineMetrics) -> None:
        """Send success notification to Slack."""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"✅ Pipeline {metrics.pipeline_name} completed",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Run ID:*\n{metrics.run_id}"},
                    {"type": "mrkdwn", "text": f"*Duration:*\n{metrics.duration_seconds:.1f}s"},
                    {"type": "mrkdwn", "text": f"*Records:*\n{metrics.records_written:,}"},
                    {"type": "mrkdwn", "text": f"*Success Rate:*\n{metrics.success_rate:.1%}"},
                ],
            },
        ]
        self.client.send(blocks=blocks)
        logger.info("slack_notification_sent", status="success", run_id=metrics.run_id)

    def send_failure(self, metrics: PipelineMetrics, error: Exception) -> None:
        """Send failure notification to Slack."""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"❌ Pipeline {metrics.pipeline_name} failed",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Run ID:*\n{metrics.run_id}"},
                    {"type": "mrkdwn", "text": f"*Error:*\n{str(error)[:200]}"},
                ],
            },
        ]
        self.client.send(blocks=blocks)
        logger.info("slack_notification_sent", status="failure", run_id=metrics.run_id)
{%- endif %}


{%- if values.notifications == "sns" %}


class SNSNotificationService(NotificationService):
    """AWS SNS notification service."""

    def __init__(self) -> None:
        self.client = boto3.client("sns", region_name=settings.aws_region)
        self.topic_arn = settings.sns_topic_arn

    def send_success(self, metrics: PipelineMetrics) -> None:
        """Send success notification to SNS."""
        message = {
            "pipeline": metrics.pipeline_name,
            "run_id": metrics.run_id,
            "status": "success",
            "records_written": metrics.records_written,
            "duration_seconds": metrics.duration_seconds,
        }
        self.client.publish(
            TopicArn=self.topic_arn,
            Subject=f"Pipeline {metrics.pipeline_name} completed",
            Message=str(message),
        )
        logger.info("sns_notification_sent", status="success", run_id=metrics.run_id)

    def send_failure(self, metrics: PipelineMetrics, error: Exception) -> None:
        """Send failure notification to SNS."""
        message = {
            "pipeline": metrics.pipeline_name,
            "run_id": metrics.run_id,
            "status": "failure",
            "error": str(error),
        }
        self.client.publish(
            TopicArn=self.topic_arn,
            Subject=f"Pipeline {metrics.pipeline_name} FAILED",
            Message=str(message),
        )
        logger.info("sns_notification_sent", status="failure", run_id=metrics.run_id)
{%- endif %}


{%- if values.notifications == "none" %}


class NoOpNotificationService(NotificationService):
    """No-op notification service."""

    def send_success(self, metrics: PipelineMetrics) -> None:
        logger.info("notification_skipped", status="success", run_id=metrics.run_id)

    def send_failure(self, metrics: PipelineMetrics, error: Exception) -> None:
        logger.info("notification_skipped", status="failure", run_id=metrics.run_id)
{%- endif %}


def get_notification_service() -> NotificationService:
    """Get the configured notification service."""
    {%- if values.notifications == "slack" %}
    return SlackNotificationService()
    {%- elif values.notifications == "sns" %}
    return SNSNotificationService()
    {%- else %}
    return NoOpNotificationService()
    {%- endif %}
