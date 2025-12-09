{%- if values.sourceType == "kinesis" or values.sourceType == "kafka" or values.sourceType == "msk" %}
"""Streaming data extractors."""

from collections.abc import Iterator
from typing import Any
import json

{%- if values.sourceType == "kinesis" %}
import boto3
{%- endif %}
{%- if values.sourceType == "kafka" %}
from confluent_kafka import Consumer, KafkaError
{%- endif %}
{%- if values.sourceType == "msk" %}
from confluent_kafka import Consumer, KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
{%- endif %}

from src.core.config import settings
from src.core.logging import get_logger
from src.extractors.base import BaseExtractor

logger = get_logger(__name__)


{%- if values.sourceType == "kinesis" %}


class KinesisExtractor(BaseExtractor[dict[str, Any]]):
    """Extract data from Kinesis stream."""

    def __init__(
        self,
        stream_name: str | None = None,
        batch_size: int = 100,
        max_records_per_shard: int = 10000,
    ) -> None:
        super().__init__(batch_size=batch_size)
        self.stream_name = stream_name or settings.kinesis_stream_name
        self.max_records_per_shard = max_records_per_shard
        self.client = boto3.client("kinesis", region_name=settings.aws_region)

    def extract(self) -> Iterator[list[dict[str, Any]]]:
        """Extract records from all shards."""
        stream = self.client.describe_stream(StreamName=self.stream_name)
        shards = stream["StreamDescription"]["Shards"]

        logger.info("kinesis_stream_info", stream=self.stream_name, shard_count=len(shards))

        for shard in shards:
            shard_id = shard["ShardId"]
            iterator_response = self.client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )
            shard_iterator = iterator_response["ShardIterator"]

            records_from_shard = 0
            batch: list[dict[str, Any]] = []

            while shard_iterator and records_from_shard < self.max_records_per_shard:
                response = self.client.get_records(
                    ShardIterator=shard_iterator,
                    Limit=self.batch_size,
                )

                for record in response["Records"]:
                    data = json.loads(record["Data"].decode("utf-8"))
                    batch.append(data)
                    records_from_shard += 1

                    if len(batch) >= self.batch_size:
                        yield self._track_batch(batch)
                        batch = []

                shard_iterator = response.get("NextShardIterator")

                if not response["Records"]:
                    break

            if batch:
                yield self._track_batch(batch)

            logger.info("shard_complete", shard_id=shard_id, records=records_from_shard)
{%- endif %}


{%- if values.sourceType == "kafka" %}


class KafkaExtractor(BaseExtractor[dict[str, Any]]):
    """Extract data from Kafka topic.

    Supports multiple authentication modes:
    - PLAINTEXT: No authentication
    - SASL_SSL with SCRAM-SHA-512: Username/password authentication
    - SASL_SSL with PLAIN: Simple username/password
    """

    def __init__(
        self,
        topic: str | None = None,
        bootstrap_servers: str | None = None,
        consumer_group: str | None = None,
        batch_size: int = 100,
        max_messages: int | None = None,
        security_protocol: str | None = None,
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
    ) -> None:
        super().__init__(batch_size=batch_size)
        self.topic = topic or settings.kafka_topic
        self.max_messages = max_messages

        config = {
            "bootstrap.servers": bootstrap_servers or settings.kafka_bootstrap_servers,
            "group.id": consumer_group or settings.kafka_consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }

        # Add security configuration if specified
        protocol = security_protocol or settings.kafka_security_protocol
        if protocol != "PLAINTEXT":
            config["security.protocol"] = protocol
            mechanism = sasl_mechanism or settings.kafka_sasl_mechanism
            if mechanism:
                config["sasl.mechanism"] = mechanism
                config["sasl.username"] = sasl_username or settings.kafka_sasl_username
                config["sasl.password"] = sasl_password or settings.kafka_sasl_password

        self.consumer = Consumer(config)
        self.consumer.subscribe([self.topic])

    def extract(self) -> Iterator[list[dict[str, Any]]]:
        """Extract messages from Kafka topic."""
        logger.info("kafka_consumer_start", topic=self.topic)

        batch: list[dict[str, Any]] = []
        messages_consumed = 0

        try:
            while True:
                if self.max_messages and messages_consumed >= self.max_messages:
                    break

                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    if batch:
                        yield self._track_batch(batch)
                        batch = []
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info("kafka_partition_eof", partition=msg.partition())
                        continue
                    raise Exception(msg.error())

                data = json.loads(msg.value().decode("utf-8"))
                batch.append(data)
                messages_consumed += 1

                if len(batch) >= self.batch_size:
                    yield self._track_batch(batch)
                    self.consumer.commit()
                    batch = []

            if batch:
                yield self._track_batch(batch)
                self.consumer.commit()

        finally:
            self.consumer.close()
            logger.info("kafka_consumer_closed", total_messages=messages_consumed)
{%- endif %}


{%- if values.sourceType == "msk" %}


def _msk_oauth_cb(oauth_config: dict) -> tuple[str, float]:
    """Callback for MSK IAM authentication token generation."""
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(settings.aws_region)
    return auth_token, expiry_ms / 1000


class MSKExtractor(BaseExtractor[dict[str, Any]]):
    """Extract data from Amazon MSK with IAM or SASL/SCRAM authentication.

    Supports:
    - IAM authentication (recommended): Uses AWS credentials for authentication
    - SASL/SCRAM: Username/password stored in AWS Secrets Manager
    """

    def __init__(
        self,
        topic: str | None = None,
        bootstrap_servers: str | None = None,
        consumer_group: str | None = None,
        batch_size: int = 100,
        max_messages: int | None = None,
        auth_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
    ) -> None:
        super().__init__(batch_size=batch_size)
        self.topic = topic or settings.msk_topic
        self.max_messages = max_messages
        self.auth_mechanism = auth_mechanism or settings.msk_auth_mechanism

        config = {
            "bootstrap.servers": bootstrap_servers or settings.msk_bootstrap_servers,
            "group.id": consumer_group or settings.msk_consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "security.protocol": "SASL_SSL",
        }

        if self.auth_mechanism == "iam":
            # IAM authentication using AWS MSK IAM SASL signer
            config["sasl.mechanism"] = "OAUTHBEARER"
            config["oauth_cb"] = _msk_oauth_cb
            logger.info("msk_auth_configured", mechanism="IAM")
        elif self.auth_mechanism == "sasl_scram":
            # SASL/SCRAM authentication
            config["sasl.mechanism"] = "SCRAM-SHA-512"
            config["sasl.username"] = sasl_username or settings.msk_sasl_username
            config["sasl.password"] = sasl_password or settings.msk_sasl_password
            logger.info("msk_auth_configured", mechanism="SASL_SCRAM")
        else:
            raise ValueError(f"Unsupported MSK auth mechanism: {self.auth_mechanism}")

        self.consumer = Consumer(config)
        self.consumer.subscribe([self.topic])

    def extract(self) -> Iterator[list[dict[str, Any]]]:
        """Extract messages from MSK topic."""
        logger.info("msk_consumer_start", topic=self.topic, auth=self.auth_mechanism)

        batch: list[dict[str, Any]] = []
        messages_consumed = 0

        try:
            while True:
                if self.max_messages and messages_consumed >= self.max_messages:
                    break

                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    if batch:
                        yield self._track_batch(batch)
                        batch = []
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info("msk_partition_eof", partition=msg.partition())
                        continue
                    raise Exception(msg.error())

                data = json.loads(msg.value().decode("utf-8"))
                batch.append(data)
                messages_consumed += 1

                if len(batch) >= self.batch_size:
                    yield self._track_batch(batch)
                    self.consumer.commit()
                    batch = []

            if batch:
                yield self._track_batch(batch)
                self.consumer.commit()

        finally:
            self.consumer.close()
            logger.info("msk_consumer_closed", total_messages=messages_consumed)
{%- endif %}
