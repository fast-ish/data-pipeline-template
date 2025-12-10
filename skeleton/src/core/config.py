"""Pipeline configuration."""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pipeline settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Pipeline
    pipeline_name: str = "${{values.name}}"
    version: str = "0.1.0"
    environment: Literal["development", "staging", "production"] = Field(
        default="development", alias="ENVIRONMENT"
    )

    # AWS
    aws_region: str = Field(default="us-east-1", alias="AWS_REGION")
    {%- if values.sourceType == "s3" or values.sinkType == "s3" %}
    s3_bucket: str = Field(default="", alias="S3_BUCKET")
    s3_prefix: str = Field(default="data", alias="S3_PREFIX")
    {%- endif %}

    {%- if values.sourceType == "postgres" or values.sinkType == "postgres" %}
    # PostgreSQL
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_database: str = Field(default="", alias="POSTGRES_DATABASE")
    postgres_user: str = Field(default="", alias="POSTGRES_USER")
    postgres_password: str = Field(default="", alias="POSTGRES_PASSWORD")

    @property
    def postgres_url(self) -> str:
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"
    {%- endif %}

    {%- if values.sourceType == "redshift" or values.sinkType == "redshift" %}
    # Redshift
    redshift_host: str = Field(default="", alias="REDSHIFT_HOST")
    redshift_port: int = Field(default=5439, alias="REDSHIFT_PORT")
    redshift_database: str = Field(default="", alias="REDSHIFT_DATABASE")
    redshift_user: str = Field(default="", alias="REDSHIFT_USER")
    redshift_password: str = Field(default="", alias="REDSHIFT_PASSWORD")
    {%- endif %}

    {%- if values.sinkType == "snowflake" %}
    # Snowflake
    snowflake_account: str = Field(default="", alias="SNOWFLAKE_ACCOUNT")
    snowflake_user: str = Field(default="", alias="SNOWFLAKE_USER")
    snowflake_password: str = Field(default="", alias="SNOWFLAKE_PASSWORD")
    snowflake_warehouse: str = Field(default="", alias="SNOWFLAKE_WAREHOUSE")
    snowflake_database: str = Field(default="", alias="SNOWFLAKE_DATABASE")
    snowflake_schema: str = Field(default="PUBLIC", alias="SNOWFLAKE_SCHEMA")
    {%- endif %}

    {%- if values.sinkType == "bigquery" %}
    # BigQuery
    bigquery_project: str = Field(default="", alias="BIGQUERY_PROJECT")
    bigquery_dataset: str = Field(default="", alias="BIGQUERY_DATASET")
    {%- endif %}

    {%- if values.sourceType == "kinesis" %}
    # Kinesis
    kinesis_stream_name: str = Field(default="", alias="KINESIS_STREAM_NAME")
    {%- endif %}

    {%- if values.sourceType == "kafka" %}
    # Kafka
    kafka_bootstrap_servers: str = Field(default="", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic: str = Field(default="", alias="KAFKA_TOPIC")
    kafka_consumer_group: str = Field(default="${{values.name}}", alias="KAFKA_CONSUMER_GROUP")
    kafka_security_protocol: str = Field(default="PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL")
    kafka_sasl_mechanism: str = Field(default="", alias="KAFKA_SASL_MECHANISM")
    kafka_sasl_username: str = Field(default="", alias="KAFKA_SASL_USERNAME")
    kafka_sasl_password: str = Field(default="", alias="KAFKA_SASL_PASSWORD")
    {%- endif %}

    {%- if values.sourceType == "msk" %}
    # Amazon MSK
    msk_bootstrap_servers: str = Field(default="", alias="MSK_BOOTSTRAP_SERVERS")
    msk_topic: str = Field(default="", alias="MSK_TOPIC")
    msk_consumer_group: str = Field(default="${{values.name}}", alias="MSK_CONSUMER_GROUP")
    msk_auth_mechanism: Literal["iam", "sasl_scram"] = Field(default="iam", alias="MSK_AUTH_MECHANISM")
    # For SASL/SCRAM auth
    msk_sasl_username: str = Field(default="", alias="MSK_SASL_USERNAME")
    msk_sasl_password: str = Field(default="", alias="MSK_SASL_PASSWORD")
    {%- endif %}

    {%- if values.sinkType == "elasticsearch" %}
    # Elasticsearch/OpenSearch
    elasticsearch_host: str = Field(default="", alias="ELASTICSEARCH_HOST")
    elasticsearch_index: str = Field(default="", alias="ELASTICSEARCH_INDEX")
    {%- endif %}

    {%- if values.sourceType == "api" %}
    # API Source
    api_base_url: str = Field(default="", alias="API_BASE_URL")
    api_key: str = Field(default="", alias="API_KEY")
    {%- endif %}

    # OpenTelemetry (Grafana)
    otel_service_name: str = Field(default="${{values.name}}", alias="OTEL_SERVICE_NAME")
    otel_service_version: str = Field(default="0.1.0", alias="OTEL_SERVICE_VERSION")
    otel_exporter_otlp_endpoint: str = Field(default="http://localhost:4318", alias="OTEL_EXPORTER_OTLP_ENDPOINT")

    {%- if values.notifications == "slack" %}
    # Slack
    slack_webhook_url: str = Field(default="", alias="SLACK_WEBHOOK_URL")
    slack_channel: str = Field(default="#data-alerts", alias="SLACK_CHANNEL")
    {%- endif %}

    {%- if values.notifications == "sns" %}
    # SNS
    sns_topic_arn: str = Field(default="", alias="SNS_TOPIC_ARN")
    {%- endif %}


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


settings = get_settings()
