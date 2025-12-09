# Architecture

## System Overview

```mermaid
flowchart TB
    subgraph Sources
        S3_SRC[S3]
        PG_SRC[PostgreSQL]
        KAFKA[Kafka]
        API[REST API]
    end

    subgraph Pipeline["${{values.name}}"]
        EXT[Extractor]
        TRANS[Transformers]
        VALID[Validators]
        LOAD[Loader]
    end

    subgraph Sinks
        S3_SINK[S3/Delta]
        REDSHIFT[Redshift]
        SNOWFLAKE[Snowflake]
        ES[Elasticsearch]
    end

    subgraph Observability
        DD[Datadog APM]
        METRICS[Metrics]
        LOGS[Structured Logs]
    end

    Sources --> EXT
    EXT --> TRANS
    TRANS --> VALID
    VALID --> LOAD
    LOAD --> Sinks

    Pipeline --> DD
    Pipeline --> METRICS
    Pipeline --> LOGS
```

## Component Architecture

```mermaid
classDiagram
    class BaseExtractor~T~ {
        <<abstract>>
        +batch_size: int
        +records_read: int
        +extract() Iterator[list[T]]
    }

    class BaseTransformer~I,O~ {
        <<abstract>>
        +transform(data: list[I]) list[O]
    }

    class BaseValidator~T~ {
        <<abstract>>
        +fail_on_error: bool
        +validate(data: list[T]) ValidationReport
    }

    class BaseLoader~T~ {
        <<abstract>>
        +records_written: int
        +load(data: list[T]) void
    }

    class Pipeline {
        +name: str
        +extractor: BaseExtractor
        +transformers: list[BaseTransformer]
        +validators: list[BaseValidator]
        +loader: BaseLoader
        +run() PipelineMetrics
    }

    Pipeline --> BaseExtractor
    Pipeline --> BaseTransformer
    Pipeline --> BaseValidator
    Pipeline --> BaseLoader
```

## Data Flow

```mermaid
sequenceDiagram
    participant CLI
    participant Pipeline
    participant Extractor
    participant Transformer
    participant Validator
    participant Loader
    participant Datadog

    CLI->>Pipeline: run()
    Pipeline->>Datadog: Start trace

    loop For each batch
        Pipeline->>Extractor: extract()
        Extractor-->>Pipeline: batch

        Pipeline->>Transformer: transform(batch)
        Transformer-->>Pipeline: transformed

        Pipeline->>Validator: validate(transformed)
        Validator-->>Pipeline: validated

        Pipeline->>Loader: load(validated)
        Loader-->>Pipeline: success

        Pipeline->>Datadog: Emit metrics
    end

    Pipeline->>Datadog: End trace
    Pipeline-->>CLI: PipelineMetrics
```

## Deployment Architecture

{%- if values.orchestrator == "airflow" %}

### Airflow Deployment

```mermaid
flowchart TB
    subgraph Airflow
        SCHED[Scheduler]
        WEB[Web UI]
        WORKER[Worker]
    end

    subgraph ECS["ECS Fargate"]
        TASK[Pipeline Task]
    end

    subgraph Data
        SRC[(Source)]
        SINK[(Sink)]
    end

    SCHED --> WORKER
    WORKER -->|DockerOperator| TASK
    TASK --> SRC
    TASK --> SINK
    TASK --> DD[Datadog]
```
{%- endif %}

{%- if values.orchestrator == "none" %}

### Kubernetes CronJob

```mermaid
flowchart TB
    subgraph K8s["Kubernetes"]
        CRON[CronJob]
        JOB[Job]
        POD[Pod]
    end

    subgraph Data
        SRC[(Source)]
        SINK[(Sink)]
    end

    CRON -->|schedules| JOB
    JOB -->|creates| POD
    POD --> SRC
    POD --> SINK
    POD --> DD[Datadog Agent]
```
{%- endif %}

## Error Handling

```mermaid
flowchart TB
    START[Process Batch] --> TRANSFORM{Transform}
    TRANSFORM -->|Success| VALIDATE{Validate}
    TRANSFORM -->|Error| DLQ[Dead Letter Queue]

    VALIDATE -->|Pass| LOAD[Load]
    VALIDATE -->|Fail| DLQ

    LOAD -->|Success| NEXT[Next Batch]
    LOAD -->|Error| RETRY{Retry?}

    RETRY -->|Yes| LOAD
    RETRY -->|No| DLQ

    DLQ --> ALERT[Send Alert]
```

## Monitoring Stack

```mermaid
flowchart LR
    subgraph Application
        APP[Pipeline]
    end

    subgraph Datadog
        APM[APM Traces]
        LOGS[Log Management]
        METRICS[Metrics]
        DASH[Dashboards]
        ALERT[Alerting]
    end

    APP -->|ddtrace| APM
    APP -->|structlog| LOGS
    APP -->|statsd| METRICS

    APM --> DASH
    LOGS --> DASH
    METRICS --> DASH
    DASH --> ALERT
```

## Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.12 |
| Package Manager | uv |
| Configuration | Pydantic Settings |
| Logging | structlog |
| Tracing | ddtrace |
{%- if values.computeEngine == "polars" %}
| Data Processing | Polars |
{%- elif values.computeEngine == "spark" %}
| Data Processing | PySpark |
{%- elif values.computeEngine == "dask" %}
| Data Processing | Dask |
{%- else %}
| Data Processing | Pandas |
{%- endif %}
{%- if values.dataQuality == "great-expectations" %}
| Data Quality | Great Expectations |
{%- elif values.dataQuality == "pandera" %}
| Data Quality | Pandera |
{%- elif values.dataQuality == "soda" %}
| Data Quality | Soda |
{%- endif %}
{%- if values.orchestrator != "none" %}
| Orchestration | ${{values.orchestrator | title}} |
{%- endif %}
| Container Runtime | Docker |
| Orchestration | Kubernetes |
| CI/CD | GitHub Actions |
| Observability | Datadog |
