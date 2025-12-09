# ADR 0003: Grafana Stack for Observability

## Status

Accepted

## Context

We need comprehensive observability for data pipelines:
- Distributed tracing for debugging
- Metrics for monitoring
- Logs for investigation
- Alerting for incidents

Options considered:
1. **Grafana Stack (LGTM)** - OpenTelemetry with Grafana backends
2. **Datadog** - Full-stack SaaS observability
3. **AWS CloudWatch** - AWS-native
4. **Prometheus + Grafana + Jaeger** - Open source stack

## Decision

We chose **Grafana Stack with OpenTelemetry** as our observability platform.

## Rationale

1. **Vendor Neutrality**: OpenTelemetry provides portable instrumentation
   - No vendor lock-in
   - Standard semantic conventions
   - Can switch backends without code changes

2. **Automatic Instrumentation**: OpenTelemetry Python auto-instruments:
   - Database queries (SQLAlchemy, psycopg2)
   - HTTP clients (httpx, requests)
   - AWS SDK calls (boto3)
   - Kafka operations

3. **Unified Grafana UI**: Single interface for all signals
   - Tempo for traces
   - Loki for logs
   - Mimir/Prometheus for metrics
   - Correlation between all signals

4. **Cost Effective**: Open-source at core
   - Self-hosted or Grafana Cloud options
   - Pay for what you use
   - No per-host pricing

5. **Organization Standard**: Aligns with company-wide observability strategy

## Consequences

### Positive
- Vendor-neutral instrumentation
- Single pane of glass for debugging in Grafana
- Cost-effective at scale
- Strong community support

### Negative
- More initial infrastructure setup
- Requires Grafana Alloy/Agent deployment

### Mitigations
- Use standard logging interfaces (structlog)
- Leverage OpenTelemetry auto-instrumentation
- Use Grafana Cloud for managed experience

## Components

| Component | Purpose |
|-----------|---------|
| Grafana Tempo | Distributed tracing |
| Grafana Loki | Log aggregation |
| Prometheus/Mimir | Metrics |
| Grafana | Visualization |
| Grafana Alloy | Collector/Agent |

## References

- [OpenTelemetry Python](https://opentelemetry.io/docs/languages/python/)
- [Grafana Tempo](https://grafana.com/docs/tempo/latest/)
- [Grafana Loki](https://grafana.com/docs/loki/latest/)
