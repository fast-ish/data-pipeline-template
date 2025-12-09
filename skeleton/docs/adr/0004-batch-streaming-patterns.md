# ADR 0004: Batch and Streaming Pipeline Patterns

## Status

Accepted

## Context

Data pipelines need to support different processing patterns:
- **Batch**: Process large historical datasets periodically
- **Streaming**: Process data as it arrives in real-time
- **Hybrid**: Combine batch backfills with streaming updates

## Decision

We support all three patterns with a unified architecture:

1. **Batch Pipelines**: CronJob-triggered, process full datasets
2. **Streaming Pipelines**: Long-running, process micro-batches
3. **Hybrid Pipelines**: Stream for real-time, batch for backfills

## Rationale

1. **Unified Interface**: Same `Pipeline` class works for all patterns
   - Extractors yield batches (large for batch, small for streaming)
   - Same transformers and validators
   - Same loaders with idempotent writes

2. **Scaling Strategy**:
   - Batch: Vertical scaling, larger batches, parallel jobs
   - Streaming: Horizontal scaling, more consumers, smaller batches

3. **Resource Efficiency**:
   - Batch: High burst resources, scheduled during off-peak
   - Streaming: Steady resources, consistent processing

4. **Failure Handling**:
   - Batch: Retry entire job or checkpoint-based recovery
   - Streaming: Per-message acknowledgment, dead letter queues

## Implementation

### Batch Pipeline
```python
class Pipeline:
    def run(self):
        for batch in self.extractor:  # Large batches
            processed = self.process(batch)
            self.loader(processed)
```

### Streaming Pipeline
```python
class StreamingPipeline(Pipeline):
    def run(self):
        for batch in self.extractor:  # Small micro-batches
            processed = self.process(batch)
            self.loader(processed)
            self.checkpoint()  # Save progress
```

## Consequences

### Positive
- Single codebase for all patterns
- Easy to switch between patterns
- Consistent monitoring and logging

### Negative
- Streaming has micro-batch latency
- True real-time requires different architecture
- Complexity in hybrid patterns

### Mitigations
- Use small batch sizes (10-100) for low latency streaming
- Document pattern selection guidelines
- Provide hybrid pipeline templates
