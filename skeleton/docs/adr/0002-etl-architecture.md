# ADR 0002: Modular ETL Architecture

## Status

Accepted

## Context

We need to design the pipeline architecture. Options considered:

1. **Monolithic scripts** - Simple but hard to maintain
2. **Class-based components** - Reusable, testable
3. **Functional composition** - Pure functions, immutable data
4. **Framework-based** (Airflow operators, Dagster assets)

## Decision

We chose a **modular, class-based ETL architecture** with:
- `BaseExtractor` - Data source abstraction
- `BaseTransformer` - Transformation logic
- `BaseValidator` - Data quality checks
- `BaseLoader` - Destination abstraction
- `Pipeline` - Orchestrates the flow

## Rationale

1. **Separation of Concerns**: Each component has a single responsibility
   - Extractors only read data
   - Transformers only modify data
   - Validators only check data
   - Loaders only write data

2. **Testability**: Components can be tested in isolation
   - Mock dependencies easily
   - Unit test each transformation
   - Integration test pipelines

3. **Reusability**: Components can be shared across pipelines
   - Same S3Extractor for multiple pipelines
   - Common transformers in a library
   - Standardized validators

4. **Flexibility**: Easy to swap implementations
   - Change source without touching transformers
   - Add validators without modifying loaders
   - Mix and match components

## Consequences

### Positive
- Clear code organization
- Easy to understand data flow
- Highly testable
- Reusable components

### Negative
- More boilerplate than scripts
- Requires understanding abstractions
- May be overkill for simple pipelines

### Mitigations
- Provide concrete implementations for common cases
- Document patterns and examples
- Allow simpler approaches for one-off scripts
