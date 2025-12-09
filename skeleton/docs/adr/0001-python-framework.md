# ADR 0001: Python as Pipeline Framework

## Status

Accepted

## Context

We need to choose a programming language and framework for building data pipelines. The options considered:

1. **Python** - Most common for data engineering
2. **Scala/Spark** - Native Spark support
3. **Go** - High performance
4. **Java** - Enterprise standard

## Decision

We chose **Python 3.12** with a modular, class-based architecture.

## Rationale

1. **Ecosystem**: Python has the richest data engineering ecosystem
   - Extensive library support (Pandas, Polars, PyArrow)
   - Native support for all major data warehouses
   - First-class ML/AI integration

2. **Developer Experience**: Most data engineers are proficient in Python
   - Lower learning curve
   - Faster iteration cycles
   - Easy debugging and testing

3. **Flexibility**: Python supports multiple compute engines
   - Can use Pandas for small data
   - Scale to Spark/Dask for large datasets
   - Easy to swap implementations

4. **Integration**: Native support for our infrastructure
   - Datadog ddtrace library
   - All cloud provider SDKs
   - Orchestrator integrations (Airflow, Dagster, Prefect)

## Consequences

### Positive
- Fast development iteration
- Easy to hire developers
- Rich ecosystem of tools

### Negative
- Performance overhead vs compiled languages
- GIL limitations for CPU-bound work
- Memory management requires attention

### Mitigations
- Use Polars/Rust-backed libraries for performance
- Process data in batches to manage memory
- Use multiprocessing for CPU-bound work
