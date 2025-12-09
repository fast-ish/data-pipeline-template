# ADR 0005: Data Quality Strategy

## Status

Accepted

## Context

Data quality is critical for reliable pipelines. We need to:
- Validate data at ingestion
- Enforce business rules
- Detect anomalies
- Handle invalid data gracefully

Options considered:
1. **Great Expectations** - Full-featured data quality framework
2. **Pandera** - Pandas/Polars schema validation
3. **Soda** - Modern data quality platform
4. **Custom validators** - Hand-rolled checks

## Decision

We support multiple data quality approaches:
1. **Built-in validators** for common checks (schema, nulls, ranges)
2. **Great Expectations** for comprehensive suites
3. **Pandera** for DataFrame schema validation
4. **Soda** for data quality SaaS integration

## Rationale

1. **Layered Approach**:
   - **Schema validation**: Catch structural issues early
   - **Business rules**: Enforce domain constraints
   - **Statistical checks**: Detect anomalies

2. **Framework Flexibility**:
   - Simple checks: Use built-in validators
   - Complex rules: Use Great Expectations
   - Schema enforcement: Use Pandera

3. **Failure Handling Options**:
   - `fail_on_error=True`: Stop pipeline on bad data
   - `fail_on_error=False`: Log and continue
   - Dead letter queue: Route invalid records for review

## Implementation

### Built-in Validators
```python
validators = [
    SchemaValidator(required_fields=["id", "email"]),
    NotNullValidator(fields=["id"]),
    RangeValidator(field="amount", min_value=0),
]
```

### Great Expectations
```python
suite = gx.ExpectationSuite(expectations=[
    gx.ExpectColumnValuesToNotBeNull(column="id"),
    gx.ExpectColumnValuesToBeBetween(column="amount", min_value=0),
])
validator = GreatExpectationsValidator(suite)
```

### Pandera
```python
schema = pa.DataFrameSchema({
    "id": pa.Column(str, nullable=False, unique=True),
    "amount": pa.Column(float, pa.Check.ge(0)),
})
validator = PanderaValidator(schema)
```

## Consequences

### Positive
- Multiple options for different needs
- Built-in validators for simple cases
- Integration with industry-standard tools

### Negative
- Multiple frameworks to learn
- Potential inconsistency across pipelines
- Overhead of validation step

### Mitigations
- Document recommended approach for each use case
- Provide validation templates
- Make validation step optional but encouraged
