# Troubleshooting

Common issues and solutions for the data pipeline.

## Connection Issues

### S3 Access Denied

**Symptoms:**
```
botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling the GetObject operation
```

**Solutions:**
1. Verify AWS credentials are configured:
   ```bash
   aws sts get-caller-identity
   ```
2. Check IAM permissions include `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`
3. Verify bucket policy allows access from your IAM role/user
4. Check S3 bucket region matches `AWS_REGION` environment variable

### Database Connection Timeout

**Symptoms:**
```
sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) could not connect to server: Connection timed out
```

**Solutions:**
1. Verify database host is reachable:
   ```bash
   nc -zv $POSTGRES_HOST $POSTGRES_PORT
   ```
2. Check security group/firewall rules allow inbound connections
3. Verify VPC configuration if running in AWS
4. Check if connection string is correct:
   ```bash
   uv run python -c "from src.core.config import settings; print(settings.postgres_url)"
   ```

### Kafka Consumer Not Receiving Messages

**Symptoms:**
- No data being extracted
- Consumer group lag increasing

**Solutions:**
1. Verify topic exists:
   ```bash
   kafka-topics.sh --list --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS
   ```
2. Check consumer group offset:
   ```bash
   kafka-consumer-groups.sh --describe --group $KAFKA_CONSUMER_GROUP --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS
   ```
3. Verify `auto.offset.reset` is set correctly (usually `earliest`)
4. Check Kafka authentication/authorization

## Pipeline Errors

### Out of Memory

**Symptoms:**
```
MemoryError: Unable to allocate array
```

**Solutions:**
1. Reduce batch size:
   ```python
   extractor = S3Extractor(batch_size=500)  # Reduce from default 1000
   ```
2. Use streaming/chunked processing
3. Increase container memory limits in Kubernetes
4. Use Dask or Spark for large datasets

### Validation Failures

**Symptoms:**
```
ValueError: Validation failed: ['required_fields', 'not_null']
```

**Solutions:**
1. Check source data quality
2. Review validation rules in your pipeline configuration
3. Set `fail_on_error=False` to log but not fail:
   ```python
   validator = SchemaValidator(required_fields=["id"], fail_on_error=False)
   ```
4. Implement dead letter queue for invalid records

### Transformation Errors

**Symptoms:**
```
KeyError: 'expected_field'
```

**Solutions:**
1. Add defensive checks in transformers:
   ```python
   value = record.get("field", default_value)
   ```
2. Add schema validation before transformation
3. Check source data for unexpected schema changes

## Performance Issues

### Slow Extraction

**Solutions:**
1. Increase batch size (if memory allows)
2. Use parallel extraction if source supports it
3. Add database indexes on filter columns
4. Use incremental extraction instead of full load

### Slow Loading

**Solutions:**
1. Use bulk insert operations
2. Disable indexes during load, rebuild after
3. Use COPY command for PostgreSQL/Redshift
4. Partition target tables appropriately

### High Memory Usage

**Solutions:**
1. Process data in smaller batches
2. Use generators instead of lists where possible
3. Clean up references to allow garbage collection
4. Use memory-efficient data formats (Parquet vs JSON)

## Datadog Issues

### Missing Traces

**Symptoms:**
- No traces appearing in Datadog APM

**Solutions:**
1. Verify `DD_TRACE_ENABLED=true`
2. Check Datadog agent is running:
   ```bash
   curl -s http://$DD_AGENT_HOST:8126/info
   ```
3. Verify network connectivity to agent
4. Check `DD_SERVICE`, `DD_ENV`, `DD_VERSION` are set

### Missing Metrics

**Solutions:**
1. Verify DogStatsD is enabled on agent
2. Check metrics are being emitted in logs
3. Verify metric names follow Datadog conventions
4. Check for typos in metric tags

### Log Correlation Not Working

**Solutions:**
1. Verify `DD_LOGS_INJECTION=true`
2. Check log format includes trace context:
   ```json
   {"dd.trace_id": "123", "dd.span_id": "456"}
   ```
3. Ensure same `DD_SERVICE` is used for logs and traces

## Deployment Issues

### Docker Build Fails

**Symptoms:**
```
ERROR: failed to solve: process "/bin/sh -c uv sync" did not complete successfully
```

**Solutions:**
1. Verify `pyproject.toml` and `uv.lock` are present
2. Check for syntax errors in dependencies
3. Ensure base image has required system dependencies
4. Clear Docker cache: `docker build --no-cache`

### Kubernetes Pod CrashLoopBackOff

**Solutions:**
1. Check pod logs:
   ```bash
   kubectl logs -f pod/${{values.name}}-xxx
   ```
2. Verify secrets are mounted:
   ```bash
   kubectl get secret ${{values.name}}-secrets -o yaml
   ```
3. Check resource limits aren't too restrictive
4. Verify container entrypoint is correct

### CronJob Not Running

**Solutions:**
1. Check CronJob status:
   ```bash
   kubectl get cronjob ${{values.name}}
   ```
2. Verify schedule syntax is correct
3. Check for failed jobs:
   ```bash
   kubectl get jobs --selector=app=${{values.name}}
   ```
4. Review pod events for scheduling issues

## Getting Help

1. Check the logs with debug level:
   ```bash
   LOG_LEVEL=DEBUG uv run python -m src.cli run
   ```

2. Review Datadog traces for error details

3. Search existing issues in the repository

4. Contact the data engineering team in #data-eng Slack channel
