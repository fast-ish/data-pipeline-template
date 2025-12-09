FROM python:3.12-slim

WORKDIR /opt/dbt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir dbt-snowflake~=${{ values.dbtVersion }}

COPY dbt /opt/dbt

RUN dbt deps

ENTRYPOINT ["dbt"]
CMD ["run"]
