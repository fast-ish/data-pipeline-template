{%- if values.orchestrator == "airflow" %}
"""Airflow DAG for ${{values.name}} pipeline."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="${{values.name}}",
    default_args=default_args,
    description="${{values.description}}",
    schedule_interval="@hourly",  # Adjust as needed
    start_date=days_ago(1),
    catchup=False,
    tags=["data-pipeline", "${{values.name}}"],
) as dag:

    run_pipeline = DockerOperator(
        task_id="run_pipeline",
        image="ghcr.io/${{values.orgName}}/${{values.name}}:latest",
        command=["run"],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "ENVIRONMENT": "{{ var.value.environment }}",
            "DD_SERVICE": "${{values.name}}",
            "DD_ENV": "{{ var.value.environment }}",
            {%- if values.sourceType == "s3" or values.sinkType == "s3" %}
            "S3_BUCKET": "{{ var.value.s3_bucket }}",
            {%- endif %}
            {%- if values.sourceType == "postgres" or values.sinkType == "postgres" %}
            "POSTGRES_HOST": "{{ var.value.postgres_host }}",
            "POSTGRES_DATABASE": "{{ var.value.postgres_database }}",
            {%- endif %}
        },
        mount_tmp_dir=False,
    )

    def notify_success(**context):
        """Send success notification."""
        print(f"Pipeline completed successfully: {context['dag_run'].dag_id}")

    def notify_failure(context):
        """Send failure notification."""
        print(f"Pipeline failed: {context['dag_run'].dag_id}")
        print(f"Error: {context.get('exception')}")

    dag.on_failure_callback = notify_failure

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule="all_success",
    )

    run_pipeline >> notify
{%- endif %}
