from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "${{ values.owner }}",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "${{ values.name | replace('-', '_') }}",
    default_args=default_args,
    description="${{ values.description }}",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "snowflake", "${{ values.owner }}"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/dbt && dbt deps",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/dbt && dbt run --select staging",
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command="cd /opt/dbt && dbt run --select intermediate",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/dbt && dbt run --select marts",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt && dbt test",
    )

    dbt_deps >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts >> dbt_test
