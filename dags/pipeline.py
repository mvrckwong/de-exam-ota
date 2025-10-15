from airflow.sdk import DAG
from airflow.providers.http.operators.http import HttpOperator
from datetime import datetime, timedelta

from json import dumps

# Define default arguments for all tasks in the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

with DAG(
     dag_id="ota-dbt",
     dag_display_name='Run DBT Silver and Gold Models',
     description='Run DBT Silver and Gold Models',
     tags=['ota', 'dbt'],
     catchup=False,
     schedule=None,
     default_args=default_args
) as dag:

      run_dbt_silver_models = HttpOperator(
            task_id="run_dbt_silver_models",
            http_conn_id='ota-http_dbt',
            endpoint="v1/run",
            method="POST",
            headers={
                  'X-API-Key': "qsyDQ1uKyqtwjRgwIZAeSQGQzmDOwu66xJxQQljpKxPt7f9RoW6Okc5QJy0d91UxTi8",
                  "Content-Type": "application/json"
            },
            data=dumps({
                  "tags": ["silver"],
                  "target": "dev"
            }),
            do_xcom_push=True,
            log_response=True
      )

      run_dbt_gold_models = HttpOperator(
            task_id="run_dbt_gold_models",
            http_conn_id='ota-http_dbt',
            endpoint="v1/run",
            method="POST",
            headers={
                  'X-API-Key': "qsyDQ1uKyqtwjRgwIZAeSQGQzmDOwu66xJxQQljpKxPt7f9RoW6Okc5QJy0d91UxTi8",
                  "Content-Type": "application/json"
            },
            data=dumps({
                  "tags": ["gold"],
                  "target": "dev"
            }),
            do_xcom_push=True,
            log_response=True
      )

      run_dbt_silver_models >> run_dbt_gold_models
