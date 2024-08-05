from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.models import Variable
import json

#airbyte_economy_connection_id = Variable.get("AIRBYTE_ECONOMY_CONNECTION_ID")


with DAG(dag_id='trigger_airbyte_dbt_job',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
         ) as dag:

    airbyte_economy_sync = AirbyteTriggerSyncOperator(
        task_id='airbyte_economy',
        airbyte_conn_id='airbytedw',
        connection_id='ed9bfc4b-e4cb-4db2-bd15-044c52d4377b',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )


    airbyte_economy_sync
