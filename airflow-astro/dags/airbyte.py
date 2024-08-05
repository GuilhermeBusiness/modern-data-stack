from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.models import Variable
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
import json

#airbyte_economy_connection_id = Variable.get("AIRBYTE_ECONOMY_CONNECTION_ID")

DBT_CLOUD_CONN_ID = "dbt_conn"
JOB_ID = "70403103994606"

with DAG(dag_id='trigger_airbyte_dbt_job',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
         ) as dag:

    airbyte_economy_sync = AirbyteTriggerSyncOperator(
        task_id='airbyte_economy',
        airbyte_conn_id='airbytedw',
        connection_id='528bfe56-56ba-407c-bf03-48c604a17988',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    trigger_job = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=JOB_ID   
    )

 

    airbyte_economy_sync >> trigger_job