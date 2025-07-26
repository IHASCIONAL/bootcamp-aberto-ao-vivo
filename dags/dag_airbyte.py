from airflow import DAG
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from datetime import datetime, timedelta

AIRBYTE_CONNECTION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")
API_KEY = f"Bearer {Variable.get('AIRBYTE_API_TOKEN')}"

with DAG(
    dag_id="meu_airbyte_trabalhoso",
    schedule="@daily",
    start_date=datetime(2025, 7, 25),
    dagrun_timeout=timedelta(minutes=1),
    tags=["airbyte"],
    catchup=False,
) as dag:
    # [START howto_operator_airbyte_synchronous]


    sync_source_destination = AirbyteTriggerSyncOperator(
        task_id="airbyte_sync_source_dest_example",
        connection_id=AIRBYTE_CONNECTION_ID,
        airbyte_conn_id="airbyte_conn",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    sync_source_destination







