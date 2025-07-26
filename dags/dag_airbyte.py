from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from datetime import datetime

AIRBYTE_CONNECTION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")
API_KEY = f"Bearer {Variable.get('AIRBYTE_API_TOKEN')}"

@dag(start_date=datetime(2025, 7, 25), schedule="@daily", catchup=False, tags=["airbyte"])
def running_airbyte():

    operador = AirbyteTriggerSyncOperator(
        task_id='start_airbyte_sync',
        airbyte_conn_id='airbyte_conn',
        connection_id=AIRBYTE_CONNECTION_ID,
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    operador 

running_airbyte() 