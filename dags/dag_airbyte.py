from airflow.decorators import dag
from airflow.providers.http.operators.http import HttpOperator
from airflow.models import Variable
from datetime import datetime
import json

AIRBYTE_CONNECTION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")
API_KEY = f"Bearer {Variable.get('AIRBYTE_API_TOKEN')}"

@dag(
    start_date=datetime(2025, 7, 25),
    schedule="@daily",
    catchup=False,
    tags=["airbyte"]
)
def running_airbyte():

    trigger_sync = HttpOperator(
        task_id="trigger_airbyte_sync",
        http_conn_id="airbyte-conn",  # Esse conn deve estar configurado na UI do Airflow
        endpoint="v1/jobs",           # sem "/" no começo evita alguns problemas
        method="POST",
        headers={
            "Content-Type": "application/json",
            "User-Agent": "fake-useragent",
            "Accept": "application/json",
            "Authorization": API_KEY
        },
        data=json.dumps({
            "connectionId": AIRBYTE_CONNECTION_ID,
            "jobType": "sync"
        }),
        log_response=True
    )

    return trigger_sync

dag_instance = running_airbyte()
