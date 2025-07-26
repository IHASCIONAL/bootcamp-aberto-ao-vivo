from airflow.decorators import dag
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
import json
from datetime import datetime

AIRBYTE_CONNECTION_ID = Variable.get("AIRBYTE_GOOGLE_POSTGRES_CONNECTION_ID")
API_KEY = f"Bearer {Variable.get('AIRBYTE_API_TOKEN')}"

@dag(start_date=datetime(2025,7,25), schedule="@daily", catchup=False)

def running_airbyte():

    start_airbyte_sync = HttpOperator(
            task_id="start_airbyte_sync",
            http_conn_id="airbyte-conn",
            endpoint=f"/v1/jobs",
            method="POST",
            headers={
                "Content-Type":"application/json",
                "User-Agent":"fake-useragent",
                "Accept":"application/json",
                "Authorization":API_KEY
            },
            data=json.dumps({
                "connectionId": AIRBYTE_CONNECTION_ID, "jobType":"sync"
            }),
            response_check=lambda response: response.json()['status'] == 'running'
    )

    start_airbyte_sync


running_airbyte()