from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests

DEFAULT_DATE = datetime(2024, 1, 1)

# Download CSV file
json = "https://airflow-poc.nyc3.digitaloceanspaces.com/u1/2024/event-data.json"
response = requests.get(json)
with open('event-data.json', 'wb') as f:
    f.write(response.content)

dag = DAG(dag_id='bash_operator', start_date=DEFAULT_DATE, schedule=None)
task = BashOperator(task_id='hostname', bash_command='hostname', dag=dag)

task = BashOperator(task_id='print', bash_command='cat event-data.json', dag=dag)
