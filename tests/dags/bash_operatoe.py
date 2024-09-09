from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_DATE = datetime(2024, 1, 1)


# Function to download CSV file
def download_csv(**kwargs):
    url = kwargs['url']
    output_path = kwargs['output_path']

    response = requests.get(url)
    response.raise_for_status()  # Raises an error for bad responses

    # Save the CSV content
    with open(output_path, 'wb') as file:
        file.write(response.content)

    print(f"CSV file has been downloaded and saved to {output_path}")

# Download CSV file
json = "https://airflow-poc.nyc3.digitaloceanspaces.com/u1/2024/event-data.json"
response = requests.get(json1)
with open('event-data.json', 'wb') as f:
    f.write(response.content)

dag = DAG(dag_id='bash_operator', start_date=DEFAULT_DATE, schedule=None)
task = BashOperator(task_id='hostname', bash_command='hostname', dag=dag)

download_csv_task = PythonOperator(
    task_id='download_csv_task',
    python_callable=download_csv,
    op_kwargs={
        'url': json,  # Replace with actual CSV URL
        'output_path': 'event-data.json',
    },
)

download_csv_task
task = BashOperator(task_id='hostname', bash_command='hostname', dag=dag)
