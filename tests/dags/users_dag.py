from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client
from datetime import datetime, timedelta
import requests
import pandas as pd
import json

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='users_dag',
    default_args=default_args,
    description='DAG to download CSV from S3, parse dates, and upload to ClickHouse',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    def download_csv_from_s3():
        csv_url = "https://airflow-poc.nyc3.digitaloceanspaces.com/u1/2024/users-2023.csv"
        response = requests.get(csv_url)
        with open('users.csv', 'wb') as f:
            f.write(response.content)


    def parse_and_upload_to_clickhouse():
        # Parse CSV data
        users_df = pd.read_csv('users.csv')
        client = Client(host='104.248.125.24', port='8123', user='default', password='TMUXp4Wn', database='U1_DWH')
        client.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id Int32,
                name String,
                age Int32
            ) ENGINE = MergeTree()
            ORDER BY id;
        ''')
        print(users_df)
        # client.insert_dataframe('users', users_df)
        client.insert_dataframe(
        'INSERT INTO "users" (id, name, age) VALUES',
        users_df,
        settings=dict(use_numpy=True),
        )
        client.disconnect()

    # Define tasks
    download_csv_task = PythonOperator(
        task_id='download_csv_from_s3',
        python_callable=download_csv_from_s3,
        provide_context=True,
    )

    parse_and_upload_task = PythonOperator(
        task_id='parse_and_upload_to_clickhouse',
        python_callable=parse_and_upload_to_clickhouse,
        provide_context=True,
    )

    # Set task dependencies
    download_csv_task >> parse_and_upload_task
