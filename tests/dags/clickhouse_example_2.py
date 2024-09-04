from airflow import DAG
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator
from datetime import datetime

with DAG('clickhouse_example_2',
         start_date=datetime(2023, 1, 1),
         schedule_interval='@daily',
         catchup=False) as dag:

    clickhouse_task = ClickHouseOperator(
        task_id='clickhouse_query',
        sql='SELECT * FROM trips LIMIT 10;',
        clickhouse_conn_id='clickhouse_default',
    )

    clickhouse_task