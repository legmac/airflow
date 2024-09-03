from airflow import DAG
from airflow.providers.clickhouse.operators.clickhouse import ClickHouseOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'clickhouse_workflow',
    default_args=default_args,
    description='A simple ClickHouse workflow',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['example'],
)

create_table = ClickHouseOperator(
    task_id='create_clickhouse_table',
    clickhouse_conn_id='clickhouse_default',
    sql='CREATE TABLE my_table ...',
    dag=dag,
)