from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from clickhouse_driver import Client
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='s3_to_clickhouse_dag',
    default_args=default_args,
    description='DAG to download CSV from S3, parse dates, and upload to ClickHouse',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    def download_csv_from_s3(**kwargs):
        """Download CSV files from S3."""
        s3_hook = S3Hook(aws_conn_id='my_s3_conn')  # Replace 'my_s3_conn' with your S3 connection ID in Airflow
        bucket_name = 'your-bucket-name'            # Replace with your S3 bucket name
        prefix = 'path/to/csv/'                     # S3 prefix where CSV files are located

        # List files in the bucket with the specified prefix
        files = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

        # Download each file and store in xcom for further processing
        downloaded_files = []
        for file in files:
            file_obj = s3_hook.get_key(key=file, bucket_name=bucket_name)
            content = file_obj.get()['Body'].read().decode('utf-8')
            downloaded_files.append(content)

        return downloaded_files

    def parse_and_upload_to_clickhouse(ti, **kwargs):
        """Parse the downloaded CSV files, convert date columns, and upload to ClickHouse."""
        # Get downloaded files from xcom
        csv_contents = ti.xcom_pull(task_ids='download_csv_from_s3')

        # ClickHouse connection details
        client = Client(host='clickhouse_host', user='default', password='password', database='default') # Replace with your ClickHouse details

        for content in csv_contents:
            # Read the CSV content into a DataFrame
            df = pd.read_csv(StringIO(content))

            # Example of parsing a date column named 'date_column'
            df['date_column'] = pd.to_datetime(df['date_column'], format='%Y-%m-%d')  # Modify the format as per your date format

            # Prepare data for ClickHouse insertion
            data_tuples = list(df.itertuples(index=False, name=None))

            # Create table in ClickHouse if not exists
            create_table_query = """
            CREATE TABLE IF NOT EXISTS my_table (
                column1 String,
                column2 Date,
                column3 Float32,
                date_column Date
            ) ENGINE = MergeTree()
            ORDER BY date_column
            """
            client.execute(create_table_query)

            # Insert data into ClickHouse
            insert_query = "INSERT INTO my_table (column1, column2, column3, date_column) VALUES"
            client.execute(insert_query, data_tuples)

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
