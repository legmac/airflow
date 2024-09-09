from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_DATE = datetime(2024, 1, 1)

dag = DAG(dag_id='bash_operator', start_date=DEFAULT_DATE, schedule=None)
task = BashOperator(task_id='hostname', bash_command='hostname', dag=dag)

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

download_csv_task = PythonOperator(
    task_id='download_csv_from_s3',
    python_callable=download_csv_from_s3,
    provide_context=True,
)

download_csv_task