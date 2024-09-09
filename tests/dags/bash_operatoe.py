from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator

DEFAULT_DATE = datetime(2019, 12, 1)

dag = DAG(dag_id='test_dag_under_subdir2', start_date=DEFAULT_DATE, schedule_interval=None)
task = BashOperator(task_id='task1', bash_command='hostname', dag=dag)
