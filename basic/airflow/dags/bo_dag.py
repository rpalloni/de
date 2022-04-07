'''
BashOperator DAG
'''

from datetime import datetime, timedelta
from airflow.models import DAG

# from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['emailtonotify@test.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=2),
}



dag = DAG(
    'pipeline_bash_operator',
    start_date=datetime.now(),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
)



run_bash_extractor = BashOperator(
    task_id='bash_extractor',
    bash_command='python /opt/airflow/dags/jobs/extract_data.py',
    dag=dag,
)


start_op = DummyOperator(task_id='start_task', dag=dag)
end_op = DummyOperator(task_id='last_task', dag=dag)


# DAG dependencies
start_op >> run_bash_extractor >> end_op