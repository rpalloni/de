'''
PythonOperator DAG
'''

import os
import pandas as pd
from datetime import datetime, timedelta
from airflow.models import DAG

# from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
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
    dag_id='pipeline_python_operator',
    start_date=datetime.now(),
    default_args=default_args,
    schedule_interval=None,
)


def extract_data():
    df = pd.read_json('https://raw.githubusercontent.com/rpalloni/dataset/master/os_endpoint.json')
    data_path = '/opt/airflow/dags/data/'
    data_file = 'extracted_data1.csv'
    file_path = os.path.join(data_path, data_file)
    df.to_csv(file_path, index=False)
    return df


run_python_extractor = PythonOperator(
    task_id='python_extractor', 
    python_callable=extract_data, 
    dag=dag
)

# context manager syntax
# with dag:
#     run_python_extractor = PythonOperator(
#         task_id='python_extractor', python_callable=extract_data
#     )


start_op = DummyOperator(task_id='start_task', dag=dag)
end_op = DummyOperator(task_id='last_task', dag=dag)


# DAG dependencies
start_op >> run_python_extractor  >> end_op