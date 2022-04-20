import os
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.dummy import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor

FILE_PATH = '/opt/airflow/dags/data/'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['emailtonotify@test.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}


# Operators configuration
def _failure_callback(context):
    if isinstance(context['exception'], AirflowSensorTimeout):
        print(context)
    print('Sensor timed out')


def save_data(df: pd.DataFrame, filename: str = 'data.csv', data_path: str = FILE_PATH):
    data_path = data_path
    data_file = filename
    file_path = os.path.join(data_path, data_file)
    df.to_csv(file_path, index=False)
    return df

# get data from an API
def extract_data(filename: str = 'extracted.csv'):
    df = pd.read_csv('https://raw.githubusercontent.com/rpalloni/dataset/master/sales.csv')
    df = save_data(df, filename=filename)
    return df


def transform_data(filename: str = 'transformed.csv'):
    #file_name = os.path.join(FILE_PATH, 'extracted.csv')
    df = pd.read_csv(os.path.join(FILE_PATH, 'extracted.csv'))
    # replacing the spaces in the column names to underscore
    df.columns = df.columns.str.replace(' ', '_')

    #convert order date to datetime
    df['Order_Date'] = df['Order_Date'].astype('datetime64[ns]')

    #convert ship date to datetime
    df['Ship_Date'] = df['Ship_Date'].astype('datetime64[ns]')

    #drop row id
    df.drop('Row_ID', axis=1, inplace=True)

    df = save_data(df, filename=filename)
    return df


def group_cat_data(filename: str = 'aggregated_cat.csv'):
    #file_name = os.path.join(FILE_PATH, 'transformed.csv')
    df = pd.read_csv(os.path.join(FILE_PATH, 'transformed.csv'))
    #sales per category and subcategory
    s_cat = df.groupby(['Category', 'Sub_Category']).agg({'Sales': sum})
    df = save_data(s_cat, filename=filename)
    return df


def group_geo_data(filename: str = 'aggregated_geo.csv'):
    #file_name = os.path.join(FILE_PATH, 'transformed.csv')
    df = pd.read_csv(os.path.join(FILE_PATH, 'transformed.csv'))

    #sales per state and city
    s_geo = df.groupby(['State']).agg({'Sales': sum}).sort_values(by=['Sales'], ascending=True)
    df = save_data(s_geo, filename=filename)
    return s_geo


def create_report(filename: str = 'SalesReport.xlsx'):
    s_cat = pd.read_csv(os.path.join(FILE_PATH, 'aggregated_cat.csv'))
    s_geo = pd.read_csv(os.path.join(FILE_PATH, 'aggregated_geo.csv'))

    ExcelObject = pd.ExcelWriter(path=filename, engine='xlsxwriter')
    s_cat.to_excel(ExcelObject, sheet_name='categories', merge_cells=True)
    s_geo.to_excel(ExcelObject, sheet_name='geography')

    wb = ExcelObject.book
    values_format = wb.add_format({'num_format': '$#,##0.00'})

    wsc = ExcelObject.sheets['categories']
    chart_cat = wb.add_chart({'type': 'column'})
    wsg = ExcelObject.sheets['geography']
    chart_geo = wb.add_chart({'type': 'bar'})

    # category
    wsc.set_column('A:B', 25)
    wsc.set_column('C:C', 20, values_format)

    cond_format = wb.add_format({'bold': True, 'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
    wsc.conditional_format('C2:C18', {'type': 'cell',
                                    'criteria': '>=',
                                    'value': 200000,
                                    'format': cond_format})


    chart_cat.set_title({'name': 'Sales by Category and Sub Category'})
    chart_cat.set_legend({'position': 'none'})
    chart_cat.add_series({
        'categories': '=categories!A2:B18',
        'values':     '=categories!C2:C18',
    })

    wsc.insert_chart('F2', chart_cat)

    # geography
    wsg.set_column('A:A', 25)
    wsg.set_column('B:B', 20, values_format)

    chart_geo.set_size({'width': 320, 'height': 960})
    chart_geo.set_title({'name': 'Sales by State'})
    chart_geo.set_legend({'position': 'none'})
    chart_geo.add_series({
        'categories': '=geography!A2:A50',
        'values':     '=geography!B2:B50',
    })

    wsg.insert_chart('F2', chart_geo)

    ExcelObject.save()

    return None


# pipeline setup
dag = DAG(
    'pipeline_etl',
    start_date=datetime.now(),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
)

# tasks are atomic and indipendent from other tasks in the pipeline
# for each task, save result in a file/db for subsequent processing
run_python_extractor = PythonOperator(
    task_id='python_extractor', python_callable=extract_data, dag=dag
)

run_python_transform = PythonOperator(
    task_id='python_transform', python_callable=transform_data, dag=dag
)

run_python_group_cat = PythonOperator(
    task_id='python_group_cat', python_callable=group_cat_data, dag=dag
)

run_python_group_geo = PythonOperator(
    task_id='python_group_geo', python_callable=group_geo_data, dag=dag
)

run_python_report = PythonOperator(
    task_id='python_report', python_callable=create_report, dag=dag
)


# use sensor to check file or folder presence in filesystem
sensor_extract = FileSensor(
    task_id='sensor_extract',
    mode='reschedule',
    on_failure_callback=_failure_callback,
    filepath='/opt/airflow/dags/data/extracted.csv',
    poke_interval=50, # seconds to wait between each tries
    timeout=50 * 60, # seconds before the task times out and fails
    fs_conn_id='file_check', # setup conn in Admin > Connections (type Docker)
)

sensor_agg_cat = FileSensor(
    task_id='sensor_agg_cat',
    mode='reschedule',
    on_failure_callback=_failure_callback,
    filepath='/opt/airflow/dags/data/aggregated_cat.csv',
    poke_interval=50,
    timeout=50 * 60,
    fs_conn_id='file_check',
)

sensor_agg_geo = FileSensor(
    task_id='sensor_agg_geo',
    mode='reschedule',
    on_failure_callback=_failure_callback,
    filepath='/opt/airflow/dags/data/aggregated_geo.csv',
    poke_interval=50, 
    timeout=50 * 60,
    fs_conn_id='file_check',
)


start_op = DummyOperator(task_id='start_task', dag=dag)
mid_op = DummyOperator(task_id='mid_task', dag=dag)
end_op = DummyOperator(task_id='end_task', dag=dag)


start_op >> run_python_extractor >> sensor_extract >> run_python_transform >> [run_python_group_cat, run_python_group_geo] >> mid_op >> [sensor_agg_cat, sensor_agg_geo] >>  run_python_report >> end_op

# xcons: save task result in a variable to be used by another task