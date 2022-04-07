import os
import pandas as pd


def extract_data():
    df = pd.read_json('https://raw.githubusercontent.com/rpalloni/dataset/master/os_endpoint.json')
    data_path = '/opt/airflow/dags/data/'
    data_file = 'extracted_data.csv'
    file_path = os.path.join(data_path, data_file)
    df.to_csv(file_path, index=False)
    return df


if __name__ == '__main__':
    extract_data()