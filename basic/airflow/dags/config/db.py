# postgres
from psycopg2 import sql, connect

def get_connection():
    try:
        with connect(
            host='basic_postgres_1',
            port='5432',
            database='airflow',
            user='airflow', # user
            password='airflow', # pwd
        ) as connection:
            print(connection)
    except Exception as e:
        print(e)

    return connection