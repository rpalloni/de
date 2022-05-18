
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PWD = 'postgres'
DB_NAME = 'public'
DB_URL = f'jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}?user={DB_USER}&password={DB_PWD}'
DRIVER_URL = '/home/rpalloni/postgresql-42.2.22.jar'

def get_db_conn():
    return DB_URL

def get_db_driver():
    return DRIVER_URL
