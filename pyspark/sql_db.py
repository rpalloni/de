'''
R/W data into postgres db
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id

DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'public'
DB_USER = 'postgres'
DB_PWD = 'postgres'
DRIVER_URL = '/path/to/postgresql-42.2.22.jar'

DB_URL_SHORT = f'jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}'
DB_URL_LONG = f'jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}?user={DB_USER}&password={DB_PWD}'

TABLE_EMPLOYEE = 'employee'
LOCAL_URL = '/path/to/people.json'


spark = (
    SparkSession.builder
    .master('local[4]')
    .appName('postgresdata')
    .config('spark.jars', DRIVER_URL)
    .getOrCreate()
)

print('PySpark Version :'+spark.version)

def get_employee(session):
    '''
    {'name':'Foo', 'age':'35', 'job':'developer'},
    {'name':'Joe', 'age':'32', 'job':'engineer'},
    {'name':'Bob', 'age':'34', 'job':'manager'}
    '''
    data = (
        session.read
        .format('json')
        .load(LOCAL_URL)
    )
    return data


df_people = get_employee(spark)
df_people.show()

# add pk
df_people = df_people.withColumn('id', monotonically_increasing_id())
df_people.show()

# CREATE TABLE and INSERT RECORDS
(
    df_people.write
    .format('jdbc')
    .option('url', DB_URL_LONG)
    .option('dbtable', TABLE_EMPLOYEE)
    .mode('append') # mode = "overwrite"
    .save()
)

''' DB_URL_SHORT
(
    df_people.write
    .format('jdbc')
    .option('url', DB_URL_SHORT)
    .option('dbtable', TABLE_EMPLOYEE)
    .option('user', DB_USER)
    .option('password', DB_PWD)
    .mode('append')
    .save()
)
'''

''' SHORT SYNTAX
url = f'jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}'
table = 'employee'
mode = 'overwrite'
properties = {'user': 'postgres', 'password': 'postgres', 'driver': 'org.postgresql.Driver'}
df_people.write.jdbc(
    url=url,
    table=table,
    mode=mode,
    properties=properties)
'''

# RETRIEVE DATA
df_employee = (
    spark.read
    .format('jdbc')
    .option('url', DB_URL_SHORT)
    .option('dbtable', TABLE_EMPLOYEE)
    .option('user', DB_USER)
    .option('password', DB_PWD)
    .load()
)

df_employee.show()

# CALCULATE RESULTS
(
    df_employee.where('age >= 34')
    .withColumn('retirement', col('age')+40)
    .groupby(col('age'))
    .agg({'retirement': 'sum'})
    .select('age', col('sum(retirement)').alias('total'))
    .sort('age', ascending=True)
    .show()
)

spark.stop()
