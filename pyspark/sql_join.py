from pyspark.sql import SparkSession
from pyspark import SparkFiles

DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PWD = 'postgres'
DB_NAME = 'public'
DB_URL_LONG = f'jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}?user={DB_USER}&password={DB_PWD}'
DRIVER_URL = '/path/to/postgresql-42.2.22.jar'

TABLE_DATA = 'emp_dep_data'
API_URL = 'https://raw.githubusercontent.com/rpalloni/dataset/master/employee.json'

spark = (
    SparkSession.builder
    .master('local[1]')
    .config('spark.jars', DRIVER_URL)
    .appName('EmployeeDeptData')
    .getOrCreate()
)

def get_api_data(session: SparkSession):
    ''' get employee data from api '''
    session.sparkContext.addFile(API_URL)
    empl = (
        session.read.json('file://'+SparkFiles.get('employee.json'))
    )
    return empl

def get_fs_data(session: SparkSession):
    ''' get department data from FS
    id,title
    1,Finance
    2,Marketing
    3,Logistics
    4,Sales
    5,IT
    6,HR
    '''
    depts = (
        session.read.csv('data/department.csv', header=True)
    )
    return depts


dep = get_fs_data(spark)
dep.show()

dep = dep.withColumnRenamed('id', 'did')
dep.show()

emp = get_api_data(spark)
emp.show()

emp = emp.withColumnRenamed('id', 'eid')

# inner join
emp.join(dep, emp.dept == dep.did, how='inner').show()

# full outer join
emp.join(dep, emp.dept == dep.did, how='full').show() # or outer or fullouter

# left join
emp.join(dep, emp.dept == dep.did, how='left').show() # or leftouter

# right join
emp.join(dep, emp.dept == dep.did, how='right').show() # or rightouter

''' join syntax to avoid cols duplication
df1.join(df2, df1.code == df2.code, how='inner')    # duplicated code column

df1.join(df2, ['code'], how='inner')                # one code column
'''

# SQL syntax
emp.createOrReplaceTempView('EMPLOYEE')
dep.createOrReplaceTempView('DEPARTMENT')

spark.sql('select *  from EMPLOYEE, DEPARTMENT where EMPLOYEE.dept==DEPARTMENT.did').show()
spark.sql('select *  from EMPLOYEE inner join DEPARTMENT on EMPLOYEE.dept==DEPARTMENT.did').show()


data = emp.join(dep, emp.dept == dep.did, how='full')
data.show()


# CREATE TABLE and INSERT RECORDS
(
    data.write
    .format('jdbc')
    .option('url', DB_URL_LONG)
    .option('dbtable', TABLE_DATA)
    .mode('append')
    .save()
)

spark.stop()
