import config
from pyspark.sql import SparkSession

sfparams = {
  'sfURL': config.snowflake_account+'.snowflakecomputing.com',
  'sfUser': config.snowflake_user,
  'sfPassword': config.snowflake_password,
  'sfWarehouse': config.snowflake_warehouse,
  'sfDatabase': config.snowflake_database,
  'sfSchema': config.snowflake_schema
}

# Snowflake JDBC driver and Spark Connector
# https://docs.snowflake.com/en/user-guide/spark-connector-use.html#using-the-connector-with-python
DRIVER_URL = 'snowflake-jdbc-3.13.10.jar, spark-snowflake_2.12-2.9.3-spark_3.1.jar'

spark = (
    SparkSession.builder
    .master('local[1]') # n of workers (cores)
    .appName('SnowPlatform')
    .config('spark.jars', DRIVER_URL)
    .getOrCreate()
)


dataframe = (
    spark.read.format('snowflake')
    .options(**sfparams)
    .option('dbtable', 'EMPLOYEES')
    .load()
)
dataframe.show()


q = 'select count(*) from employees'
dfe = (
    spark.read.format('snowflake')
    .options(**sfparams)
    .option('query',  q)
    .option('autopushdown', 'off') # when a query is run on Spark, if possible 'push down' part of the query to Snowflake server to improve performance
    .load()
)
dfe.show()


query = '''
SELECT C.C_MKTSEGMENT AS MARKET_SEGMENT, SUM(O_TOTALPRICE) AS REVENUE
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS O
LEFT OUTER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER C
ON O.O_CUSTKEY = C.C_CUSTKEY
GROUP BY C.C_MKTSEGMENT;
'''

df = (
    spark.read.format('snowflake')
    .options(**sfparams)
    .option('query',  query)
    .option('autopushdown', 'off')
    .load()
)
df.show()

(
    df.write.format('snowflake')
    .options(**sfparams)
    .option('dbtable', 'SEGMENT_REV')
    .mode('overwrite') # append
    .save()
)
