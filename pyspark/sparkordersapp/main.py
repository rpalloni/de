import psycopg2
from utils import db
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = (
    SparkSession.builder
    .appName('Test Pyspark')
    .getOrCreate()
)

db_properties = {
    'url': f'jdbc:postgresql://{db.DB_HOST}:{db.DB_PORT}/{db.DB_NAME}',
    'user': db.DB_USER,
    'password': db.DB_PWD
}

################################################
########### check sql/json data init ###########
################################################
with psycopg2.connect(
        host=db.DB_HOST,
        user=db.DB_USER,
        password=db.DB_PWD,
        dbname=db.DB_NAME
        ) as connection:
    with connection.cursor() as cursor:
        cursor.execute('SELECT count(*) FROM geography;')
        for r in cursor.fetchall():
            print(r[0])

print(spark.read.format('jdbc').options(**db_properties, dbtable='geography').load().count())
print(spark.read.json('data/orders.json').count())

################################################
#################### order kpi #################
################################################
def get_fs_data(session: SparkSession, filepath: str):
    data = (
        session.read
        .format('json')
        .load(filepath)
    )
    return data

orders = get_fs_data(spark, 'data/orders.json')
orders.show(5)

def get_db_data(session: SparkSession):
    data = (
        session.read
        .format('jdbc')
        .options(**db_properties, dbtable='geography')
        .load()
    )
    return data

geo = get_db_data(spark)
geo.show(5)

orders.printSchema()


geo.printSchema()

df = orders.join(geo, orders.city_code == geo.code, how='inner')
df.show()

df.printSchema()


q1 = (
    df
    .groupBy(
        expr('substring(creation_time,1,10)').alias('date'),
        'country_code'
    ).count().sort('country_code', 'date')
)

q1.count()
q1.show()


q2 = (
    df
    .filter('cancel_reason is NULL') # OK .filter(df.cancel_reason.isNull())  KO .where(df.cancel_reason == 'null')
    .groupBy(
        expr('substring(creation_time,1,10)').alias('date'),
        'country_code'
    )
    .sum('purchases_total_price_in_cents')
    .sort('country_code', 'date')
)

q2.count()
q2.show()

# check sum
(
    df
    .filter((df.country_code == 'BG') & (expr('substring(creation_time,1,10)') == '2021-06-13'))
    .groupBy('country_code', expr('substring(creation_time,1,10)'))
    .sum('purchases_total_price_in_cents')
    .show()
)


q3 = (
    df
    .groupBy(
        expr('substring(creation_time,1,10)').alias('date'),
        'country_code'
    ).avg('purchases_total_price_in_cents').sort('country_code', 'date')
)

q3.count()
q3.show()

# check average
(
    df
    .filter((df.country_code == 'BG') & (expr('substring(creation_time,1,10)') == '2021-06-13'))
    .groupBy('country_code', expr('substring(creation_time,1,10)'))
    .avg('purchases_total_price_in_cents')
    .show()
)

# collect results by date and country
kpi = (
    # q1.join(q2, ((q1.date == q2.date) & (q1.country_code == q2.country_code)), how='left') # duplicated join cols
    q1
    .join(q2, ['date', 'country_code'], how='left')
    .join(q3, ['date', 'country_code'], how='left') # no cols duplication
    .sort('country_code', 'date')
)

kpi.count()

kpi.show()

q4 = (
    kpi
    .filter(kpi.date == '2021-06-26')
    .sort('sum(purchases_total_price_in_cents)', ascending=False)
)

q4.show(3)

(
    kpi.write
    .format('jdbc')
    .option(**db_properties, dbtable='kpi')
    .mode('append')  # overwrite error
    .save()
)


spark.stop()
