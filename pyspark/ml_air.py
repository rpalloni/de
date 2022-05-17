from pyspark.sql import SparkSession
from pyspark import SparkFiles

from pyspark.sql.types import StructType, IntegerType, DoubleType, StringType
from pyspark.sql.functions import isnan, when, count, col

from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.ml.regression import LinearRegression


# create spark session
spark = SparkSession.builder.master('local[4]').appName('AirQuality').getOrCreate()
spark # open Spark Web UI to monitor jobs/stages/storage/env/executors/SQL

# configure and start spark history server (app log)
# https://sparkbyexamples.com/spark/spark-history-server-to-monitor-applications/


url = 'https://raw.githubusercontent.com/rpalloni/dataset/master/airquality.csv'
spark.sparkContext.addFile(url)

schema = StructType() \
            .add("Ozone", IntegerType(), True) \
            .add("SolarRay", IntegerType(), True) \
            .add("Wind", DoubleType(), True) \
            .add("Temp", IntegerType(), True) \
            .add("Month", IntegerType(), True) \
            .add("Day", IntegerType(), True)

SparkFiles.getRootDirectory()
SparkFiles.get('airquality.csv')

df_s = spark.read.csv("file://"+SparkFiles.get('airquality.csv'), header=True, schema=schema) # header present in file

type(df_s) # pyspark.sql.dataframe.DataFrame
df_s.show()
df_s.count()
df_s.columns
df_s.select("Ozone").show(10)
df_s.describe(['Ozone', 'SolarRay']).show()

# SQL syntax
df_s.createOrReplaceTempView('airdata') # create an SQL view
spark.sql('select * from airdata limit 10').show()

# NAs
df_s.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_s.columns]).show()
df_s.na.drop(how='any') # any/all, thresh (min # of null values to delete record)

# missing values
imputer = Imputer(
    inputCols=['Ozone', 'SolarRay'],
    outputCols=['{}_imputed'.format(c) for c in ['Ozone', 'SolarRay']]
).setStrategy('mean')

imputer.fit(df_s).transform(df_s).show()
df_s = imputer.fit(df_s).transform(df_s)

# filter
df_s.filter('Ozone>=120').show()
df_s.filter((df_s['Ozone'] >= 120) & (df_s['Wind'] == 4)).show()

# aggregate
df_s.groupBy('Month').count().show()
df_s.groupBy('Month').avg('Ozone').show()
df_s.groupBy('Month').max('Ozone').show()


# merge multiple columns in a single vector column
predictors_assembler = VectorAssembler(
    inputCols=['SolarRay_imputed', 'Wind', 'Temp'],
    outputCol='predictors')

df_reg = predictors_assembler.transform(df_s).select('Ozone_imputed', 'predictors')
df_reg.show()

train_data, test_data = df_reg.randomSplit([0.75, 0.25])
lm = LinearRegression(featuresCol='predictors', labelCol='Ozone_imputed')

res = lm.fit(train_data)
res.coefficients
res.intercept
res.summary.residuals.show()

pred = res.evaluate(test_data)
pred.r2adj
pred.meanAbsoluteError
pred.meanSquaredError
pred.predictions.show()

# end session
spark.stop()
