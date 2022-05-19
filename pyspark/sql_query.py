from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, regexp_extract, from_unixtime

# create spark session
spark = (
    SparkSession.builder
    .appName('NYTbooks')
    .config('spark.sql.shuffle.partitions', '50')
    .config('spark.driver.maxResultSize', '5g')
    .config('spark.sql.execution.arrow.pyspark.enabled', 'true')
    .getOrCreate()
)

dataframe = spark.read.json('data/nyt2.json') # kaggle data - New York Times Best Sellers

dataframe.columns
dataframe.count()
dataframe.printSchema()
dataframe.show(20)

#################  Data Cleaning  ################

# convert unix seconds to date
# datetime.fromtimestamp(int('1211587200000'[0:10])).strftime('%Y-%m-%d')
dataframe = dataframe.withColumn(
    'bestsellers_date',
    from_unixtime(col('bestsellers_date.$date.$numberLong').substr(0, 10).cast('long'), 'yyyy-MM-dd')
)

dataframe = dataframe.withColumn(
    'published_date',
    from_unixtime(col('published_date.$date.$numberLong').substr(0, 10).cast('long'), 'yyyy-MM-dd')
)

dataframe.show(5)

# clean price
dataframe.groupBy('price').count().show()
dataframe = dataframe.withColumn('price', dataframe['price'].cast('string'))
dataframe = dataframe.withColumn('price_', regexp_extract('price', "[+-]?([0-9]+([,.][0-9]|[0-9]+))", 0)) # extract numeric part
dataframe = dataframe.withColumn('price_', dataframe['price_'].cast('double'))

dataframe.describe('price_').show()

dataframe.filter('price_ is NULL').count()
dataframe.show(20)

dataframe_dropdup = dataframe.dropDuplicates()
dataframe_dropdup.show(10)
dataframe_dropdup.count()


#################  Data Structures  ################
# Converting dataframe into an RDD
rdd_convert = dataframe.rdd

# Obtaining contents of df as Pandas dataFrame
dataframe.toPandas()

# divide/merge rdd
dataframe.repartition(10).rdd.getNumPartitions() # split data into an RDD with 10 partitions
dataframe.coalesce(1).rdd.getNumPartitions() # reduce to 1 partition


################### DataFrame API ###################
# select
dataframe.select('author').show(10)
dataframe.select('author', 'title', 'rank', 'price_').show(10)

# where
dataframe.where(dataframe.publisher == 'Little, Brown').count()
dataframe.where(dataframe.publisher == 'Little, Brown').select('author').show(20)

# when
dataframe.select('title', when(dataframe.title != 'ODD HOURS', 1).otherwise(0)).show(10)

# isin
dataframe[dataframe.author.isin('John Sandford', 'Emily Giffin')].show(5) # records with specified authors if in the given options

# like
dataframe.select('author', 'title', dataframe.title.like('% THE %')).show(15)

# startswith - endswith
dataframe.select('author', 'title', dataframe.title.startswith('THE')).show(5)
dataframe.select('author', 'title', dataframe.title.endswith('NT')).show(5)

# substring
dataframe.select(dataframe.author.substr(1, 6).alias('title')).show()

# groupby
dataframe.groupBy('rank_last_week').count().show(10)


dataframe.printSchema()

(
    dataframe
    .where(dataframe.price_.isNotNull())
    .select('publisher', 'price_')
    .groupby('publisher')
    .avg('price_')
    .show()
)

(
    dataframe
    .where(dataframe.price_.isNotNull())
    .select('publisher', 'price_')
    .groupby('publisher')
    .agg({'price_': 'avg', 'publisher': 'count'})
    .show()
)


##################### SQL syntax ##################
# Registering a table
dataframe.registerTempTable('df')

spark.sql('select * from df').show(3)

spark.sql('select \
           CASE WHEN description LIKE "%love%" THEN "Love_Theme" \
           WHEN description LIKE "%hate%" THEN "Hate_Theme" \
           WHEN description LIKE "%happy%" THEN "Happiness_Theme" \
           WHEN description LIKE "%anger%" THEN "Anger_Theme" \
           WHEN description LIKE "%horror%" THEN "Horror_Theme" \
           WHEN description LIKE "%death%" THEN "Criminal_Theme" \
           WHEN description LIKE "%detective%" THEN "Mystery_Theme" \
           ELSE "Other_Themes" \
           END Themes \
    from df').groupBy('Themes').count().show()


###################### save result ##################
(
    dataframe.select('author', 'title', 'publisher')
    .write.partitionBy('publisher')         # data partitioning by col
    .save('book_folder', format='json')     # folder/subfolder/partition_file(s) => folder_name/partition_col=xxx/part-xxxxx
)                                           # default format: .parquet

###################### retrieve partitions ##################
df = spark.read.json('book_folder')
# spark reads through the nested structure of partitions
# without explicit definition of the partition


# end session
spark.stop()
