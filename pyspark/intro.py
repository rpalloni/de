from pyspark import StorageLevel
from pyspark.sql import SparkSession

# create spark session
spark = (
    SparkSession.builder
    .master('local[1]') # n of workers (cores)
    .appName('DataPlatform')
    .getOrCreate()
)

# open Web UI at localhost:4040

# transformation
dataframe = (
    spark.read                      # job(0): read
    .option('inferSchema', 'True')  # job(1): infer
    .option('header', 'True')
    .csv('data/data.csv')
)

# action
dataframe.count()                   # job(2): count

# persist https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence
dataframe.persist(StorageLevel.MEMORY_AND_DISK) # or dataframe.cache()


# parse text
readmeFile = (
    spark.read
    .text('README.md')
    .cache()
)

readmeFile.count() # number of rows

linesWithSpark = readmeFile.filter(readmeFile.value.contains('Spark'))
linesWithSpark.count()
linesWithSpark.collect() # return all the elements of the dataset as an array to the driver program


spark.stop()
