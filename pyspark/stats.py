# pip install pyspark
# $SPARK_HOME: ../python3.8/dist-packages/pyspark/

from pyspark import SparkContext
from pyspark.mllib.stat import Statistics
from pyspark.mllib.random import RandomRDDs

# create spark context => entry point (gateway) of the app to Apache Spark
sc = SparkContext(appName="Stats")
sc.version # version

inputData = [
    [2, 2, 3],
    [0, 9, 2],
    [4, 4, 4]
]

matrix = sc.parallelize(inputData) # RDD
type(matrix)
print(matrix.collect())

for i in matrix.take(2):
    print(i)

flat = matrix.flatMap(lambda n: n)
print(flat.collect())

summary = Statistics.colStats(matrix)
print(summary.mean()) # [2 5 3]

seriesx = sc.parallelize([41, 36, 12, 18, 28, 23, 19, 8, 7, 16, 11, 14, 18, 14, 34,
                          6, 30, 11, 1, 4, 32, 23, 45, 18, 26, 32, 41, 2, 29, 30], numSlices=(4))
seriesy = sc.parallelize([190, 118, 149, 313, 300, 99, 19, 194, 256, 65, 334, 307, 78, 322,
                          44, 8, 320, 25, 95, 25, 63, 51, 89, 48, 26, 99, 210, 280, 32, 65], numSlices=(4))

seriesx.getNumPartitions()
seriesy.getNumPartitions()

c = Statistics.corr(seriesx, seriesy, method='pearson')
print(c)


# generate a random RDD with 100 i.i.d. values drawn from N(0,1) evenly divided in 10 partitions
data = RandomRDDs.uniformRDD(sc, 100, 10)
data.collect()
data.show()  # error!
datasq = data.map(lambda d: d*d)
datasq.collect()

sc.stop()
