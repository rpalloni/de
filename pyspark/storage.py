# StorageLevel decides whether RDD should be stored in-memory or to disk (or both)
from pyspark import SparkContext, StorageLevel

sc = SparkContext(appName="StorageLevel")

words = ('apple', 'lenovo', 'dell', 'samsung')
rdd = sc.parallelize(words)
rdd.persist(StorageLevel.MEMORY_AND_DISK_2) # store in memory AND disk with 2x replication
rdd.count() # open tab Storage in Spark Web UI

# Note that the newly persisted RDDs or DataFrames are not shown in the tab before they are materialized.
# To monitor a specific RDD or DataFrame, make sure an action operation has been triggered (e.g. count, show, etc)

sc.stop()
