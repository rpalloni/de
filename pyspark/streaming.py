from operator import add
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="Adder")
ssc = StreamingContext(sc, 1) # run every second

inputData = [
    [1, 2, 3],
    [0, 9],
    [4, 4, 4],
    [0, 0, 0, 25],
    [1, -1, 10],
]

rddQueue = []
for datum in inputData:
    rddQueue += [ssc.sparkContext.parallelize(datum)] # transform each sublist in an RDD

inputStream = ssc.queueStream(rddQueue) # transform RDD in a streaming queue => [1,2,3] after second 1, [0,9] after second 2, etc
inputStream.reduce(add).pprint() # each time an input is recived, apply add as reduce operation

ssc.start()
ssc.awaitTerminationOrTimeout(10) # wait for the computation to terminate or timeout
ssc.stop()
# streaming data differs from batch data as it is not clear when
# data will stop coming in so the streming context must be stopped
