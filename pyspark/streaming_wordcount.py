# https://spark.apache.org/docs/latest/streaming-programming-guide.html

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working threads and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

# Create a Discretized Streams that will connect to hostname:port
lines = ssc.socketTextStream("localhost", 9999) # stream of data from server

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1)) # create a pair RDD: key(word):value(1)
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

''' reduceByKey
key-value object [(1,2), (2,4), (2,5)] with f(x) sum => [(1,2), (2,9)]
'''

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTerminationOrTimeout(10)  # Wait for the computation to terminate or timeout
ssc.stop()


# run Netcat as data server in a terminal and start writing something
# nc -lk 9999
