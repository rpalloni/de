# PySpark

Spark Data Processing Framework

Ecosystem components:
* core
* SQL
* streaming
* MLlib

Docs:
- http://spark.apache.org/docs/latest/api/python/index.html
- https://spark.apache.org/docs/latest/api/python/reference/index.html

Examples:
- https://github.com/apache/spark/tree/v3.2.1/examples/src/main/python


### Spark data structure interfaces
- RDD resilient distributed dataset (*original data structure for Apache Spark*)
- DataFrame (*Python and R like*)
- Datasets (*Java and Scala only*)

RDD is a fault-tolerant collection of elements that can be operated on in parallel. \
There are two ways to create RDDs:
* *parallelizing* an existing iterable/collection defined in driver program (parallelize method)
* referencing a dataset in an external storage system
Once created, the distributed dataset can be operated on in parallel

### Partitions
One important parameter for parallel collections is the number of partitions/slices to cut the dataset into. \
**Spark will run one task for each partition of the cluster**. Typically you want 2-4 partitions for each CPU in your cluster. \ Normally, Spark tries to set the number of partitions automatically based on your cluster. \
However, you can also set it manually by passing it as a second parameter to parallelize (e.g. sc.parallelize(data, 10)).
Spark creates one partition for each block of the file (blocks being 128MB).

### Data partitioning
Core Spark feature: data are split in subsets and distributed across nodes to optimize management and calculation

### In-memory computing
Data is stored in RAM to access the data quickly and accelerate analytics

### Lazy evaluation of transformations
*Transformation*: map, filter, join, union \
*Action*: operations as reduce, count, show \
*DAG (Directed Acyclic Graph)*: scheduling layer of Spark Architecture

https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations

Transformations are only added to a DAG of computation (**no data** has been loaded yet) and only when an action is called the DAG gets executed.
The aim of lazy evaluation is to optimize the data processing workflow:
Spark can make many optimization decisions (such as data partition) after it had a chance to look at the DAG in entirety.
This would not be possible if it executed everything as soon as it got it.

In alternative, the execution of transformations will materialize the many intermediate datasets in memory.
This is evidently not efficient and effective since you're really not interested in those intermediate results as such (those are just convenient abstractions while writing the program).
So, just tell Spark what is the eventual answer to get and it figures out best way to get there.

https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations \
https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions

### Example of transformation pipelining
A series of transformation on intermediate (abstract) datasets:
~~~
dt = (
    dataframe
    .where(dataframe.price > 0)
    .select('publisher', 'price')
    .groupby('publisher')
    .avg('price')
)  # lazy evaluation
~~~

The actual computation:
~~~
dt.show() # execution with action
~~~

### Web UI
Apache Spark provides a web app with user interfaces:
* Jobs
* Stages
* Tasks
* Storage
* Environment
* Executors
* SQL

to monitor the status of the application, resource consumption of the cluster and configurations. \
In a Spark local environment, components are accessible @:
* Web UI -> localhost:4040
* Resource Manager -> localhost:9870
* Job Tracker -> localhost:8088
* Node Info -> localhost:8042

Docs: https://spark.apache.org/docs/latest/web-ui.html

~~~
from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder.master("local[1]") \
    .appName('DataPlatform') \
    .getOrCreate()

# transformation
dataframe = (
    spark.read                      # job(0): read
    .option('inferSchema', 'True')  # job(1): infer
    .option('header', 'True')
    .csv('data.csv')
)

# action
dataframe.count()                   # job(2): count

spark.stop()
~~~

![webui](https://user-images.githubusercontent.com/17080117/165984346-502a0697-629b-40cd-ba14-0809b38d260d.png)
