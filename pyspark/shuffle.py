'''
How does spark handle operations on partitions allocated in different executors?
Example: 2 dataframes, 3 partitions per dataframe assigned to 3 executors
Operation: JOIN
The join cannot be performed if the data to be linked is on different executors

Solution: A map-reduce transformation (SHUFFLE JOIN) is performed
1) map: spark maps all the ids on which the query must take place
2) reduce: rearrange records to different executors based on ids
3) perform the join
'''

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master('local[3]') # three executors
    .appName('Partitions')
    .config('spark.sql.shuffle.partitions', 3) # return three partitions after the shuffle
    .getOrCreate()
)

def get_fs_data(session: SparkSession, filepath: str):
    data = (
        session.read
        .format('json')
        .option('multiLine', 'true')
        .load(filepath)
    )
    return data

posts = get_fs_data(spark, 'data/posts.json')
posts.show(5)

comments = get_fs_data(spark, 'data/comments.json')
comments.show(5)

'''
Reshuffle the data in the RDD randomly to create either more or fewer partitions
and balance it across them. This always shuffles all data over the network.
'''

p = posts.repartition(3) # split in three partitions
p.write.save('data/df1', format='json')

c = comments.repartition(3) # split in three partitions
c.write.save('data/df2', format='json')


dfposts = spark.read.json('data/df1')
dfcomms = spark.read.json('data/df2')

df = (
    dfposts
    .join(dfcomms, dfcomms.postId == dfposts.pId, how='inner')
    .select('pId', 'commId', 'commTitle', 'commEmail')
    .sort('pId', 'commId')
)

df.count()
df.show()

# post 84 has 5 comments split across partitions:
# 1 in part-00000, 1 in part-00001, 3 in part-00002
# however its data are mapped and reduced in one executor to join records
df.where(df.pId == '84').count() # 5

df.write.save('data/dfinner', format='json') # keep three partitions (see config)

spark.stop()
