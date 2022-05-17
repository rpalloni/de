'''
WholeStageCodegen: generate RDD/Dataframe partitions
Exchange: shuffle data between executors to allow operations
on partitions allocated in different executors (spark_shuffle.py)
MapPartitionsInternal: traverse partitions to get result
'''
