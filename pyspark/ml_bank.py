from pyspark import SparkContext
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
import matplotlib.pyplot as plt

'''
P=positive, A=Average, N=Negative, B=Bankruptcy, NB=NonBankruptcy
1 - Industrial Risk: {P, A, N}
2 - Management Risk: {P, A, N}
3 - Fiancial Flexibility: {P, A, N}
4 - Credibility: {P, A, N}
5 - Competitiveness: {P, A, N}
6 - Operating Risk: {P, A, N}
7 - Class {B, NB}
'''

sc = SparkContext(appName="Bank")
ss = SparkSession.builder.getOrCreate()

url = 'https://raw.githubusercontent.com/rpalloni/dataset/master/bankruptcy.txt'

sc.addFile(url)

rdd = sc.textFile("file://"+SparkFiles.get('bankruptcy.txt')).map(lambda n: n.split(","))
type(rdd)
rdd.getNumPartitions()

# Print the RDD content
rdd.count()
print(rdd.collect())

for i in rdd.take(5):
    print(i)

d = rdd.toDF()
print(d.show())

# RDD to DataFrame
schema = StructType([
    StructField('IndRisk', StringType(), True),
    StructField('MngRisk', StringType(), True),
    StructField('FinFlex', StringType(), True),
    StructField('Cred', StringType(), True),
    StructField('Compet', StringType(), True),
    StructField('OpRisk', StringType(), True),
    StructField('Class', StringType(), True)
])

df = ss.createDataFrame(rdd, schema)
df.printSchema()
df.show()

# Refactor categorical to numerical
indexer = StringIndexer(
    inputCols=['IndRisk', 'MngRisk', 'FinFlex', 'Cred', 'Compet', 'OpRisk', 'Class'],
    outputCols=['IndRisk_idx', 'MngRisk_idx', 'FinFlex_idx', 'Cred_idx', 'Compet_idx', 'OpRisk_idx', 'Class_idx'])
dfi = indexer.fit(df).transform(df)
dfi.show()

# Logistic Regression
predictors_assembler = VectorAssembler(
    inputCols=['IndRisk_idx', 'MngRisk_idx', 'FinFlex_idx', 'Cred_idx', 'Compet_idx', 'OpRisk_idx'],
    outputCol='predictors')

dfl = predictors_assembler.transform(dfi)
dfl.show()

train_data, test_data = dfl.randomSplit([0.75, 0.25])
lm = LogisticRegression(featuresCol='predictors', labelCol='Class_idx')

res = lm.fit(train_data)
res.coefficients
res.intercept
res.summary.roc.show()

res.summary.roc.select('FPR').collect() # false positive rate
res.summary.roc.select('TPR').collect() # true positive rate

plt.figure(figsize=(5, 5))
plt.plot([0, 1], [0, 1], 'r--')
plt.plot(res.summary.roc.select('FPR').collect(),
         res.summary.roc.select('TPR').collect())
plt.xlabel('FPR')
plt.ylabel('TPR')
plt.show()


pred = res.evaluate(test_data)
pred.accuracy

# end session
ss.stop()
