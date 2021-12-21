from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
ssc = StreamingContext(sc, 5)
# Provide a checkpointing directory. Required for stateful transformations
ssc.checkpoint("checkpoint")

numPartitions = 10
rdd = sc.textFile('./dataSet/adj_noun_pairs.txt', 8).map(lambda l: tuple(l.split())).filter(lambda p: len(p)==2)
rddQueue = rdd.randomSplit([1]*10, 123)
lines = ssc.queueStream(rddQueue)

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

# In order to facilitate the grading, the output part of the
# code has been provided, you only need to care about how to
# find the largest frequencies freq(A,N).

def updateFunc(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

running_counts = lines.map(lambda x: (x,1))\
                      .updateStateByKey(updateFunc)

counts_sorted = running_counts.transform(lambda rdd: rdd.sortBy(lambda x: x[1], False))


# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****


def printResults(rdd):
    print(rdd.take(10),",")

counts_sorted.foreachRDD(printResults)
  
ssc.start() 
ssc.awaitTermination(60)
ssc.stop(False)
