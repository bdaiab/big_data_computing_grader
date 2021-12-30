from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
ssc = StreamingContext(sc, 10)
# Provide a checkpointing directory. Required for stateful transformations
ssc.checkpoint("checkpoint")

numPartitions = 8
rdd = sc.textFile('./dataSet/numbers.txt', numPartitions)
rdd = rdd.map(lambda u: int(u))
rddQueue = rdd.randomSplit([1]*100, 123)
numbers = ssc.queueStream(rddQueue)

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

# In order to facilitate the grading, the output part of the
# code has been provided, you only need to care about how to
# find the longest noun. There is no need to sort the results.

Stat = numbers.map(lambda u:(u,1)).reduceByWindow(lambda x ,y: (x[0]+y[0],x[1]+y[1]), lambda x,y: (x[0]-y[0],x[1]-y[1]), 30,10 )


# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****


def printResult(rdd):
    result = rdd.take(1)
    print(result[0][0]//result[0][1],",")

Stat.foreachRDD(printResult)
  
ssc.start() 
ssc.awaitTermination(50)
ssc.stop(False)
