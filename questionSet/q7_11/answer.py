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
rdd = sc.textFile('./dataSet/adj_noun_pairs.txt', numPartitions)
rddQueue = rdd.randomSplit([1]*10, 123)
lines = ssc.queueStream(rddQueue)

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

# In order to facilitate the grading, the output part of the
# code has been provided, you only need to care about how to
# find the longest noun. There is no need to sort the results.

def updateFunc(newValues, runningLongest):
    if runningLongest is None:
        runningLongest = ""
    return max(newValues + [runningLongest], key=len)
    # keep the longest word

word_list = lines.map(lambda line: tuple(line.split()))\
                .filter(lambda p: len(p)==2)\
                .updateStateByKey(updateFunc) 


# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****


def output(rdd):
    print("[",rdd.take(5),",")
    temp = rdd.filter(lambda x: x[0] == 'good')
    print(temp.collect(),"],")
  
word_list.foreachRDD(output)
  
ssc.start() 
ssc.awaitTermination(50)
ssc.stop(False)
