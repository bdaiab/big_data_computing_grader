from pyspark.sql import SparkSession
from pyspark import SparkContext

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

numPartitions = 10
rdd1 = sc.textFile('./dataSet/points.txt',numPartitions).map(lambda x: int(x.split()[0]))
rdd2 = sc.textFile('./dataSet/points.txt',numPartitions).map(lambda x: int(x.split()[1]))

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

def ele_wise_add(rdd1, rdd2): 
    #FILL IN YOUR CODE HERE
    return rdd1.zip(rdd2).map(lambda x: x[0]+x[1])

# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

rdd3 = ele_wise_add(rdd1, rdd2)
print(rdd3.collect())