from pyspark.sql import SparkSession
from pyspark import SparkContext

# Our auto-grader uses spark-submit to submit your code to a
# cluster, so we need to create sc/spark here. You don't need
# this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****
        
# Your answer 'counts' should be an RDD of (key, value) pairs, where
# key is a string and value is an int, e.g., ('example', 123).
        
from operator import add
lines = sc.textFile('./dataSet/README.md')
counts = lines.flatMap(lambda x: x.split()) \
              .map(lambda x: (x, 1)) \
              .reduceByKey(add)

# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****
        
result = counts
        
print(result)
      