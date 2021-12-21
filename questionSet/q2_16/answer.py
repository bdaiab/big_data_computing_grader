from pyspark.sql import SparkSession
from pyspark import SparkContext

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

from operator import add
lines = sc.textFile('./dataSet/README.md')
counts = lines.flatMap(lambda x: x.split()) \
 	          .map(lambda x: (x, 1)) \
		      .reduceByKey(add)\
              .max(lambda x : x[1])

# Please make result a list
result = counts

# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****
print(result)