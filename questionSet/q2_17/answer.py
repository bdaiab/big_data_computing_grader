from pyspark.sql import SparkSession
from pyspark import SparkContext

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

A = sc.parallelize(range(1, 100))
t = 50
B = A.filter(lambda x: x < t)
B.cache()
count_B = B.count()
t = 10
C = B.filter(lambda x: x > t)
count_C = C.count()

result = [count_B, count_C]

# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****
print(result)