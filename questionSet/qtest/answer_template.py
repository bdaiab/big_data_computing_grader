from pyspark.sql import SparkSession
from pyspark import SparkContext

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****


# please use your_answer.collect() to replace None below
res = None

# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****
print(result)