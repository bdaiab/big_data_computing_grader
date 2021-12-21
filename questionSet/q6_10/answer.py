from pyspark.sql import SparkSession
from pyspark import SparkContext

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

from graphframes import *
from pyspark.sql.functions import *

# Vertics DataFrameW
v = spark.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 37),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 38),
  ("g", "Gabby", 60)
], ["id", "name", "age"])

# Edges DataFrame
e = spark.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend"),
  ("g", "e", "follow")
], ["src", "dst", "relationship"])

# Create a GraphFrame
g = GraphFrame(v, e)

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

# Please rename the target column as 'targets'
targets = g.find("(a)-[e1]->(b);(c)-[e2]->(b)")\
        .filter("a != c AND e1.relationship = 'follow' AND e2.relationship = 'follow'")\
        .select(col('b.name').alias("targets"))\
        .distinct()\
        .orderBy("targets")


# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****


result = targets.rdd.map(lambda x: x["targets"]).collect()

print(result)