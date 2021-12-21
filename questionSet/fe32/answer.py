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
 ("a", "b", "follow"),
 ("a", "c", "friend"),
 ("a", "g", "friend"),
 ("b", "c", "friend"),
 ("c", "a", "friend"),
 ("c", "b", "friend"),
 ("c", "d", "follow"),
 ("c", "g", "friend"),
 ("d", "a", "follow"),
 ("d", "g", "friend"),
 ("e", "a", "follow"),
 ("e", "d", "follow"),
 ("f", "b", "follow"),
 ("f", "c", "follow"),
 ("f", "d", "follow"),
 ("g", "a", "friend"),
 ("g", "c", "friend"),
 ("g", "d", "friend")
], ["src", "dst", "relationship"])

# Create a GraphFrame
g = GraphFrame(v, e)

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

# Please rename the target column as 'user' and 'recommended user'
recommend = g.find('(a)-[e]->(b);(b)-[f]->(c);!(a)-[]->(c)')\
            .filter('e.relationship == "follow" and f.relationship == "friend"')\
            .select(col('a.name').alias('user'),col('c.name').alias('recommended user'))\
            .distinct()


# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****


result = recommend.rdd.map(lambda x: (x["user"], x["recommended user"])).sortBy(lambda x: x[0]).collect()

print(result)