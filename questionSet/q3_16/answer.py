from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import *

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

lines = spark.read.text("./dataSet/pagerank_data.txt")
# You can also test your program on the follow larger data set:
# lines = spark.read.text("dblp.in")

numOfIterations = 10

a = lines.select(split(lines[0],' '))
links = a.select(a[0][0].alias('src'), a[0][1].alias('dst'))
outdegrees = links.groupBy('src').count()
ranks = outdegrees.select('src', lit(1).alias('rank'))

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

for iteration in range(numOfIterations):
    # Please rename the PageRank as 'rank'
    ranks = links.join(outdegrees, 'src')\
                .join(ranks, 'src')\
                .select(col('dst').alias('src'), (ranks['rank'])/outdegrees['count'])\
                .groupBy('src').sum('(rank / count)')\
                .select('src', (col('sum((rank / count))') * 0.85 + 0.15).alias('rank'))

# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

result = ranks.orderBy(desc('rank')).rdd.map(lambda x: (x['src'],x['rank'])).collect()

print(result)