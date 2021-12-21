from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql.functions import *

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df = spark.read.csv('./dataSet/sales.csv', header=True, inferSchema=True)

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

records = df.select('Name','Price')\
            .where(df.Country=='Brazil')

# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

result = records.rdd.map(lambda x: (x['Name'],x['Price'])).collect()

print(result)