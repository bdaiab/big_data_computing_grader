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
df2 = spark.read.csv('./dataSet/countries.csv', header=True, inferSchema=True)

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

# Please rename the total Price as 'Total Price'
records = df.select('Country', 'Price')\
            .groupBy('Country').sum('Price')\
            .withColumnRenamed('sum(Price)', 'Total Price')

# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

result = records.rdd.map(lambda x: (x['Country'],x['Total Price'])).collect()

print(result)