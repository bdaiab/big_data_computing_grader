from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

e=sc.parallelize([(1,2),(1,3),(1,4),(2,3),(3,1)]).toDF(["src","dst"])

e1=e.withColumnRenamed('src', 'x').withColumnRenamed('dst','y')
e2=e.withColumnRenamed('src', 'y1').withColumnRenamed('dst','z')

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

# Please keep the column as 'x' 'y' 'z'
e3=e.withColumnRenamed('src', 'z1').withColumnRenamed('dst','x1')
temp_result = e1.join(e2,e1['y']==e2['y1'])
join_result = temp_result.join(e3,[temp_result['x']==e3['x1'], temp_result['z'] == e3['z1']])
result = join_result.select('x','y','z').filter('x < y and y < z')


# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****


result = result.rdd.map(lambda x: (x["x"], x["y"],x["z"])).sortBy(lambda x: x[0]).collect()

print(result)