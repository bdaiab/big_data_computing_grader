from pyspark.sql import SparkSession
from pyspark import SparkContext

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

x = 'abcccbcbcacaccacaabb'
y = 'abcccbcccacaccacaabb'

numPartitions = 4
rdd = sc.parallelize(zip(x,y), numPartitions)

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

def isEqual(itr):
    res = '='
    for i in itr:
        if i[0] > i[1]:
            res = '>'
            break
        if i[0] < i[1]:
            res = '<'
            break
    yield res


res = rdd.mapPartitions(isEqual).collect()
ans = '='
for i in res:
    if i != '=':
        ans = i[0]
        break


# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

result = [ans]

print(result)