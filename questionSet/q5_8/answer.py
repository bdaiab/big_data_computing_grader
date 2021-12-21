from pyspark.sql import SparkSession
from pyspark import SparkContext

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

numPartitions = 10

lines = sc.textFile('./dataSet/adj_noun_pairs.txt', numPartitions)
pairs = lines.map(lambda l: tuple(l.split())).filter(lambda p: len(p)==2)
pairs.cache()

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

# let ans be target (adj, noun) pair
def find_min(itr):
    index = float('inf')
    text = []
    for p in itr:
        if(p[1] < index):
            index = p[1]
            text = p[0]
    yield (text, index)
    
kpairs = pairs.zipWithIndex()
res = kpairs.filter(lambda item: item[0][1] == 'unification')\
            .mapPartitions(find_min)   
ans = res.reduce(lambda x, y : x if x[1] < y[1] else y)[0]

# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

result = ans

print(result)