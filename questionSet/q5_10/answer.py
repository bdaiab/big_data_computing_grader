from pyspark.sql import SparkSession
from pyspark import SparkContext

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

numPartitions = 10

points = sc.textFile('./dataSet/points.txt',numPartitions)
pairs = points.map(lambda l: tuple(l.split()))
pairs = pairs.map(lambda pair: (int(pair[0]),int(pair[1])))
pairs.cache()

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

# Your algorithm should report the result in variable "result", which
# is a list of K elements, the type of element is like (point.x,point.y)
# for example, (5000,4999) represents the point (5000,4999).
# The points should be sorted in ascending order of the distance.
result = []

def f(iterator):
	dump = list(iterator)
	for P in dump:
		k = 1
		for Q in dump:
			if P[0] == Q[0] and P[1] == Q[1]:
				continue
			if P[0] <= Q[0] and P[1] <= Q[1]:
				k = 0
				break
		if k == 1:
			yield P

temp = pairs.mapPartitions(f).collect()

for i in temp:
	k = 1;
	for j in temp:
		if i[0] == j[0] and i[1] == j[1]:
			continue
		if i[1] <= j[1] and i[0] <= j[0] :
			k = 0
			break
	if k == 1 :
		result.append(i)
# Also you can sort the sequence in advance and use linear scan to find the local and global Skyline

# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****


result.sort(key=lambda x: x[0])

print(result)