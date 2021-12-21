from pyspark.sql import SparkSession
from pyspark import SparkContext

# Our grading system use spark-submit to submit your code to
# cluster, so we need to create sparkContext here, you don't
# need this if you use Jupyter Notebook or shell.
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

p = 4
# example: 
# rdd = sc.parallelize([1, 0, 0, 1, 1, 1, 1, 0, 1, 1, 1, 0, 0], p)
rdd = sc.textFile('./dataSet/zeros_ones.txt',p).flatMap(lambda x: eval(x))

# *****START OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

def divide(it):
    left_one = 0    # Number of continuous 1's counted from left
    right_one = 0    # Number of continuous 1's counted from right
    local_one = 0    # The longest continuous 1's within partition
    indicator = True # indicates whether the partition is all 1's
    running_one = 0
    
    for x in it:
        if (x == 0):
            if (indicator): # Encountered the first zero
                left_one = running_one 
            if (running_one > local_one):
                local_one = running_one
            running_one = 0
            indicator = False
        else:
            running_one += 1
        
    right_one = running_one
    if (indicator): # update the case of all 1's
        left_one = running_one
        local_one = running_one
            
    yield [left_one, right_one, local_one, indicator]
    
    
L = rdd.mapPartitions(divide).collect()

def conquer(L):
    res = 0
    running_one = 0
    
    for x in L:
        (left_one, right_one, local_one, indicator) = x
        # case 1, maximum appears within site
        if (local_one > res):
            res = local_one
        # case 2 and 3, maximum cross multiple partitions
        if (indicator): # carry to the next partition
            running_one += local_one
        else: # solve the cross-partition max
            running_one += left_one
            if (running_one > res):
                res = running_one
            running_one = right_one
    # deal with the end
    if (running_one > res):
        res = running_one
    return res

# *****END OF YOUR CODE (DO NOT DELETE/MODIFY THIS LINE)*****

print([conquer(L)])