from pyspark.sql import SparkSession
from pyspark import SparkContext
import sys
# import os
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
# os.environ["SPARK_HOME"] = "/home/lemitus/spark-3.0.3-bin-hadoop2.7"
# import findspark
# findspark.init()
# spark = SparkSession.builder.master("spark://vm1.mj3j1pjsmxvehb3lracj5cfx5e.bx.internal.cloudapp.net:7077").getOrCreate()
# sc = SparkContext(master="spark://vm1.mj3j1pjsmxvehb3lracj5cfx5e.bx.internal.cloudapp.net:7077")
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
# sc.setLogLevel("ERROR")
n = 500
allnumbers = sc.parallelize(range(2, n)).cache()
composite = allnumbers.flatMap(lambda x: range(x*2, n, x))
prime = allnumbers.subtract(composite)

print(prime.collect())
# print(sys.argv)
# res = list(prime.collect())
# idx_arg = int(sys.argv[1]) if len(sys.argv) > 1 else 2
# idx = idx_arg % (len(res) - 20)
# piece = res[idx:idx+20]
# print(piece)