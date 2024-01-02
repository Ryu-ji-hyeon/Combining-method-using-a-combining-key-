from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col
from pyspark.sql.functions import *
import time
#
start = time.time()
spark = SparkSession.builder.appName('missing').getOrCreate()
spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")

A_B = spark.read.csv('AB_공통단일결합.csv', header=True, inferSchema=True)
A = spark.read.csv('A_id_attr.csv', header=True, inferSchema=True)
B = spark.read.csv('B_id_attr.csv', header=True, inferSchema=True)

# A_B를 기준으로 A와 조인
result_A = A_B.join(A, A_B.A_id  == A.id, how="inner")
result_A = result_A.withColumnRenamed("A_id", "A_id_a").withColumnRenamed("B_id", "B_id_a").withColumnRenamed("C_id", "C_id_a").withColumnRenamed("id", "id_a")

# A_B를 기준으로 B와 조인
result_B = A_B.join(B, A_B.B_id == B.id, how="inner")
result_B = result_B.withColumnRenamed("A_id", "A_id_b").withColumnRenamed("B_id", "B_id_b").withColumnRenamed("C_id", "C_id_b").withColumnRenamed("id", "id_b")

# A, B, C의 조인 결과를 A_B를 기준으로 조인
final_result = result_A.join(result_B, result_A . B_id_a == result_B . id_b , how="inner")

# 결과 확인
final_result.show(truncate=False)

column_count = len(final_result.columns)
print(f"Row count: {final_result.count()}")
print(f"Column count: {column_count}")

end = time.time()
print(end-start)

