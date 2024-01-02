from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col
from pyspark.sql.functions import *
import time

start = time.time()
spark = SparkSession.builder.appName('missing').getOrCreate()
spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")

A_C = spark.read.csv('AC_잔여단일결합_A기준.csv', header=True, inferSchema=True)
A = spark.read.csv('A_id_attr.csv', header=True, inferSchema=True)
C = spark.read.csv('C_id_attr.csv', header=True, inferSchema=True)

result_A = A_C.join(A, A_C.A_id  == A.id, how="inner")
result_A = result_A.withColumnRenamed("A_id", "A_id_a").withColumnRenamed("C_id", "C_id_a").withColumnRenamed("id", "id_a")

result_C = A_C.join(C, A_C.A_id == C.id, how="inner")
result_C = result_C.withColumnRenamed("A_id", "A_id_c").withColumnRenamed("C_id", "C_id_c").withColumnRenamed("id", "id_c")

final_result = result_A.join(result_C, result_A . A_id_a == result_C . id_c , how="left")

# 결과 확인
final_result.show(truncate=False)
end = time.time()

column_count = len(final_result.columns)
print(f"Row count: {final_result.count()}")
print(f"Column count: {column_count}")

print(end-start)
##
