import pandas as pd
from dask import dataframe as dd
import time

start = time.time()
A_B_C = dd.read_csv("ABC_공통단일결합.csv")
A = dd.read_csv("A_id_attr.csv")
B = dd.read_csv("B_id_attr.csv")
C = dd.read_csv("C_id_attr.csv")


#1 
A_Key = dd.merge(A, A_B_C, left_on="id", right_on="A_id", how='inner')

#2 
B_Key = dd.merge(B, A_B_C, left_on="id", right_on="B_id", how='inner')

#3 
C_Key = dd.merge(C, A_B_C, left_on="id", right_on="C_id", how='inner')

#4 
A_B_Key = dd.merge(A_Key, B_Key, left_on="B_id", right_on="id", how='inner')

#5 
A_B_C_Key = dd.merge(A_B_Key, C_Key, left_on="C_id_x", right_on="id", how='inner')

# A_B_C_Key.compute().to_csv('A,B,C 공통단일결합_combined.csv', index=False)

num_rows, num_columns = A_B_C_Key.compute().shape
print(f"행 수: {num_rows}, 열 수: {num_columns}")

print(A_B_C_Key.compute())

end = time.time()
print(end-start)

