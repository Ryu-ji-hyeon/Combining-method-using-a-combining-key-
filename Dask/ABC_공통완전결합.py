import pandas as pd
from dask import dataframe as dd
import time
import numpy as np

start = time.time()
A_B_C = dd.read_csv("ABC_공통완전결합.csv",assume_missing=True)

A = dd.read_csv("A_id_attr.csv")
B = dd.read_csv("B_id_attr.csv")
C = dd.read_csv("C_id_attr.csv")

A_B_C['A_id'] = A_B_C['A_id'].fillna(0).astype('int64')
A_B_C['B_id'] = A_B_C['B_id'].fillna(0).astype('int64')
A_B_C['C_id'] = A_B_C['C_id'].fillna(0).astype('int64')



#1 
A_Key = dd.merge(A, A_B_C, left_on="id", right_on="A_id", how='outer')

#2 
B_Key = dd.merge(B, A_B_C, left_on="id", right_on="B_id", how='outer')

#3 
C_Key = dd.merge(C, A_B_C, left_on="id", right_on="C_id", how='outer')

#4 
A_B_Key = dd.merge(A_Key, B_Key, left_on="B_id", right_on="id", how='outer')

#5 
A_B_C_Key = dd.merge(A_B_Key, C_Key, left_on="C_id_y", right_on="id", how='outer')

# A_B_Key.compute().to_csv('ABC_공통완전결합_combined.csv', index=False)

# num_rows, num_columns = A_Key.compute().shape

print(A_B_C_Key.compute())

end = time.time()
# print(f"행 수: {num_rows}, 열 수: {num_columns}")
print(end-start)

# 4까지 실행 시 4,175,198뜬다 5 실행시 다른 레코드 수 나옴 5,147,842
