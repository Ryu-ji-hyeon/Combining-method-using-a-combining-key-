import pandas as pd
from dask import dataframe as dd
import time
import numpy as np

start = time.time()
A_B_C = pd.read_csv("ABC_공통다중결합.csv")

A = pd.read_csv("A_id_attr.csv")
B = pd.read_csv("B_id_attr.csv")
C = pd.read_csv("C_id_attr.csv")

A_B_C['B_id'] = A_B_C['B_id'].fillna(0).astype('int64')
A_B_C['C_id'] = A_B_C['C_id'].fillna(0).astype('int64')

#1 
A_Key = pd.merge(A, A_B_C, left_on="id", right_on="A_id", how='inner')

#2 
B_Key = pd.merge(B, A_B_C, left_on="id", right_on="B_id", how='inner')

#3 
C_Key = pd.merge(C, A_B_C, left_on="id", right_on="C_id", how='inner')

#4 
A_B_Key = pd.merge(A_Key, B_Key, left_on="B_id", right_on="id", how='outer')

#5 
A_B_C_Key = pd.merge(A_B_Key, C_Key, left_on="C_id_x", right_on="id", how='left')

# A_B_C_Key.to_csv('ABC_공통다중결합_combined.csv', index=False)

print(A_B_C_Key)

end = time.time()
print(end-start)
