import pandas as pd
from dask import dataframe as dd
import time

start = time.time()
A_C = pd.read_csv("AC_확대단일결합_A기준.csv")  
A = pd.read_csv("A_id_attr.csv")
C = pd.read_csv("C_id_attr.csv")


A_merge = pd.merge(A, A_C, left_on="id", right_on="A_id", how='inner')
C_merge = pd.merge(A_merge, C, left_on="A_id", right_on="id", how='left')

#C_merge.to_csv('AC_확대단일결합_A기준_combined.csv', index=False)
C_merge_filtered = C_merge.loc[~C_merge['A_id'].isnull()]
num_rows, num_columns = C_merge_filtered.compute().shape


print(C_merge)
print(f"행 수: {num_rows}, 열 수: {num_columns}")
end = time.time()

print(end-start)