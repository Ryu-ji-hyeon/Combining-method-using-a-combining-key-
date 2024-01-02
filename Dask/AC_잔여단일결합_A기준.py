import pandas as pd
import time
from dask import dataframe as dd

start = time.time()
A_C=dd.read_csv("AC_잔여단일결합_A기준.csv")
A=dd.read_csv("A_id_attr.csv")
C=dd.read_csv("C_id_attr.csv")


A_merge=dd.merge(A,A_C,left_on="id",right_on="A_id",how='inner' )
C_merge=dd.merge(C,A_C,left_on="id",right_on="A_id",how='inner' )
A_C_merge=dd.merge(A_merge,C_merge,left_on="id",right_on="id",how='left' )

# A_C_merge.compute().to_csv('AC_잔여단일결합_A기준_combined.csv', index=False)

A_C_merge_filtered = A_C_merge.loc[~A_C_merge['A_id'].isnull()]
num_rows, num_columns = A_C_merge_filtered.compute().shape

print(f"행 수: {num_rows}, 열 수: {num_columns}")
print(A_C_merge.compute())

end = time.time()
print(end-start) 
