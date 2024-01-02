import pandas as pd
import time
from dask import dataframe as dd

start = time.time()
A_B = dd.read_csv("AB_공통단일결합.csv")
A = dd.read_csv("A_id_attr.csv")
B = dd.read_csv("B_id_attr.csv")


A_merge = dd.merge(A, A_B, left_on="id", right_on="A_id", how='inner')
B_merge = dd.merge(B, A_merge, left_on="id", right_on="B_id", how='inner')

# B_merge.compute().to_csv('AB_공통단일결합_combined.csv', index=False)

print(B_merge.compute())
end = time.time()
print(end-start)