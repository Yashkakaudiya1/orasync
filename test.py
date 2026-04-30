import pandas as pd
import os

os.makedirs("output", exist_ok=True)

df = pd.DataFrame({
    "id": [1,2,3],
    "name": ["A","B","C"]
})

df.to_parquet("output/test.parquet", index=False)

print("Parquet working")
