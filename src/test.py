import pandas as pd

df = pd.read_parquet("data/gold/energy_mix/data.parquet")
print(df.head())