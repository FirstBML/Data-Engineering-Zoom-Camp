import sys
import pandas as pd

# 1. Define 'month' before using it
month = 12 

df = pd.DataFrame({"Day": [1, 2], "Passanger_Number": [3, 4]})
df['month'] = month
print(df.head())

# 2. Ensure an argument exists to avoid IndexError
if len(sys.argv) > 1:
    day_val = sys.argv[1]
    df.to_parquet(f"output_day_{day_val}.parquet")
# 3. Added 'f' prefix for string interpolation
print(f'hello pipeline month={month}') 
