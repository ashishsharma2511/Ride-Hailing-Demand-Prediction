import pandas as pd
import os
DATA_PATH = os.path.join(os.path.dirname(__file__), "data/ride_requests.csv")
# Read CSV
df = pd.read_csv(DATA_PATH)

DATA_PATH = os.path.join(os.path.dirname(__file__), "data/ride_data.parquet")
# Save as Parquet
df.to_parquet(DATA_PATH, engine="pyarrow", index=False)