import pandas as pd
import os
DATA_PATH = os.path.join(os.path.dirname(__file__), "data/ride_requests.csv")
# Read CSV
df = pd.read_csv(DATA_PATH)

df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], utc=True)

# Optional: if you have created_timestamp column, convert it as well
# df['created_timestamp'] = pd.to_datetime(df['created_timestamp'], utc=True)

# Verify the conversion
print(df.dtypes)
print(df.head())
DATA_PATH = os.path.join(os.path.dirname(__file__), "data/ride_requests.parquet")
# Save back to CSV or Parquet (Parquet recommended for Feast)
df.to_parquet(DATA_PATH, index=False)
