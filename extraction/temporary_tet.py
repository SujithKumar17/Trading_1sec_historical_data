import argparse
from datetime import datetime, timedelta
import logging
import math
from pathlib import Path
import time
import pandas as pd

from breeze_client2 import create_breeze_client

# Initialize Breeze Client
breeze = create_breeze_client()

# Fetch Historical 1-Second Options Data
response = breeze.get_historical_data_v2(
    interval="1second",
    from_date="2023-12-07T09:15:00.000Z",
    to_date="2023-12-07T09:16:00.000Z",
    stock_code="NIFTY",
    exchange_code="NFO",
    product_type="options",
    expiry_date="2023-12-07T06:00:00.000Z",  # Matching expiry year (2023)
    right="put",
    strike_price=21150,
)

print("\n========== NIFTY OPTION DATA EXTRACTION ==========")

if response.get("Success"):
  df = pd.DataFrame(response["Success"])
  output_filename = "TEMP_NIFTY_21150_PUT_20231207.csv"

  # Export dataframe directly to CSV file
  df.to_csv(output_filename, index=False)

  print(f"Success! Data successfully saved to '{output_filename}'")
  print(f"Total Rows Extracted: {len(df)}")
else:
  print(f"Failed to fetch data or no records returned. Response: {response}")

print("==================================================")