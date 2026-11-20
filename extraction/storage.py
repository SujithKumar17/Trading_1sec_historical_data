import os
import pandas as pd
from pathlib import Path

def safe_name(value):
    return str(value).replace("/", "_").replace("\\", "_").replace(":", "-").replace(" ", "_")

def save_rows(rows, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return 0
    new_df = pd.DataFrame(rows)
    if path.exists():
        try:
            old_df = pd.read_csv(path)
            combined = pd.concat([old_df, new_df], ignore_index=True, sort=False)
        except Exception:
            combined = new_df
    else:
        combined = new_df
    if "datetime" in combined.columns:
        combined = combined.drop_duplicates(subset=["datetime"], keep="last")
        combined = combined.sort_values("datetime")
    combined.to_csv(path, index=False)
    return len(new_df)

def append_csv_row(row, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([row])
    header = not path.exists()
    df.to_csv(path, mode="a", header=header, index=False)

def file_has_data(path):
    if not Path(path).exists():
        return False
    try:
        return os.path.getsize(path) > 20 and len(pd.read_csv(path, nrows=2)) > 0
    except Exception:
        return False
