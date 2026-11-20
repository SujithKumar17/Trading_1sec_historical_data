import calendar
from datetime import date, datetime, timedelta
import pandas as pd

def parse_date(value):
    return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()

def month_start(d):
    return date(d.year, d.month, 1)

def next_month(d):
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)

def load_date_column(path, column):
    if not path.exists():
        return []
    try:
        df = pd.read_csv(path, comment="#")
        if column not in df.columns:
            return []
        out = []
        for value in df[column].dropna().astype(str):
            value = value.strip()
            if value:
                out.append(parse_date(value))
        return sorted(set(out))
    except pd.errors.EmptyDataError:
        return []

def get_trading_dates(start_date, end_date, calendar_path):
    official = load_date_column(calendar_path, "trade_date")
    if official:
        return [d for d in official if start_date <= d <= end_date]
    # Fallback is weekdays only; NSE holidays will be caught as NO_DATA.
    dates = []
    d = start_date
    while d <= end_date:
        if d.weekday() < 5:
            dates.append(d)
        d += timedelta(days=1)
    return dates

def get_expiry_for_trade_date(trade_date, expiry_dates):
    candidates = [d for d in expiry_dates if d >= trade_date]
    return candidates[0] if candidates else None

def build_chunks(trade_date, chunk_seconds):
    start = datetime.combine(trade_date, datetime.strptime("09:15:00", "%H:%M:%S").time())
    end = datetime.combine(trade_date, datetime.strptime("09:30:00", "%H:%M:%S").time())
    chunks = []
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(seconds=chunk_seconds), end)
        # API end timestamp is the final included second, avoiding overlap.
        api_end = chunk_end - timedelta(seconds=1)
        chunks.append((cursor, api_end))
        cursor = chunk_end
    return chunks

def breeze_timestamp(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
