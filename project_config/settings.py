from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Preserve the existing ICICI Breeze .env variable names.
ENV_FILE = PROJECT_ROOT / ".env"
API_INTERVAL = "1second"

# Local exchange session window. These are passed in the same ISO-Z
# convention used by the user's existing Breeze integration.
SESSION_START = "09:15:00"
SESSION_END = "09:30:00"  # exclusive end boundary in our chunk planner

# Conservative chunk size below Breeze's documented 1,000-candle/request cap.
CHUNK_SECONDS = 800
REQUEST_SLEEP_SECONDS = 0.75
MAX_RETRIES = 4
RETRY_BASE_SECONDS = 2.0

# NIFTY strike spacing used for the initial research universe.
# Keep configurable because historical contract specifications can change.
STRIKE_STEP = 50
STRIKES_EACH_SIDE = 1

# Validation run selects the most recent N weekdays. Exchange holidays may
# appear in this list; empty API responses are recorded as NO_DATA.
VALIDATION_DAYS = 10
DEFAULT_LOOKBACK_YEARS = 3

# API guardrails. Allow the downloader to continue until it reaches the full 50k
# daily cap rather than the older 4.5k safety threshold.
MAX_CALLS_PER_MINUTE = 100
MAX_CALLS_PER_DAY = 50000
DAILY_CALLS_SOFT_LIMIT = 50000

# Set True only after populating futures_expiry_calendar.csv.
DOWNLOAD_FUTURES = False

HISTORICAL_START_DATE = "2025-06-01"
HISTORICAL_END_DATE = "2025-12-31"
# HISTORICAL_TIMEFRAMES = ("1second", "1minute", "5minute")
HISTORICAL_TIMEFRAMES = ("1second",)
# HISTORICAL_TIMEFRAMES = ("5minute",)
HISTORICAL_INSTRUMENTS = ("spot", "futures", "options")
OPTION_DEPTH = 1
START_TIME = "09:15:00"
END_TIME = "15:30:00"
RESUME_DOWNLOAD = True
OVERWRITE_EXISTING = False
HISTORICAL_OUTPUT_ROOT = PROJECT_ROOT / "data" / "raw" / "NIFTY50"
HISTORICAL_STATUS_FILE = (
    PROJECT_ROOT / "reports" / "NIFTY50_historical_download_status.csv"
)
HISTORICAL_REQUEST_BUDGET_FILE = (
    PROJECT_ROOT / "reports" / "NIFTY50_historical_request_budget.json"
)
MAX_CANDLES_PER_REQUEST = 1000
HISTORICAL_CHUNK_CANDLES = 800

RAW_SPOT_DIR = PROJECT_ROOT / "data" / "raw" / "spot"
RAW_OPTIONS_DIR = PROJECT_ROOT / "data" / "raw" / "options"
RAW_FUTURES_DIR = PROJECT_ROOT / "data" / "raw" / "futures"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
MONTHLY_REPORT_DIR = PROJECT_ROOT / "reports" / "monthly"
REPORT_DIR = PROJECT_ROOT / "reports"

EXPIRY_CALENDAR = PROJECT_ROOT / "project_config" / "expiry_calendar.csv"
FUTURES_EXPIRY_CALENDAR = PROJECT_ROOT / "project_config" / "futures_expiry_calendar.csv"
TRADING_CALENDAR = PROJECT_ROOT / "project_config" / "trading_calendar.csv"
