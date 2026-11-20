
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

PROJECT_CONFIG_DIR = BASE_DIR / "project_config"

DATA_ROOT = BASE_DIR / "data" / "raw" / "multi_index"

LOG_DIR = BASE_DIR / "logs"

START_DATE = "2024-01-01"
END_DATE = "2026-11-06"

INTERVAL = "1minute"

MARKET_START = "09:15:00"
MARKET_END = "09:21:00"

API_DELAY_SECONDS = 0.30
MAX_RETRIES = 4

INDEX_CONFIG = {

    "NIFTY50": {
        "stock_code": "NIFTY",
        "spot_exchange": "NSE",
        "option_exchange": "NFO",
        "strike_width": 50,
        "calendar": (
            PROJECT_CONFIG_DIR /
            "nifty50_options_expiry_calendar.csv"
        )
    },

    "BANKNIFTY": {
        "stock_code": "CNXBAN",
        "spot_exchange": "NSE",
        "option_exchange": "NFO",
        "strike_width": 100,
        "calendar": (
            PROJECT_CONFIG_DIR /
            "banknifty_options_expiry_calendar.csv"
        )
    }

# "SENSEX": {
#     "stock_code": "BSESEN",
#     "spot_exchange": "BSE",
#     "option_exchange": "BFO",
#     "strike_width": 100,
#     "calendar": (
#         PROJECT_CONFIG_DIR /
#         "sensex_options_expiry_calendar.csv"
#     )
# }
}
