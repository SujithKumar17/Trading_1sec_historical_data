
import argparse
import logging
import math
import time

from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

from project_config.settings2 import (
    DATA_ROOT,
    LOG_DIR,
    START_DATE,
    END_DATE,
    INTERVAL,
    MARKET_START,
    MARKET_END,
    API_DELAY_SECONDS,
    MAX_RETRIES,
    INDEX_CONFIG
)

from extraction.breeze_client2 import create_breeze_client


# ============================================================
# LOGGING
# ============================================================

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_DIR / "multi_index_extraction.log",
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# ============================================================
# DATE UTILITIES
# ============================================================

def get_date_range(start_date, end_date):

    current = start_date

    while current <= end_date:

        yield current

        current += timedelta(days=1)


def get_year_month(trading_date):

    year = trading_date.strftime("%Y")

    month = trading_date.strftime("%m")

    return year, month


def get_api_timestamp(trading_date, market_time):

    return (
        trading_date.strftime("%Y-%m-%d")
        + "T"
        + market_time
        + ".000Z"
    )


def get_expiry_timestamp(expiry_date):

    return (
        expiry_date.strftime("%Y-%m-%d")
        + "T07:00:00.000Z"
    )


def parse_datetime_column(df):

    if "datetime" not in df.columns:

        raise ValueError(
            "API response does not contain datetime column"
        )

    df = df.copy()

    df["datetime"] = pd.to_datetime(
        df["datetime"],
        errors="coerce"
    )

    df = df.dropna(
        subset=["datetime"]
    )

    return df


# ============================================================
# DIRECTORY AND FILE NAMING
# ============================================================

def get_spot_file_path(index_name, trading_date):

    year, month = get_year_month(trading_date)

    folder = (
        DATA_ROOT /
        index_name /
        "spot" /
        year /
        month
    )

    folder.mkdir(
        parents=True,
        exist_ok=True
    )

    filename = (
        f"{index_name}_SPOT_"
        f"{trading_date.isoformat()}.csv"
    )

    return folder / filename


def get_option_file_path(
    index_name,
    trading_date,
    option_name,
    moneyness
):

    year, month = get_year_month(trading_date)

    folder = (
        DATA_ROOT /
        index_name /
        "options" /
        year /
        month
    )

    folder.mkdir(
        parents=True,
        exist_ok=True
    )

    filename = (
        f"{index_name}_"
        f"{trading_date.isoformat()}_"
        f"{option_name}_"
        f"{moneyness}.csv"
    )

    return folder / filename


def is_valid_existing_file(file_path):

    return (
        file_path.exists()
        and file_path.stat().st_size > 0
    )


# ============================================================
# SAFE CSV STORAGE
# ============================================================

def save_csv_atomic(df, file_path):

    if df.empty:

        logger.warning(
            "Not saving empty file: %s",
            file_path
        )

        return False

    file_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temp_path = file_path.with_name(
        file_path.stem + ".tmp"
    )

    try:

        df.to_csv(
            temp_path,
            index=False
        )

        # Replace destination only after successful write.
        temp_path.replace(file_path)

        logger.info(
            "SAVED: %s | Rows: %s",
            file_path,
            len(df)
        )

        return True

    except Exception:

        if temp_path.exists():
            temp_path.unlink()

        raise


# ============================================================
# EXPIRY CALENDAR
# ============================================================

def load_expiry_calendar(calendar_path):

    if not calendar_path.exists():

        raise FileNotFoundError(
            f"Expiry calendar not found: {calendar_path}"
        )

    df = pd.read_csv(calendar_path)

    columns = {
        str(col).strip().lower().replace(" ", "_"): col
        for col in df.columns
    }

    if "expiry_date" not in columns:

        raise ValueError(
            f"{calendar_path.name} must contain expiry_date"
        )

    expiry_column = columns["expiry_date"]

    expiries = pd.to_datetime(
        df[expiry_column],
        errors="coerce"
    ).dropna().dt.date.tolist()

    expiries = sorted(set(expiries))

    if not expiries:

        raise ValueError(
            f"No valid expiry dates in {calendar_path}"
        )

    logger.info(
        "Loaded %s expiry dates from %s",
        len(expiries),
        calendar_path.name
    )

    return expiries


def get_next_expiry(trading_date, expiry_dates):

    for expiry in expiry_dates:

        if expiry >= trading_date:

            return expiry

    return None


# ============================================================
# ATM / ITM CALCULATION
# ============================================================

def calculate_atm(index_open, strike_width):

    return (
        int(
            math.floor(
                index_open / strike_width + 0.5
            )
        )
        * strike_width
    )


def calculate_option_strikes(index_open, strike_width):

    atm = calculate_atm(
        index_open,
        strike_width
    )

    ce = {

        "ATM": atm,

        "ITM1": atm - strike_width,

        "ITM2": atm - (2 * strike_width)
    }

    pe = {

        "ATM": atm,

        "ITM1": atm + strike_width,

        "ITM2": atm + (2 * strike_width)
    }

    return ce, pe


# ============================================================
# API REQUEST HANDLER
# ============================================================

def call_historical_api(breeze, request_params):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = breeze.get_historical_data_v2(
                **request_params
            )

            if not isinstance(response, dict):

                raise RuntimeError(
                    "Unexpected Breeze API response"
                )

            error = response.get("Error")

            if error:

                raise RuntimeError(str(error))

            status = response.get("Status")

            if status is not None and int(status) != 200:

                raise RuntimeError(
                    f"Breeze API status: {status}"
                )

            records = response.get("Success") or []

            return records

        except Exception as exc:

            logger.warning(
                "API request failed | Attempt %s/%s | %s",
                attempt,
                MAX_RETRIES,
                exc
            )

            if attempt == MAX_RETRIES:

                logger.error(
                    "Maximum retries reached."
                )

                raise

            time.sleep(2 ** attempt)

    return []


# ============================================================
# COMMON REQUEST PARAMETERS
# ============================================================

def get_common_request_params(trading_date):

    return {

        "interval": INTERVAL,

        "from_date": get_api_timestamp(
            trading_date,
            MARKET_START
        ),

        "to_date": get_api_timestamp(
            trading_date,
            MARKET_END
        )
    }


# ============================================================
# SPOT DATA DOWNLOAD
# ============================================================

def download_spot_data(
    breeze,
    index_name,
    config,
    trading_date
):

    params = get_common_request_params(
        trading_date
    )

    params.update({

        "stock_code": config["stock_code"],

        "exchange_code": config["spot_exchange"],

        "product_type": "cash"
    })

    records = call_historical_api(
        breeze,
        params
    )

    if not records:

        logger.warning(
            "%s | No spot data for %s",
            index_name,
            trading_date
        )

        return pd.DataFrame()

    df = pd.DataFrame(records)

    df = parse_datetime_column(df)

    # Retain regular trading session only.
    time_values = df["datetime"].dt.strftime("%H:%M")

    df = df[
        (time_values >= "09:15") &
        (time_values <= "15:30")
    ].copy()

    if df.empty:

        return df

    # Numeric columns.
    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "open_interest"
    ]:

        if col in df.columns:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

    df["index_name"] = index_name

    df["trading_date"] = trading_date.isoformat()

    return df


# ============================================================
# GET INDEX OPENING PRICE
# ============================================================

def get_index_opening_price(spot_df):

    if spot_df.empty:

        return None

    df = spot_df.sort_values(
        "datetime"
    )

    opening_candle = df[
        df["datetime"].dt.strftime("%H:%M") == "09:15"
    ]

    if opening_candle.empty:

        logger.warning(
            "09:15 opening candle not available."
        )

        return None

    opening_price = opening_candle.iloc[0]["open"]

    if pd.isna(opening_price):

        return None

    return float(opening_price)


# ============================================================
# OPTION NAME GENERATION
# ============================================================

def generate_option_name(
    stock_code,
    strike,
    right,
    expiry_date
):

    direction = right.upper()

    return (
        f"{stock_code}_"
        f"{strike}_"
        f"{direction}_"
        f"EXP_{expiry_date.isoformat()}"
    )


# ============================================================
# INDIVIDUAL OPTION CONTRACT DOWNLOAD
# ============================================================

def download_option_contract(
    breeze,
    index_name,
    config,
    trading_date,
    expiry_date,
    index_open,
    strike,
    right,
    moneyness,
    force=False
):

    option_name = generate_option_name(
        config["stock_code"],
        strike,
        right,
        expiry_date
    )

    file_path = get_option_file_path(
        index_name,
        trading_date,
        option_name,
        moneyness
    )

    # Skip an already downloaded contract.
    if not force and is_valid_existing_file(file_path):

        logger.info(
            "ALREADY EXISTS | %s",
            file_path.name
        )

        return True

    params = get_common_request_params(
        trading_date
    )

    params.update({

        "stock_code": config["stock_code"],

        "exchange_code": config["option_exchange"],

        "product_type": "options",

        "expiry_date": get_expiry_timestamp(
            expiry_date
        ),

        "right": right.lower(),

        "strike_price": str(strike)
    })

    logger.info(
        "Downloading | %s | %s | %s | Strike %s | %s",
        index_name,
        trading_date,
        right.upper(),
        strike,
        moneyness
    )

    records = call_historical_api(
        breeze,
        params
    )

    if not records:

        logger.warning(
            "NO OPTION DATA | %s | %s",
            option_name,
            trading_date
        )

        return False

    df = pd.DataFrame(records)

    df = parse_datetime_column(df)

    if df.empty:

        return False

    # Retain regular market session only.
    time_values = df["datetime"].dt.strftime("%H:%M")

    df = df[
        (time_values >= "09:15") &
        (time_values <= "15:30")
    ].copy()

    if df.empty:

        return False

    # Convert numerical fields.
    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "open_interest"
    ]:

        if col in df.columns:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

    # --------------------------------------------------------
    # CONTRACT METADATA
    # --------------------------------------------------------

    df["index_name"] = index_name

    df["option_name"] = option_name

    df["trading_date"] = trading_date.isoformat()

    df["expiry_date"] = expiry_date.isoformat()

    df["strike"] = strike

    df["option_type"] = right.upper()

    df["moneyness"] = moneyness

    df["index_open"] = index_open

    # Save this contract independently.
    return save_csv_atomic(
        df,
        file_path
    )


# ============================================================
# DAILY OPTION CONTRACT GENERATION
# ============================================================

def generate_daily_contracts(
    index_open,
    strike_width
):

    ce_strikes, pe_strikes = calculate_option_strikes(
        index_open,
        strike_width
    )

    contracts = []

    for moneyness, strike in ce_strikes.items():

        contracts.append({

            "right": "call",

            "strike": strike,

            "moneyness": moneyness
        })

    for moneyness, strike in pe_strikes.items():

        contracts.append({

            "right": "put",

            "strike": strike,

            "moneyness": moneyness
        })

    return contracts


# ============================================================
# PROCESS ONE TRADING DAY
# ============================================================

def process_trading_day(
    breeze,
    index_name,
    config,
    expiry_dates,
    trading_date,
    force=False
):

    logger.info("-" * 75)

    logger.info(
        "%s | Processing %s",
        index_name,
        trading_date
    )

    # --------------------------------------------------------
    # 1. SPOT DATA
    # --------------------------------------------------------

    spot_file = get_spot_file_path(
        index_name,
        trading_date
    )

    if not force and is_valid_existing_file(spot_file):

        spot_df = pd.read_csv(
            spot_file,
            parse_dates=["datetime"]
        )

        logger.info(
            "SPOT ALREADY EXISTS | %s",
            trading_date
        )

    else:

        spot_df = download_spot_data(
            breeze,
            index_name,
            config,
            trading_date
        )

        if spot_df.empty:

            logger.info(
                "%s | No spot data. Skipping.",
                trading_date
            )

            return False

        save_csv_atomic(
            spot_df,
            spot_file
        )

    # --------------------------------------------------------
    # 2. INDEX OPEN
    # --------------------------------------------------------

    index_open = get_index_opening_price(
        spot_df
    )

    if index_open is None:

        logger.warning(
            "%s | Cannot calculate ATM. Skipping.",
            trading_date
        )

        return False

    # --------------------------------------------------------
    # 3. EXPIRY
    # --------------------------------------------------------

    expiry_date = get_next_expiry(
        trading_date,
        expiry_dates
    )

    if expiry_date is None:

        logger.warning(
            "%s | No applicable expiry.",
            trading_date
        )

        return False

    # --------------------------------------------------------
    # 4. GENERATE SIX CONTRACTS
    # --------------------------------------------------------

    contracts = generate_daily_contracts(
        index_open,
        config["strike_width"]
    )

    logger.info(
        "%s | Index Open: %.2f | ATM: %s | Expiry: %s",
        index_name,
        index_open,
        calculate_atm(
            index_open,
            config["strike_width"]
        ),
        expiry_date
    )

    # --------------------------------------------------------
    # 5. DOWNLOAD EACH CONTRACT SEPARATELY
    # --------------------------------------------------------

    successful_contracts = 0

    failed_contracts = []

    for contract in contracts:

        try:

            success = download_option_contract(

                breeze=breeze,

                index_name=index_name,

                config=config,

                trading_date=trading_date,

                expiry_date=expiry_date,

                index_open=index_open,

                strike=contract["strike"],

                right=contract["right"],

                moneyness=contract["moneyness"],

                force=force
            )

            if success:

                successful_contracts += 1

            else:

                failed_contracts.append(
                    contract
                )

        except Exception:

            logger.exception(
                "Contract download failed | %s | %s",
                trading_date,
                contract
            )

            failed_contracts.append(
                contract
            )

        time.sleep(API_DELAY_SECONDS)

    # --------------------------------------------------------
    # 6. DAILY COMPLETION STATUS
    # --------------------------------------------------------

    logger.info(
        "%s | %s | Successful: %s/6 | Failed: %s",
        index_name,
        trading_date,
        successful_contracts,
        len(failed_contracts)
    )

    if failed_contracts:

        logger.warning(
            "INCOMPLETE DAY | %s | Missing contracts: %s",
            trading_date,
            failed_contracts
        )

        return False

    logger.info(
        "DAY COMPLETED SUCCESSFULLY | %s | %s",
        index_name,
        trading_date
    )

    return True


# ============================================================
# PROCESS COMPLETE INDEX
# ============================================================

def process_index(
    breeze,
    index_name,
    start_date,
    end_date,
    force=False
):

    config = INDEX_CONFIG[index_name]

    expiry_dates = load_expiry_calendar(
        config["calendar"]
    )

    logger.info("=" * 80)

    logger.info(
        "STARTING INDEX: %s | %s TO %s",
        index_name,
        start_date,
        end_date
    )

    completed_days = 0

    incomplete_days = 0

    for trading_date in get_date_range(
        start_date,
        end_date
    ):

        # Skip weekends.
        if trading_date.weekday() >= 5:

            continue

        try:

            success = process_trading_day(

                breeze=breeze,

                index_name=index_name,

                config=config,

                expiry_dates=expiry_dates,

                trading_date=trading_date,

                force=force
            )

            if success:

                completed_days += 1

            else:

                incomplete_days += 1

        except Exception:

            incomplete_days += 1

            logger.exception(
                "%s | Failed trading date: %s",
                index_name,
                trading_date
            )

    logger.info("=" * 80)

    logger.info(
        "%s EXTRACTION FINISHED | "
        "Completed Days: %s | Incomplete Days: %s",
        index_name,
        completed_days,
        incomplete_days
    )


# ============================================================
# MAIN
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "ICICI Breeze Multi-Index Historical "
            "Options Data Extractor"
        )
    )

    parser.add_argument(
        "--start-date",
        default=START_DATE
    )

    parser.add_argument(
        "--end-date",
        default=END_DATE
    )

    parser.add_argument(
        "--index",
        choices=[
            "NIFTY50",
            "BANKNIFTY",
            "SENSEX",
            "ALL"
        ],
        default="ALL"
    )

    parser.add_argument(
        "--month",
        help="Example: 2025-06"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Redownload existing spot and option files"
    )

    args = parser.parse_args()

    start_date = datetime.strptime(
        args.start_date,
        "%Y-%m-%d"
    ).date()

    end_date = datetime.strptime(
        args.end_date,
        "%Y-%m-%d"
    ).date()

    if args.month:

        period = pd.Period(
            args.month,
            freq="M"
        )

        start_date = max(
            start_date,
            period.start_time.date()
        )

        end_date = min(
            end_date,
            period.end_time.date()
        )

    if start_date > end_date:

        raise ValueError(
            "Invalid extraction date range."
        )

    # Authenticate once.
    breeze = create_breeze_client()

    if args.index == "ALL":

        indexes = list(
            INDEX_CONFIG.keys()
        )

    else:

        indexes = [
            args.index
        ]

    for index_name in indexes:

        process_index(

            breeze=breeze,

            index_name=index_name,

            start_date=start_date,

            end_date=end_date,

            force=args.force
        )

    logger.info(
        "ALL REQUESTED INDEX EXTRACTIONS FINISHED."
    )


if __name__ == "__main__":

    main()
