import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path
import pandas as pd

from project_config import settings
from extraction.breeze_client import connect_breeze
from extraction.calendar_utils import (
    load_date_column, get_trading_dates, get_expiry_for_trade_date,
    build_chunks, month_start, next_month
)
from extraction.api import request_chunk, normalize_rows
from extraction.storage import safe_name, save_rows, append_csv_row, file_has_data

LOG = logging.getLogger("nifty_opening_spike_extractor")

def setup_logging():
    settings.REPORT_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(settings.REPORT_DIR / "extraction.log", encoding="utf-8")
        ]
    )

def option_path(trade_date, expiry, right, strike):
    folder = settings.RAW_OPTIONS_DIR / trade_date.strftime("%Y") / trade_date.strftime("%m")
    name = "%s_%s_%s_%s_%s.csv" % (
        trade_date.isoformat(), expiry.isoformat(), right.upper(), str(strike), "1sec"
    )
    return folder / name

def fetch_instrument(breeze, trade_date, base_params, meta, output_path, force=False):
    if file_has_data(output_path) and not force:
        LOG.info("SKIP existing: %s", output_path)
        return "SKIPPED_EXISTS", 0

    all_rows = []
    chunks = build_chunks(trade_date, settings.CHUNK_SECONDS)
    for chunk_no, (start_dt, end_dt) in enumerate(chunks, 1):
        try:
            rows = request_chunk(breeze, base_params, start_dt, end_dt)
            rows = normalize_rows(rows, meta)
            all_rows.extend(rows)
            LOG.info(
                "%s %s chunk %d/%d returned %d rows",
                trade_date, meta.get("instrument"), chunk_no, len(chunks), len(rows)
            )
        except Exception:
            LOG.exception(
                "REQUEST_FAILED date=%s instrument=%s chunk=%d",
                trade_date, meta.get("instrument"), chunk_no
            )
            return "REQUEST_FAILED", len(all_rows)

    if not all_rows:
        return "NO_DATA", 0
    saved = save_rows(all_rows, output_path)
    return "SUCCESS", saved

def calls_in_manifest_estimate(number_days, instruments_per_day):
    chunks = len(build_chunks(date(2026, 1, 1), settings.CHUNK_SECONDS))
    return number_days * instruments_per_day * chunks

def write_status(trade_date, instrument, status, rows, path, details=""):
    append_csv_row({
        "trade_date": trade_date.isoformat(),
        "instrument": instrument,
        "status": status,
        "rows_returned": rows,
        "file_path": str(path),
        "details": details
    }, settings.REPORT_DIR / "extraction_status.csv")

def run_for_dates(breeze, dates, force=False, include_futures=False):
    option_expiries = load_date_column(settings.EXPIRY_CALENDAR, "expiry_date")
    futures_expiries = load_date_column(settings.FUTURES_EXPIRY_CALENDAR, "expiry_date")
    if not option_expiries:
        raise RuntimeError(
            "No option expiries found. Populate config/expiry_calendar.csv with "
            "actual historical NIFTY expiry dates (YYYY-MM-DD), one per row."
        )

    calls_today = 0
    current_api_date = None

    for trade_date in dates:
        if current_api_date != trade_date:
            current_api_date = trade_date
            calls_today = 0

        expiry = get_expiry_for_trade_date(trade_date, option_expiries)
        if expiry is None:
            write_status(trade_date, "OPTIONS_UNIVERSE", "MISSING_EXPIRY", 0, "", "No expiry >= trade date")
            LOG.warning("No option expiry configured for %s", trade_date)
            continue

        # NIFTY cash index
        spot_path = settings.RAW_SPOT_DIR / trade_date.strftime("%Y") / (
            trade_date.isoformat() + "_NIFTY_SPOT_1sec.csv"
        )
        spot_params = {
            "interval": settings.API_INTERVAL,
            "stock_code": "NIFTY",
            "exchange_code": "NSE",
            "product_type": "cash"
        }
        status, rows = fetch_instrument(
            breeze, trade_date, spot_params,
            {"trade_date": trade_date.isoformat(), "instrument": "NIFTY_SPOT"},
            spot_path, force
        )
        write_status(trade_date, "NIFTY_SPOT", status, rows, spot_path)
        calls_today += len(build_chunks(trade_date, settings.CHUNK_SECONDS))

        # Determine ATM from the first available spot candle's open.
        # If spot is missing, do not guess strikes; record and move on.
        if not file_has_data(spot_path):
            LOG.warning("Spot data unavailable for %s; skipping options to avoid guessed ATM", trade_date)
            continue
        spot_df = pd.read_csv(spot_path)
        if spot_df.empty or "open" not in spot_df.columns:
            write_status(trade_date, "OPTIONS_UNIVERSE", "INVALID_SPOT_DATA", 0, spot_path)
            continue
        opening_spot = float(spot_df.iloc[0]["open"])
        atm = int(round(opening_spot / settings.STRIKE_STEP) * settings.STRIKE_STEP)

        strikes = [
            atm + offset * settings.STRIKE_STEP
            for offset in range(-settings.STRIKES_EACH_SIDE, settings.STRIKES_EACH_SIDE + 1)
        ]

        for strike in strikes:
            for right, label in (("call", "CE"), ("put", "PE")):
                instrument = "NIFTY_%s_%s_%s" % (expiry.isoformat(), strike, label)
                out_path = option_path(trade_date, expiry, label, strike)
                params = {
                    "interval": settings.API_INTERVAL,
                    "stock_code": "NIFTY",
                    "exchange_code": "NFO",
                    "product_type": "options",
                    "expiry_date": expiry.strftime("%Y-%m-%dT07:00:00.000Z"),
                    "right": right,
                    "strike_price": str(strike)
                }
                meta = {
                    "trade_date": trade_date.isoformat(),
                    "instrument": instrument,
                    "expiry_date_requested": expiry.isoformat(),
                    "strike_price_requested": strike,
                    "right_requested": right,
                    "atm_reference_spot_open": opening_spot
                }
                status, rows = fetch_instrument(
                    breeze, trade_date, params, meta, out_path, force
                )
                write_status(trade_date, instrument, status, rows, out_path)
                calls_today += len(build_chunks(trade_date, settings.CHUNK_SECONDS))
                if calls_today >= settings.DAILY_CALLS_SOFT_LIMIT:
                    raise RuntimeError(
                        "Daily API soft limit reached. Stop and resume on a later day."
                    )

        if include_futures and futures_expiries:
            fut_expiry = get_expiry_for_trade_date(trade_date, futures_expiries)
            if fut_expiry:
                fut_path = settings.RAW_FUTURES_DIR / trade_date.strftime("%Y") / (
                    trade_date.isoformat() + "_NIFTY_FUTURES_1sec.csv"
                )
                fut_params = {
                    "interval": settings.API_INTERVAL,
                    "stock_code": "NIFTY",
                    "exchange_code": "NFO",
                    "product_type": "futures",
                    "expiry_date": fut_expiry.strftime("%Y-%m-%dT07:00:00.000Z"),
                    "right": "others",
                    "strike_price": "0"
                }
                status, rows = fetch_instrument(
                    breeze, trade_date, fut_params,
                    {
                        "trade_date": trade_date.isoformat(),
                        "instrument": "NIFTY_FUTURES",
                        "expiry_date_requested": fut_expiry.isoformat()
                    },
                    fut_path, force
                )
                write_status(trade_date, "NIFTY_FUTURES", status, rows, fut_path)

def month_ranges(start_date, end_date):
    cursor = month_start(start_date)
    while cursor <= end_date:
        nxt = next_month(cursor)
        yield max(cursor, start_date), min(nxt - timedelta(days=1), end_date)
        cursor = nxt

def export_monthly_summary():
    status_file = settings.REPORT_DIR / "extraction_status.csv"
    if not status_file.exists():
        return
    df = pd.read_csv(status_file)
    if df.empty:
        return
    df["month"] = df["trade_date"].astype(str).str[:7]
    summary = df.groupby(["month", "status"]).size().unstack(fill_value=0).reset_index()
    for month, group in df.groupby("month"):
        out = settings.MONTHLY_REPORT_DIR / (month + "_extraction_status.csv")
        group.drop(columns=["month"]).to_csv(out, index=False)
    summary.to_csv(settings.REPORT_DIR / "monthly_summary.csv", index=False)

    # Keep a focused exception file for quick data-quality review.
    exceptions = df[~df["status"].isin(["SUCCESS", "SKIPPED_EXISTS"])].copy()
    exceptions.to_csv(settings.REPORT_DIR / "missing_data.csv", index=False)

def choose_validation_dates(start_date, end_date, count):
    dates = get_trading_dates(start_date, end_date, settings.TRADING_CALENDAR)
    return dates[-count:]

def main():
    setup_logging()
    parser = argparse.ArgumentParser(
        description="ICICI Breeze NIFTY 1-second opening-window historical extractor"
    )
    parser.add_argument("--mode", choices=["validate", "monthly", "range"], default="validate")
    parser.add_argument("--start-date", help="YYYY-MM-DD; defaults to approx 3 years ago")
    parser.add_argument("--end-date", help="YYYY-MM-DD; defaults to yesterday")
    parser.add_argument("--validation-days", type=int, default=settings.VALIDATION_DAYS)
    parser.add_argument("--month", help="YYYY-MM for monthly mode")
    parser.add_argument("--force", action="store_true", help="Redownload existing files")
    parser.add_argument("--include-futures", action="store_true")
    args = parser.parse_args()

    today = date.today()
    end_date = date.fromisoformat(args.end_date) if args.end_date else today - timedelta(days=1)
    start_date = date.fromisoformat(args.start_date) if args.start_date else (
        end_date - timedelta(days=365 * settings.DEFAULT_LOOKBACK_YEARS)
    )

    if args.mode == "validate":
        dates = choose_validation_dates(start_date, end_date, args.validation_days)
        if not dates:
            raise RuntimeError("No candidate weekdays found in selected validation range.")
        LOG.info("Validation dates: %s through %s (%d candidate sessions)",
                 dates[0], dates[-1], len(dates))
        # Validation mode only downloads the selected sample.
    elif args.mode == "monthly":
        if not args.month:
            parser.error("--month YYYY-MM is required for monthly mode")
        month_date = date.fromisoformat(args.month + "-01")
        start_date = max(start_date, month_date)
        end_date = min(end_date, next_month(month_date) - timedelta(days=1))
        dates = get_trading_dates(start_date, end_date, settings.TRADING_CALENDAR)
    else:
        dates = get_trading_dates(start_date, end_date, settings.TRADING_CALENDAR)

    if not dates:
        raise RuntimeError("No candidate trading dates in requested range.")

    LOG.info("Starting %s: %s to %s; dates=%d", args.mode, dates[0], dates[-1], len(dates))
    breeze = connect_breeze()
    run_for_dates(breeze, dates, force=args.force, include_futures=args.include_futures)
    export_monthly_summary()
    LOG.info("Extraction run completed. Review reports/extraction_status.csv and missing_data.csv.")

if __name__ == "__main__":
    main()
