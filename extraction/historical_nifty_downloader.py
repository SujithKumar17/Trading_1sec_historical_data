import argparse
import csv
import json
import logging
import math
import os
import tempfile
from datetime import date, datetime, time, timedelta
from pathlib import Path

import pandas as pd

from extraction.api import request_chunk
from extraction.breeze_client import connect_breeze
from extraction.calendar_utils import (
    get_expiry_for_trade_date,
    get_trading_dates,
    load_date_column,
)
from project_config import settings, settings2


LOGGER = logging.getLogger("nifty50_historical_downloader")
CANDLE_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]
FUTURES_COLUMNS = CANDLE_COLUMNS + ["open_interest"]
OPTION_COLUMNS = CANDLE_COLUMNS + [
    "open_interest",
    "strike_price",
    "option_name",
    "expiry_date",
]
STATUS_COLUMNS = [
    "trade_date",
    "instrument",
    "strike",
    "expiry",
    "category",
    "timeframe",
    "download_status",
    "record_count",
    "error",
    "file_path",
]
TIMEFRAME_SECONDS = {"1second": 1, "1minute": 60, "5minute": 300}
OPTION_RIGHTS = {
    "CALL": ("call", -1),
    "PUT": ("put", 1),
}


class DailyRequestLimitReached(RuntimeError):
    pass


class RequestBudget:
    def __init__(self, limit):
        self.limit = limit
        self.path = settings.HISTORICAL_REQUEST_BUDGET_FILE
        self.request_date = date.today().isoformat()
        self.calls = self._load_calls()

    def _load_calls(self):
        if not self.path.exists():
            return 0
        with self.path.open("r", encoding="utf-8") as budget_file:
            data = json.load(budget_file)
        if data.get("request_date") != self.request_date:
            return 0
        calls = data.get("calls")
        if not isinstance(calls, int) or calls < 0:
            raise ValueError("Invalid daily request budget file: %s" % self.path)
        return calls

    def reserve(self):
        if self.calls >= self.limit:
            raise DailyRequestLimitReached(
                "The per-run request safety limit (%d) has been reached. "
                "Rerun the command to resume from the last completed files."
                % self.limit
            )
        self.calls += 1
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file_descriptor, temporary_name = tempfile.mkstemp(
            prefix=self.path.stem + "_",
            suffix=".tmp",
            dir=str(self.path.parent),
        )
        os.close(file_descriptor)
        temporary_path = Path(temporary_name)
        try:
            with temporary_path.open("w", encoding="utf-8") as budget_file:
                json.dump(
                    {"request_date": self.request_date, "calls": self.calls},
                    budget_file,
                )
            os.replace(str(temporary_path), str(self.path))
        except Exception:
            if temporary_path.exists():
                temporary_path.unlink()
            self.calls -= 1
            raise


def configure_logging():
    settings.REPORT_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                settings.REPORT_DIR / "NIFTY50_historical_downloader.log",
                encoding="utf-8",
            ),
        ],
    )


def session_bounds(trade_date):
    start = datetime.combine(trade_date, time.fromisoformat(settings.START_TIME))
    end = datetime.combine(trade_date, time.fromisoformat(settings.END_TIME))
    if start >= end:
        raise ValueError("START_TIME must be earlier than END_TIME")
    return start, end


def build_chunks(trade_date, timeframe):
    if timeframe not in TIMEFRAME_SECONDS:
        raise ValueError("Unsupported timeframe: %s" % timeframe)
    start, end = session_bounds(trade_date)
    step_seconds = TIMEFRAME_SECONDS[timeframe]
    candle_limit = min(
        settings.HISTORICAL_CHUNK_CANDLES,
        settings.MAX_CANDLES_PER_REQUEST,
    )
    if candle_limit < 1:
        raise ValueError("Historical request candle limits must be positive")
    chunk_length = timedelta(
        seconds=step_seconds * candle_limit
    )
    interval = timedelta(seconds=step_seconds)
    chunks = []
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + chunk_length, end)
        chunks.append((cursor, chunk_end - interval))
        cursor = chunk_end
    return chunks


def calculate_atm(opening_spot, strike_step):
    if strike_step <= 0:
        raise ValueError("Strike interval must be positive")
    if not math.isfinite(float(opening_spot)) or float(opening_spot) <= 0:
        raise ValueError("Opening spot must be a positive finite number")
    return int(math.floor(float(opening_spot) / strike_step + 0.5) * strike_step)


def build_option_contracts(atm, strike_step, depth):
    if strike_step <= 0 or depth < 1:
        raise ValueError("Strike interval and option depth must be positive")
    contracts = []
    for right_name, (api_right, itm_direction) in OPTION_RIGHTS.items():
        contracts.append(
            {
                "right_name": right_name,
                "api_right": api_right,
                "strike": atm,
                "category": "ATM",
            }
        )
        for offset in range(1, depth + 1):
            contracts.append(
                {
                    "right_name": right_name,
                    "api_right": api_right,
                    "strike": atm + offset * strike_step * itm_direction,
                    "category": "ITM%d" % offset,
                }
            )
            contracts.append(
                {
                    "right_name": right_name,
                    "api_right": api_right,
                    "strike": atm - offset * strike_step * itm_direction,
                    "category": "OTM%d" % offset,
                }
            )
    return contracts


def _timestamp_as_ist(value):
    parsed = pd.Timestamp(value)
    if pd.isna(parsed):
        raise ValueError("Invalid candle timestamp: %r" % value)
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert("Asia/Kolkata").tz_localize(None)
    return parsed


def normalize_candles(
    rows,
    trade_date,
    timeframe,
    option_metadata=None,
    include_open_interest=False,
):
    include_open_interest = include_open_interest or option_metadata is not None
    columns = (
        OPTION_COLUMNS
        if option_metadata is not None
        else FUTURES_COLUMNS
        if include_open_interest
        else CANDLE_COLUMNS
    )
    if not rows:
        return pd.DataFrame(columns=columns)
    raw = pd.DataFrame(rows)
    timestamp_column = "datetime" if "datetime" in raw.columns else "timestamp"
    required = {timestamp_column, "open", "high", "low", "close", "volume"}
    oi_column = "open_interest" if "open_interest" in raw.columns else "oi"
    if include_open_interest:
        required.add(oi_column)
    missing = required.difference(raw.columns)
    if missing:
        raise ValueError("Breeze candles missing fields: %s" % sorted(missing))

    frame = pd.DataFrame()
    frame["timestamp"] = [
        _timestamp_as_ist(value) for value in raw[timestamp_column]
    ]
    for column in ("open", "high", "low", "close", "volume"):
        frame[column] = pd.to_numeric(raw[column], errors="coerce")
    if include_open_interest:
        frame["open_interest"] = pd.to_numeric(raw[oi_column], errors="coerce")
    numeric = frame[["open", "high", "low", "close", "volume"]]
    if numeric.isna().any().any() or not all(
        math.isfinite(float(value)) for value in numeric.to_numpy().flat
    ):
        raise ValueError("Breeze candles contain missing or non-numeric OHLCV values")
    if (numeric[["open", "high", "low", "close"]] < 0).any().any():
        raise ValueError("Breeze candles contain negative prices")
    if (numeric["volume"] < 0).any():
        raise ValueError("Breeze candles contain negative volume")
    if include_open_interest:
        open_interest = frame["open_interest"]
        if (
            open_interest.isna().any()
            or not all(math.isfinite(float(value)) for value in open_interest)
        ):
            raise ValueError("Breeze candles contain missing or non-numeric open interest")
        if (open_interest < 0).any():
            raise ValueError("Breeze candles contain negative open interest")
    if (numeric["high"] < numeric["low"]).any():
        raise ValueError("Breeze candles contain high values below low")
    if (
        (numeric["open"] > numeric["high"])
        | (numeric["open"] < numeric["low"])
        | (numeric["close"] > numeric["high"])
        | (numeric["close"] < numeric["low"])
    ).any():
        raise ValueError("Breeze candle open/close is outside its high/low range")

    start, end = session_bounds(trade_date)
    frame = frame[(frame["timestamp"] >= start) & (frame["timestamp"] < end)]
    step_seconds = TIMEFRAME_SECONDS[timeframe]
    aligned = frame["timestamp"].map(
        lambda value: (
            value.second == 0 and value.minute % (step_seconds // 60) == 0
            if step_seconds >= 60
            else True
        )
    )
    if not aligned.all():
        raise ValueError("Breeze returned timestamps not aligned to %s" % timeframe)

    frame = frame.drop_duplicates(subset=["timestamp"], keep="last")
    frame = frame.sort_values("timestamp")
    frame["timestamp"] = frame["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    if option_metadata is not None:
        for column in ("strike_price", "option_name", "expiry_date"):
            frame[column] = option_metadata[column]
    return frame[columns].reset_index(drop=True)


def validate_saved_csv(
    path,
    trade_date,
    timeframe,
    option_metadata=None,
    include_open_interest=False,
):
    frame = pd.read_csv(path)
    include_open_interest = include_open_interest or option_metadata is not None
    columns = (
        OPTION_COLUMNS
        if option_metadata is not None
        else FUTURES_COLUMNS
        if include_open_interest
        else CANDLE_COLUMNS
    )
    if list(frame.columns) != columns:
        raise ValueError("CSV columns do not match the canonical candle schema")
    if option_metadata is not None:
        for column, expected in option_metadata.items():
            if not frame[column].astype(str).eq(str(expected)).all():
                raise ValueError("CSV contains unexpected option metadata: %s" % column)
    normalized = normalize_candles(
        frame.to_dict("records"),
        trade_date,
        timeframe,
        option_metadata,
        include_open_interest,
    )
    if len(normalized) != len(frame):
        raise ValueError("CSV contains duplicate or out-of-session candles")
    if frame.empty:
        raise ValueError("CSV contains no candles")
    original_timestamps = frame["timestamp"].astype(str).tolist()
    if original_timestamps != normalized["timestamp"].tolist():
        raise ValueError("CSV timestamps are not canonical and chronological")
    return normalized


def _append_csv_chunk(frame, path, partial_path, first_chunk):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    partial_path.write_text("in progress\n", encoding="utf-8")
    with path.open(
        "w" if first_chunk else "a", newline="", encoding="utf-8"
    ) as output_file:
        frame.to_csv(
            output_file,
            index=False,
            header=first_chunk,
            columns=frame.columns,
        )
        output_file.flush()
        os.fsync(output_file.fileno())


def save_status(record):
    path = settings.HISTORICAL_STATUS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    if not write_header:
        with path.open("r", newline="", encoding="utf-8") as status_file:
            header = next(csv.reader(status_file), [])
        if header != STATUS_COLUMNS:
            raise ValueError("Download status file has an unexpected schema: %s" % path)
    with path.open("a", newline="", encoding="utf-8") as status_file:
        writer = csv.DictWriter(status_file, fieldnames=STATUS_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerow(record)


def _status_record(
    trade_date,
    instrument,
    timeframe,
    status,
    record_count=0,
    strike="",
    expiry="",
    category="",
    error="",
    file_path="",
):
    return {
        "trade_date": trade_date.isoformat(),
        "instrument": instrument,
        "strike": strike,
        "expiry": expiry,
        "category": category,
        "timeframe": timeframe,
        "download_status": status,
        "record_count": record_count,
        "error": str(error),
        "file_path": str(file_path),
    }


def _path_for(trade_date, timeframe, instrument, filename):
    path = (
        settings.HISTORICAL_OUTPUT_ROOT
        / timeframe
        / instrument
        / trade_date.strftime("%Y")
        / trade_date.strftime("%m")
    )
    if instrument == "options":
        path /= trade_date.isoformat()
    return path / filename


def _fetch_candles(
    breeze,
    base_params,
    trade_date,
    timeframe,
    instrument,
    output_path,
    budget,
    progress,
    option_metadata=None,
):
    output_path = Path(output_path)
    partial_path = output_path.with_name(output_path.name + ".partial")
    chunks = build_chunks(trade_date, timeframe)
    record_count = 0
    opening_spot = None

    for chunk_number, (start, end) in enumerate(chunks, start=1):
        budget.reserve()
        LOGGER.info(
            "Requesting %s %s chunk %d/%d (%s through %s)",
            instrument,
            timeframe,
            chunk_number,
            len(chunks),
            start.strftime("%H:%M:%S"),
            end.strftime("%H:%M:%S"),
        )
        chunk_rows = request_chunk(breeze, base_params, start, end)
        if chunk_rows is None:
            raise RuntimeError("Breeze returned a null Success value")
        if not isinstance(chunk_rows, list):
            raise RuntimeError(
                "Unexpected Breeze candle response type: %s" % type(chunk_rows).__name__
            )
        frame = normalize_candles(
            chunk_rows,
            trade_date,
            timeframe,
            option_metadata,
            include_open_interest=instrument == "NIFTY50_FUTURES",
        )
        if frame.empty:
            LOGGER.info(
                "No candles returned for %s %s chunk %d/%d",
                instrument,
                timeframe,
                chunk_number,
                len(chunks),
            )
            continue

        _append_csv_chunk(
            frame,
            output_path,
            partial_path,
            first_chunk=record_count == 0,
        )
        record_count += len(frame)
        if instrument == "NIFTY50_SPOT" and opening_spot is None:
            opening_spot = _opening_spot(frame, trade_date)
        progress["record_count"] = record_count
        progress["opening_spot"] = opening_spot
        LOGGER.info(
            "Wrote %d candles for %s %s chunk %d/%d (total: %d) to %s",
            len(frame),
            instrument,
            timeframe,
            chunk_number,
            len(chunks),
            record_count,
            output_path,
        )

    return record_count, opening_spot


def _download_to_path(
    breeze,
    trade_date,
    timeframe,
    instrument,
    base_params,
    output_path,
    budget,
    resume,
    overwrite,
    option_metadata=None,
):
    output_path = Path(output_path)
    partial_path = output_path.with_name(output_path.name + ".partial")
    if resume and not overwrite and output_path.exists() and not partial_path.exists():
        try:
            saved = validate_saved_csv(
                output_path,
                trade_date,
                timeframe,
                option_metadata,
                include_open_interest=instrument == "NIFTY50_FUTURES",
            )
            opening_spot = _opening_spot(saved, trade_date)
            if instrument == "NIFTY50_SPOT" and opening_spot is None:
                raise ValueError("Spot CSV is missing the 09:15 opening candle")
            LOGGER.info(
                "Skipping existing %s %s file: %s",
                instrument,
                timeframe,
                output_path,
            )
            return "SKIPPED_EXISTS", len(saved), "", opening_spot
        except (OSError, ValueError, pd.errors.ParserError) as exc:
            LOGGER.warning("Redownloading invalid file %s: %s", output_path, exc)
    elif partial_path.exists():
        LOGGER.warning(
            "Resuming incomplete %s %s download by redownloading: %s",
            instrument,
            timeframe,
            output_path,
        )

    LOGGER.info(
        "Starting %s %s download for %s; output: %s",
        instrument,
        timeframe,
        trade_date,
        output_path,
    )
    progress = {"record_count": 0, "opening_spot": None}
    try:
        _fetch_candles(
            breeze,
            base_params,
            trade_date,
            timeframe,
            instrument,
            output_path,
            budget,
            progress,
            option_metadata,
        )
        if progress["record_count"] == 0:
            LOGGER.warning(
                "No candles returned for %s %s on %s",
                instrument,
                timeframe,
                trade_date,
            )
            return "NO_DATA", 0, "", None
        if instrument == "NIFTY50_SPOT" and progress["opening_spot"] is None:
            return (
                "INCOMPLETE",
                progress["record_count"],
                "Breeze response is missing the 09:15 opening candle",
                None,
            )
        validate_saved_csv(
            output_path,
            trade_date,
            timeframe,
            option_metadata,
            include_open_interest=instrument == "NIFTY50_FUTURES",
        )
        if partial_path.exists():
            partial_path.unlink()
        LOGGER.info(
            "Completed %s %s download for %s: %d candles",
            instrument,
            timeframe,
            trade_date,
            progress["record_count"],
        )
        return "SUCCESS", progress["record_count"], "", progress["opening_spot"]
    except DailyRequestLimitReached:
        raise
    except Exception as exc:
        LOGGER.exception(
            "Historical download failed: date=%s instrument=%s timeframe=%s",
            trade_date,
            instrument,
            timeframe,
        )
        LOGGER.error(
            "%s %s download failed after writing %d candles; partial file: %s",
            instrument,
            timeframe,
            progress["record_count"],
            output_path,
        )
        return (
            "FAILED",
            progress["record_count"],
            str(exc),
            progress["opening_spot"],
        )


def _spot_params(timeframe):
    return {
        "interval": timeframe,
        "stock_code": "NIFTY",
        "exchange_code": "NSE",
        "product_type": "cash",
    }


def _opening_spot(frame, trade_date):
    if frame is None or frame.empty:
        return None
    opening_timestamp = datetime.combine(
        trade_date, time.fromisoformat(settings.START_TIME)
    ).strftime("%Y-%m-%d %H:%M:%S")
    opening_rows = frame[frame["timestamp"] == opening_timestamp]
    if opening_rows.empty:
        return None
    return float(opening_rows.iloc[0]["open"])


def _fetch_opening_spot(breeze, trade_date, budget):
    start, _ = session_bounds(trade_date)
    budget.reserve()
    rows = request_chunk(
        breeze,
        _spot_params("1second"),
        start,
        start,
    )
    frame = normalize_candles(rows or [], trade_date, "1second")
    return _opening_spot(frame, trade_date), len(frame)


def _option_expiry_timestamp(expiry):
    return expiry.strftime("%Y-%m-%dT07:00:00.000Z")


def _expiry_from_calendar(trade_date, expiries):
    if not expiries or trade_date < expiries[0] or trade_date > expiries[-1]:
        return None
    return get_expiry_for_trade_date(trade_date, expiries)


def _run_one_date(
    breeze,
    trade_date,
    timeframes,
    instruments,
    option_expiries,
    futures_expiries,
    budget,
    resume,
    overwrite,
):
    spot_reference = None
    reference_source = None
    LOGGER.info("Starting extraction for trading date %s", trade_date)

    if "spot" in instruments:
        preferred = [tf for tf in ("1second", "1minute", "5minute") if tf in timeframes]
        for timeframe in timeframes:
            filename = "NIFTY50_%s_SPOT.csv" % trade_date.isoformat()
            path = _path_for(trade_date, timeframe, "spot", filename)
            status, record_count, error, opening = _download_to_path(
                breeze,
                trade_date,
                timeframe,
                "NIFTY50_SPOT",
                _spot_params(timeframe),
                path,
                budget,
                resume,
                overwrite,
            )
            save_status(
                _status_record(
                    trade_date,
                    "NIFTY50_SPOT",
                    timeframe,
                    status,
                    record_count,
                    error=error,
                    file_path=path,
                )
            )
            if timeframe in preferred:
                if opening is not None and (
                    reference_source is None
                    or preferred.index(timeframe) < preferred.index(reference_source)
                ):
                    spot_reference = opening
                    reference_source = timeframe

    if "options" in instruments and spot_reference is None:
        try:
            spot_reference, count = _fetch_opening_spot(breeze, trade_date, budget)
            status = "SUCCESS" if spot_reference is not None else "MISSING_OPENING_CANDLE"
            error = "" if spot_reference is not None else "No 09:15 spot candle returned"
        except DailyRequestLimitReached:
            raise
        except Exception as exc:
            status = "FAILED"
            count = 0
            error = str(exc)
            LOGGER.exception("Could not fetch opening spot for %s", trade_date)
        save_status(
            _status_record(
                trade_date,
                "NIFTY50_SPOT_REFERENCE",
                "1second",
                status,
                count,
                error=error,
            )
        )
        reference_source = "1second"

    option_expiry = None
    if "options" in instruments and spot_reference is not None:
        option_expiry = _expiry_from_calendar(trade_date, option_expiries)
        if option_expiry is None:
            for timeframe in timeframes:
                save_status(
                    _status_record(
                        trade_date,
                        "NIFTY50_OPTIONS_UNIVERSE",
                        timeframe,
                        "MISSING_EXPIRY",
                        error="No option expiry on or after trade date in calendar",
                    )
                )

    futures_expiry = None
    if "futures" in instruments:
        futures_expiry = _expiry_from_calendar(trade_date, futures_expiries)

    strike_step = settings2.INDEX_CONFIG["NIFTY50"]["strike_width"]
    option_contracts = (
        build_option_contracts(
            calculate_atm(spot_reference, strike_step),
            strike_step,
            settings.OPTION_DEPTH,
        )
        if option_expiry is not None and spot_reference is not None
        else []
    )

    for timeframe in timeframes:
        if "futures" in instruments:
            if futures_expiry is None:
                save_status(
                    _status_record(
                        trade_date,
                        "NIFTY50_FUTURES",
                        timeframe,
                        "MISSING_EXPIRY",
                        error="No futures expiry on or after trade date in calendar",
                    )
                )
            else:
                filename = "NIFTY50_%s_FUTURES_EXP_%s.csv" % (
                    trade_date.isoformat(),
                    futures_expiry.isoformat(),
                )
                path = _path_for(trade_date, timeframe, "futures", filename)
                params = {
                    "interval": timeframe,
                    "stock_code": "NIFTY",
                    "exchange_code": "NFO",
                    "product_type": "futures",
                    "expiry_date": _option_expiry_timestamp(futures_expiry),
                    "right": "others",
                    "strike_price": "0",
                }
                status, record_count, error, _ = _download_to_path(
                    breeze,
                    trade_date,
                    timeframe,
                    "NIFTY50_FUTURES",
                    params,
                    path,
                    budget,
                    resume,
                    overwrite,
                )
                save_status(
                    _status_record(
                        trade_date,
                        "NIFTY50_FUTURES",
                        timeframe,
                        status,
                        record_count,
                        expiry=futures_expiry.isoformat(),
                        error=error,
                        file_path=path,
                    )
                )

        if "options" not in instruments or option_expiry is None:
            continue
        if spot_reference is None:
            save_status(
                _status_record(
                    trade_date,
                    "NIFTY50_OPTIONS_UNIVERSE",
                    timeframe,
                    "MISSING_OPENING_CANDLE",
                    error="A 09:15 spot open is required to select strikes",
                )
            )
            continue

        for contract in option_contracts:
            instrument = "NIFTY50_%s_%s" % (
                contract["right_name"],
                contract["strike"],
            )
            filename = "NIFTY50_%s_NIFTY_%s_%s_EXP_%s_%s.csv" % (
                trade_date.isoformat(),
                contract["strike"],
                contract["right_name"],
                option_expiry.isoformat(),
                contract["category"],
            )
            path = _path_for(trade_date, timeframe, "options", filename)
            params = {
                "interval": timeframe,
                "stock_code": "NIFTY",
                "exchange_code": "NFO",
                "product_type": "options",
                "expiry_date": _option_expiry_timestamp(option_expiry),
                "right": contract["api_right"],
                "strike_price": str(contract["strike"]),
            }
            status, record_count, error, _ = _download_to_path(
                breeze,
                trade_date,
                timeframe,
                instrument,
                params,
                path,
                budget,
                resume,
                overwrite,
                option_metadata={
                    "strike_price": contract["strike"],
                    "option_name": "NIFTY_%s_%s_EXP_%s"
                    % (
                        contract["strike"],
                        contract["right_name"],
                        option_expiry.isoformat(),
                    ),
                    "expiry_date": option_expiry.isoformat(),
                },
            )
            save_status(
                _status_record(
                    trade_date,
                    instrument,
                    timeframe,
                    status,
                    record_count,
                    strike=contract["strike"],
                    expiry=option_expiry.isoformat(),
                    category=contract["category"],
                    error=error,
                    file_path=path,
                )
            )

    LOGGER.info(
        "Processed %s; opening spot=%s; reference timeframe=%s",
        trade_date,
        spot_reference,
        reference_source,
    )


def run_download(start_date, end_date, timeframes, instruments, resume, overwrite):
    if start_date > end_date:
        raise ValueError("START_DATE must be on or before END_DATE")
    if end_date == date.today() and datetime.now().time() < time.fromisoformat(
        settings.END_TIME
    ):
        raise ValueError(
            "The current trading session is not complete; use an end date before "
            "today or rerun after the configured market close"
        )
    invalid_timeframes = set(timeframes).difference(TIMEFRAME_SECONDS)
    if invalid_timeframes:
        raise ValueError("Unsupported timeframes: %s" % sorted(invalid_timeframes))

    option_expiries = []
    futures_expiries = []
    if "options" in instruments:
        option_calendar = settings2.INDEX_CONFIG["NIFTY50"]["calendar"]
        option_expiries = load_date_column(option_calendar, "expiry_date")
        if not option_expiries:
            raise RuntimeError("No valid expiry dates in %s" % option_calendar)
    if "futures" in instruments:
        futures_expiries = load_date_column(
            settings.FUTURES_EXPIRY_CALENDAR, "expiry_date"
        )
        if not futures_expiries:
            raise RuntimeError(
                "No valid expiry dates in %s" % settings.FUTURES_EXPIRY_CALENDAR
            )

    trading_dates = get_trading_dates(
        start_date, end_date, settings.TRADING_CALENDAR
    )
    if not trading_dates:
        raise RuntimeError("No candidate trading dates in the requested range")
    LOGGER.info(
        "Starting %s through %s (%d candidate trading dates)",
        trading_dates[0],
        trading_dates[-1],
        len(trading_dates),
    )

    breeze = connect_breeze()
    budget = RequestBudget(settings.DAILY_CALLS_SOFT_LIMIT)
    for trade_date in trading_dates:
        _run_one_date(
            breeze,
            trade_date,
            timeframes,
            instruments,
            option_expiries,
            futures_expiries,
            budget,
            resume,
            overwrite,
        )
    LOGGER.info("Download run complete; API calls issued: %d", budget.calls)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Download NIFTY 50 historical spot, futures, and options data"
    )
    parser.add_argument(
        "--start-date",
        default=settings.HISTORICAL_START_DATE,
        help="First trading date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        default=settings.HISTORICAL_END_DATE
        or (date.today() - timedelta(days=1)).isoformat(),
        help="Last trading date (YYYY-MM-DD; defaults to yesterday)",
    )
    parser.add_argument(
        "--timeframes",
        nargs="+",
        choices=tuple(TIMEFRAME_SECONDS),
        default=settings.HISTORICAL_TIMEFRAMES,
    )
    parser.add_argument(
        "--instruments",
        nargs="+",
        choices=settings.HISTORICAL_INSTRUMENTS,
        default=settings.HISTORICAL_INSTRUMENTS,
    )
    resume_group = parser.add_mutually_exclusive_group()
    resume_group.add_argument(
        "--resume",
        dest="resume",
        action="store_true",
        default=settings.RESUME_DOWNLOAD,
        help="Skip existing valid CSV files (default)",
    )
    resume_group.add_argument(
        "--no-resume",
        dest="resume",
        action="store_false",
        help="Redownload files even when they are valid",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=settings.OVERWRITE_EXISTING,
        help="Replace existing files even when they are valid",
    )
    return parser


def main():
    configure_logging()
    parser = _build_parser()
    args = parser.parse_args()
    try:
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        run_download(
            start_date,
            end_date,
            args.timeframes,
            args.instruments,
            args.resume,
            args.overwrite,
        )
    except DailyRequestLimitReached as exc:
        LOGGER.warning("%s", exc)
        raise SystemExit(2) from exc
    except Exception:
        LOGGER.exception("Historical download did not complete")
        raise


if __name__ == "__main__":
    main()
