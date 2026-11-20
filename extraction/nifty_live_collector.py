
import os
import json
import time
import math
import queue
import logging
import threading

from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from breeze_connect import BreezeConnect


# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

API_KEY = os.getenv("ICICI_BREEZE_API_KEY")
API_SECRET = os.getenv("ICICI_BREEZE_SECRET_KEY")
SESSION_TOKEN = os.getenv("ICICI_BREEZE_SESSION_TOKEN")

if not all([API_KEY, API_SECRET, SESSION_TOKEN]):
    raise ValueError("Missing ICICI Breeze credentials in .env")

# Data storage
DATA_ROOT = Path(r"C:\Users\sujit\OneDrive\Documents\TRADING\NIFTY_OPENING_SPIKE\NIFTY_OPENING_SPIKE\data\NIFTY_MARKET_DATA")

# Set these to the actual expiries in the Breeze instrument master.
# Format: DD-MMM-YYYY
OPTION_EXPIRY_DATE = "09-NOV-2026"
FUTURES_EXPIRY_DATE = "23-NOV-2026"

# NIFTY configuration
STRIKE_INTERVAL = 50
STRIKES_EACH_SIDE = 2

# ATM recalculation
ATM_RECALC_SECONDS = 60

# Subscribe new contracts before removing old contracts.
ATM_OVERLAP_SECONDS = 2

# Small delay between subscription commands to avoid bursts.
SUBSCRIPTION_DELAY_SECONDS = 0.20

# Futures subscription
ENABLE_FUTURES = True
ENABLE_FUTURES_DEPTH = True

# Spot index depth is not available in the same way as NFO depth.
ENABLE_SPOT_DEPTH = False

# Queue and writer configuration
QUEUE_MAX_SIZE = 200000
WRITER_BATCH_SIZE = 100

# Wait for initial spot price after connecting.
INITIAL_SPOT_WAIT_SECONDS = 45

IST = ZoneInfo("Asia/Kolkata")


# ============================================================
# 2. LOGGING
# ============================================================

(DATA_ROOT / "logs").mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(
            DATA_ROOT / "logs" / "collector.log",
            encoding="utf-8"
        ),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("NIFTY_COLLECTOR")


# ============================================================
# 3. SHARED STATE
# ============================================================

event_queue = queue.Queue(maxsize=QUEUE_MAX_SIZE)

state_lock = threading.Lock()

latest_spot_price = None
latest_spot_received_at = None

dropped_events = 0
received_events = 0

running = True

# Active option subscriptions:
# {(strike, right): subscription_parameters}
active_options = {}


# ============================================================
# 4. VALIDATE EXPIRY CONFIGURATION
# ============================================================

def validate_expiry(value, label):
    try:
        datetime.strptime(value, "%d-%b-%Y")
    except ValueError:
        raise ValueError(
            "{} must be in DD-MMM-YYYY format. Current value: {}".format(
                label, value
            )
        )


validate_expiry(OPTION_EXPIRY_DATE, "OPTION_EXPIRY_DATE")

if ENABLE_FUTURES:
    validate_expiry(FUTURES_EXPIRY_DATE, "FUTURES_EXPIRY_DATE")


# ============================================================
# 5. EVENT CALLBACK
# ============================================================

def identify_event(payload):
    quote_type = str(payload.get("quotes", "")).lower()

    if "market depth" in quote_type:
        return "depth"

    if "quotes data" in quote_type:
        return "quote"

    return "other"


def on_ticks(ticks):
    """
    Keep this callback lightweight.

    Do not write files or make REST calls here.
    Copy the event into the queue and let the writer
    thread handle disk operations.
    """

    global latest_spot_price
    global latest_spot_received_at
    global dropped_events
    global received_events

    if not isinstance(ticks, list):
        ticks = [ticks]

    for tick in ticks:

        if not isinstance(tick, dict):
            continue

        received_at = datetime.now(IST)

        payload = dict(tick)

        symbol = str(payload.get("symbol", ""))

        # Capture NIFTY 50 spot for ATM calculation.
        is_spot = (
            "NIFTY 50" in symbol.upper()
            or str(payload.get("stock_name", "")).upper() == "NIFTY 50"
        )

        if is_spot:
            try:
                spot = float(payload.get("last"))

                if spot > 0:
                    with state_lock:
                        latest_spot_price = spot
                        latest_spot_received_at = received_at

            except (TypeError, ValueError):
                pass

        event_type = identify_event(payload)

        record = {
            "received_at": received_at.isoformat(),
            "received_epoch": received_at.timestamp(),
            "event_type": event_type,
            "symbol": symbol,
            "payload": payload
        }

        try:
            event_queue.put_nowait(record)

            with state_lock:
                received_events += 1

        except queue.Full:

            with state_lock:
                dropped_events += 1


# ============================================================
# 6. DISK WRITER
# ============================================================

def get_output_path(event_type, received_at):
    date_folder = (
        DATA_ROOT
        / "raw"
        / received_at.strftime("%Y")
        / received_at.strftime("%m")
        / received_at.strftime("%d")
    )

    date_folder.mkdir(parents=True, exist_ok=True)

    filename = "{}_{}.jsonl".format(
        event_type,
        received_at.strftime("%Y-%m-%d")
    )

    return date_folder / filename


def writer_worker():
    """
    One dedicated writer thread.

    Writes every received event to JSONL.
    Quote and depth events are kept in separate files.
    """

    file_handles = {}
    pending_writes = 0
    last_flush = time.time()

    while running or not event_queue.empty():

        try:
            record = event_queue.get(timeout=1)

        except queue.Empty:
            record = None

        if record is not None:

            received_at = datetime.fromisoformat(
                record["received_at"]
            )

            event_type = record["event_type"]

            path = get_output_path(event_type, received_at)
            path_key = str(path)

            if path_key not in file_handles:

                file_handles[path_key] = open(
                    path,
                    "a",
                    encoding="utf-8",
                    buffering=1024 * 1024
                )

            handle = file_handles[path_key]

            handle.write(
                json.dumps(record, separators=(",", ":"), default=str)
                + "\n"
            )

            pending_writes += 1

        # Batch flushing
        if (
            pending_writes >= WRITER_BATCH_SIZE
            or time.time() - last_flush >= 1
        ):

            for handle in file_handles.values():
                handle.flush()

            pending_writes = 0
            last_flush = time.time()

        if record is not None:
            event_queue.task_done()

    # Final flush and close
    for handle in file_handles.values():
        handle.flush()
        handle.close()

    logger.info("Writer thread stopped.")


# ============================================================
# 7. BREEZE CONNECTION
# ============================================================

breeze = BreezeConnect(api_key=API_KEY)


def connect_breeze():

    logger.info("Generating Breeze session...")

    breeze.generate_session(
        api_secret=API_SECRET,
        session_token=SESSION_TOKEN
    )

    # Register callback before connecting.
    breeze.on_ticks = on_ticks

    logger.info("Connecting WebSocket...")

    breeze.ws_connect()

    logger.info("WebSocket connected.")


# ============================================================
# 8. SUBSCRIPTION HELPERS
# ============================================================

def check_response(response, operation):

    if isinstance(response, dict):

        if response.get("Error"):
            raise RuntimeError(
                "{} failed: {}".format(operation, response["Error"])
            )

        if response.get("Status") not in (None, 200):
            raise RuntimeError(
                "{} returned unexpected response: {}".format(
                    operation, response
                )
            )


def subscribe_instrument(params, description):

    response = breeze.subscribe_feeds(**params)

    check_response(response, "Subscribe " + description)

    logger.info(
        "SUBSCRIBED: %s | Response: %s",
        description,
        response
    )

    time.sleep(SUBSCRIPTION_DELAY_SECONDS)

    return response


def unsubscribe_instrument(params, description):

    response = breeze.unsubscribe_feeds(**params)

    check_response(response, "Unsubscribe " + description)

    logger.info(
        "UNSUBSCRIBED: %s | Response: %s",
        description,
        response
    )

    time.sleep(SUBSCRIPTION_DELAY_SECONDS)

    return response


# ============================================================
# 9. NIFTY SPOT SUBSCRIPTION
# ============================================================

def subscribe_spot():

    params = {
        "exchange_code": "NSE",
        "stock_code": "NIFTY",
        "product_type": "cash",
        "get_exchange_quotes": True,
        "get_market_depth": ENABLE_SPOT_DEPTH
    }

    subscribe_instrument(params, "NIFTY 50 SPOT")


# ============================================================
# 10. NIFTY FUTURES SUBSCRIPTION
# ============================================================

def subscribe_futures():

    if not ENABLE_FUTURES:
        return

    params = {
        "exchange_code": "NFO",
        "stock_code": "NIFTY",
        "product_type": "futures",
        "expiry_date": FUTURES_EXPIRY_DATE,
        "strike_price": "0",
        "right": "others",
        "get_exchange_quotes": True,
        "get_market_depth": ENABLE_FUTURES_DEPTH
    }

    try:
        subscribe_instrument(params, "NIFTY FUTURES")

    except Exception as exc:
        logger.exception(
            "NIFTY futures subscription failed. "
            "Spot and options collection will continue. Error: %s",
            exc
        )


# ============================================================
# 11. ATM CALCULATION
# ============================================================

def calculate_atm(spot_price):

    # Avoid Python's banker's rounding.
    return int(
        math.floor(
            spot_price / STRIKE_INTERVAL + 0.5
        ) * STRIKE_INTERVAL
    )


def generate_option_universe(atm):

    strikes = [
        atm + offset * STRIKE_INTERVAL
        for offset in range(
            -STRIKES_EACH_SIDE,
            STRIKES_EACH_SIDE + 1
        )
    ]

    instruments = []

    for strike in strikes:

        for right in ["call", "put"]:

            instruments.append({
                "strike": strike,
                "right": right
            })

    return instruments


# ============================================================
# 12. OPTION SUBSCRIPTION PARAMETERS
# ============================================================

def make_option_params(strike, right):

    return {
        "exchange_code": "NFO",
        "stock_code": "NIFTY",
        "product_type": "options",
        "expiry_date": OPTION_EXPIRY_DATE,
        "strike_price": str(strike),
        "right": right,
        "get_exchange_quotes": True,
        "get_market_depth": True
    }


# ============================================================
# 13. SUBSCRIBE TO OPTION UNIVERSE
# ============================================================

def subscribe_option_universe(atm):

    global active_options

    instruments = generate_option_universe(atm)

    newly_subscribed = {}

    logger.info(
        "Subscribing option universe | ATM: %s | Contracts: %s",
        atm,
        len(instruments)
    )

    for instrument in instruments:

        strike = instrument["strike"]
        right = instrument["right"]

        key = (strike, right)

        if key in active_options:
            continue

        params = make_option_params(strike, right)

        description = "NIFTY {} {}".format(
            strike,
            right.upper()
        )

        try:

            subscribe_instrument(params, description)

            newly_subscribed[key] = params

        except Exception as exc:

            logger.exception(
                "Failed to subscribe %s: %s",
                description,
                exc
            )

    active_options.update(newly_subscribed)

    logger.info(
        "Active option contracts: %s",
        len(active_options)
    )


# ============================================================
# 14. ATM ROLLOVER
# ============================================================

def rollover_option_universe(new_atm):

    global active_options

    old_options = dict(active_options)

    logger.info(
        "ATM CHANGE DETECTED | New ATM: %s",
        new_atm
    )

    new_universe = generate_option_universe(new_atm)

    new_keys = {
        (item["strike"], item["right"])
        for item in new_universe
    }

    # First subscribe to all new contracts.
    subscribe_option_universe(new_atm)

    # Retain old contracts briefly to avoid a data gap.
    time.sleep(ATM_OVERLAP_SECONDS)

    # Unsubscribe only contracts no longer required.
    for key, params in old_options.items():

        if key in new_keys:
            continue

        strike, right = key

        description = "NIFTY {} {}".format(
            strike,
            right.upper()
        )

        try:

            unsubscribe_instrument(params, description)

            active_options.pop(key, None)

        except Exception as exc:

            logger.exception(
                "Unsubscribe failed for %s: %s",
                description,
                exc
            )

    logger.info(
        "ATM rollover completed | ATM: %s | Active contracts: %s",
        new_atm,
        len(active_options)
    )


# ============================================================
# 15. WAIT FOR INITIAL SPOT PRICE
# ============================================================

def wait_for_initial_spot():

    logger.info("Waiting for initial NIFTY spot tick...")

    start = time.time()

    while time.time() - start < INITIAL_SPOT_WAIT_SECONDS:

        with state_lock:
            spot = latest_spot_price

        if spot is not None:
            logger.info("Initial NIFTY spot: %s", spot)
            return spot

        time.sleep(0.5)

    raise TimeoutError(
        "No NIFTY spot price received. "
        "Check WebSocket connection and spot subscription."
    )


# ============================================================
# 16. MAIN ATM MONITOR
# ============================================================

def monitor_atm():

    current_atm = None
    last_recalculation = 0
    last_status_log = 0

    logger.info("Starting ATM monitoring.")

    while running:

        now = time.time()

        if now - last_recalculation >= ATM_RECALC_SECONDS:

            with state_lock:
                spot = latest_spot_price
                spot_time = latest_spot_received_at

            last_recalculation = now

            if spot is not None:

                new_atm = calculate_atm(spot)

                if current_atm is None:

                    current_atm = new_atm

                    logger.info(
                        "Initial ATM calculated: %s",
                        current_atm
                    )

                    subscribe_option_universe(current_atm)

                elif new_atm != current_atm:

                    logger.info(
                        "Spot: %s | Previous ATM: %s | New ATM: %s",
                        spot,
                        current_atm,
                        new_atm
                    )

                    rollover_option_universe(new_atm)

                    current_atm = new_atm

                else:

                    logger.info(
                        "ATM unchanged | Spot: %s | ATM: %s",
                        spot,
                        current_atm
                    )

                if spot_time is not None:

                    age = (
                        datetime.now(IST) - spot_time
                    ).total_seconds()

                    if age > 15:
                        logger.warning(
                            "Latest spot update is %.1f seconds old.",
                            age
                        )

        # Operational status
        if now - last_status_log >= 60:

            with state_lock:
                total_received = received_events
                total_dropped = dropped_events

            logger.info(
                "STATUS | Queue: %s | Received: %s | Dropped: %s | "
                "Active options: %s | ATM: %s",
                event_queue.qsize(),
                total_received,
                total_dropped,
                len(active_options),
                current_atm
            )

            last_status_log = now

        time.sleep(0.5)


# ============================================================
# 17. MAIN
# ============================================================

def main():

    global running

    DATA_ROOT.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("NIFTY LIVE DATA COLLECTOR STARTING")
    logger.info("Data directory: %s", DATA_ROOT)
    logger.info("Option expiry: %s", OPTION_EXPIRY_DATE)
    logger.info("Futures expiry: %s", FUTURES_EXPIRY_DATE)
    logger.info("ATM recalculation: %s seconds", ATM_RECALC_SECONDS)
    logger.info("Strike interval: %s", STRIKE_INTERVAL)
    logger.info("Contracts around ATM: +/- %s", STRIKES_EACH_SIDE)
    logger.info("=" * 70)

    writer = threading.Thread(
        target=writer_worker,
        name="JSONL-Writer",
        daemon=False
    )

    writer.start()

    try:

        connect_breeze()

        # Subscribe spot first.
        subscribe_spot()

        # Subscribe near-month futures.
        subscribe_futures()

        # Wait until spot is received.
        wait_for_initial_spot()

        # Start option collection and ATM monitoring.
        monitor_atm()

    except KeyboardInterrupt:

        logger.info("Keyboard interrupt received.")

    except Exception as exc:

        logger.exception("Collector stopped due to error: %s", exc)

    finally:

        running = False

        logger.info("Stopping collector...")

        try:
            breeze.ws_disconnect()
            logger.info("WebSocket disconnected.")

        except Exception:
            logger.exception("Error disconnecting WebSocket.")

        # Allow writer to finish queued events.
        writer.join(timeout=60)

        with state_lock:
            logger.info(
                "FINAL COUNTS | Received: %s | Dropped: %s",
                received_events,
                dropped_events
            )

        logger.info("Collector stopped.")


if __name__ == "__main__":
    main()
