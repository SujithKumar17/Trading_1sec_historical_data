
import os
import json
import time
import math
import queue
import logging
import threading

from pathlib import Path
from datetime import datetime
from collections import defaultdict
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from breeze_connect import BreezeConnect


# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

# Load .env from script directory, with project-root fallback.
load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR.parent / ".env")

API_KEY = os.getenv("ICICI_BREEZE_API_KEY")
API_SECRET = os.getenv("ICICI_BREEZE_SECRET_KEY")
SESSION_TOKEN = os.getenv("ICICI_BREEZE_SESSION_TOKEN")

if not all([API_KEY, API_SECRET, SESSION_TOKEN]):
    raise ValueError(
        "Missing credentials. Check ICICI_BREEZE_API_KEY, "
        "ICICI_BREEZE_SECRET_KEY and ICICI_BREEZE_SESSION_TOKEN."
    )


# ------------------------------------------------------------
# DATA DIRECTORY
# ------------------------------------------------------------

# Your specified project structure:
# NIFTY_OPENING_SPIKE/
#   extraction/
#   data/
#       NIFTY_MARKET_DATA/

if BASE_DIR.name.lower() == "extraction":
    PROJECT_DIR = BASE_DIR.parent
else:
    PROJECT_DIR = BASE_DIR

DATA_ROOT = PROJECT_DIR / "data" / "NIFTY_MARKET_DATA"


# ------------------------------------------------------------
# EXPIRY CONFIGURATION
# ------------------------------------------------------------

# IMPORTANT:
# Enter the actual expiry dates from the current Breeze
# instrument master. Do not use expired or guessed dates.

OPTION_EXPIRY_DATE = os.getenv(
    "NIFTY_OPTION_EXPIRY",
    "REPLACE-WITH-OPTION-EXPIRY"
)

FUTURES_EXPIRY_DATE = os.getenv(
    "NIFTY_FUTURES_EXPIRY",
    "REPLACE-WITH-FUTURES-EXPIRY"
)


# ------------------------------------------------------------
# INSTRUMENT CONFIGURATION
# ------------------------------------------------------------

STRIKE_INTERVAL = 50

# ATM, ATM-1, ATM-2, ATM+1, ATM+2
STRIKES_EACH_SIDE = 2

# Recalculate ATM every 60 seconds.
ATM_RECALC_SECONDS = 60

# Keep old subscriptions temporarily during rollover.
ATM_OVERLAP_SECONDS = 2

# Delay between individual Breeze subscription calls.
SUBSCRIPTION_DELAY_SECONDS = 0.20

# Include NIFTY futures.
ENABLE_FUTURES = True

# Futures depth is attempted separately.
ENABLE_FUTURES_DEPTH = True

# Index depth is not expected for NIFTY 50.
ENABLE_SPOT_DEPTH = False


# ------------------------------------------------------------
# WRITER CONFIGURATION
# ------------------------------------------------------------

QUEUE_MAX_SIZE = 200000

WRITER_BATCH_SIZE = 100

WRITER_FLUSH_SECONDS = 1

INITIAL_SPOT_WAIT_SECONDS = 45

# Alert if no update arrives for this many seconds.
STALE_FEED_SECONDS = 15


IST = ZoneInfo("Asia/Kolkata")


# ============================================================
# 2. VALIDATE CONFIGURATION
# ============================================================

def validate_expiry(value, label):

    try:
        datetime.strptime(value, "%d-%b-%Y")

    except ValueError:
        raise ValueError(
            "{} must be DD-MMM-YYYY. Current value: {}".format(
                label, value
            )
        )


validate_expiry(
    OPTION_EXPIRY_DATE,
    "OPTION_EXPIRY_DATE"
)

if ENABLE_FUTURES:
    validate_expiry(
        FUTURES_EXPIRY_DATE,
        "FUTURES_EXPIRY_DATE"
    )


# ============================================================
# 3. LOGGING
# ============================================================

(DATA_ROOT / "logs").mkdir(
    parents=True,
    exist_ok=True
)

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
# 4. SHARED STATE
# ============================================================

event_queue = queue.Queue(
    maxsize=QUEUE_MAX_SIZE
)

state_lock = threading.Lock()

stop_event = threading.Event()

latest_spot_price = None
latest_spot_received_at = None

received_events = 0
dropped_events = 0

event_counts = defaultdict(int)
last_event_time = {}

# Active option contracts:
# {(strike, right): {"quote": params, "depth": params}}

active_options = {}

# Futures subscription parameters.
active_futures = {}


# ============================================================
# 5. BREEZE INSTANCE
# ============================================================

breeze = BreezeConnect(
    api_key=API_KEY
)


# ============================================================
# 6. EVENT IDENTIFICATION
# ============================================================

def identify_event(payload):

    symbol = str(
        payload.get("symbol", "")
    )

    quote_type = str(
        payload.get("quotes", "")
    ).lower()

    if (
        "market depth" in quote_type
        or ".2!" in symbol
        or isinstance(payload.get("depth"), list)
    ):
        return "depth"

    if (
        "quotes data" in quote_type
        or ".1!" in symbol
    ):
        return "quote"

    return "other"


def is_nifty_spot(payload):

    symbol = str(
        payload.get("symbol", "")
    ).upper()

    exchange = str(
        payload.get("exchange", "")
    ).upper()

    stock_name = str(
        payload.get("stock_name", "")
    ).upper()

    # Do not mistake NIFTY options for the spot index.
    is_spot_symbol = (
        symbol == "4.1!NIFTY 50"
        or (
            stock_name == "NIFTY 50"
            and exchange == "NSE EQUITY"
        )
    )

    return is_spot_symbol


# ============================================================
# 7. WEBSOCKET CALLBACK
# ============================================================

def on_ticks(ticks):

    global latest_spot_price
    global latest_spot_received_at
    global received_events
    global dropped_events

    if isinstance(ticks, dict):
        ticks = [ticks]

    if not isinstance(ticks, list):
        logger.warning(
            "Unexpected WebSocket payload type: %s",
            type(ticks)
        )
        return

    for tick in ticks:

        if not isinstance(tick, dict):
            continue

        received_at = datetime.now(IST)

        payload = dict(tick)

        symbol = str(
            payload.get("symbol", "UNKNOWN")
        )

        event_type = identify_event(payload)

        # Update latest NIFTY spot.
        if is_nifty_spot(payload):

            try:

                price = float(
                    payload.get("last")
                )

                if price > 0:

                    with state_lock:
                        latest_spot_price = price
                        latest_spot_received_at = received_at

            except (TypeError, ValueError):
                pass

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

                event_counts[
                    (symbol, event_type)
                ] += 1

                last_event_time[
                    (symbol, event_type)
                ] = received_at

        except queue.Full:

            with state_lock:
                dropped_events += 1


# ============================================================
# 8. DISK WRITER
# ============================================================

def get_output_path(event_type, received_at):

    date_folder = (
        DATA_ROOT
        / "raw"
        / received_at.strftime("%Y")
        / received_at.strftime("%m")
        / received_at.strftime("%d")
    )

    date_folder.mkdir(
        parents=True,
        exist_ok=True
    )

    filename = "{}_{}.jsonl".format(
        event_type,
        received_at.strftime("%Y-%m-%d")
    )

    return date_folder / filename


def writer_worker():

    file_handles = {}

    pending_writes = 0

    last_flush = time.time()

    logger.info("Disk writer started.")

    while (
        not stop_event.is_set()
        or not event_queue.empty()
    ):

        try:

            record = event_queue.get(
                timeout=1
            )

        except queue.Empty:

            record = None

        if record is not None:

            try:

                received_at = datetime.fromisoformat(
                    record["received_at"]
                )

                event_type = record["event_type"]

                path = get_output_path(
                    event_type,
                    received_at
                )

                path_key = str(path)

                if path_key not in file_handles:

                    file_handles[path_key] = open(
                        path,
                        "a",
                        encoding="utf-8",
                        buffering=1024 * 1024
                    )

                file_handles[path_key].write(
                    json.dumps(
                        record,
                        separators=(",", ":"),
                        default=str
                    ) + "\n"
                )

                pending_writes += 1

            except Exception:

                logger.exception(
                    "Writer failed to write event."
                )

            finally:

                event_queue.task_done()

        # Flush in batches.
        if (
            pending_writes >= WRITER_BATCH_SIZE
            or time.time() - last_flush >= WRITER_FLUSH_SECONDS
        ):

            for handle in file_handles.values():

                try:
                    handle.flush()

                except Exception:
                    logger.exception("File flush failed.")

            pending_writes = 0
            last_flush = time.time()

    # Final flush and close.
    for handle in file_handles.values():

        try:
            handle.flush()
            handle.close()

        except Exception:
            logger.exception("Final file close failed.")

    logger.info("Disk writer stopped.")


# ============================================================
# 9. BREEZE RESPONSE VALIDATION
# ============================================================

def check_response(response, operation):

    # Some SDK failures are returned as strings.
    if isinstance(response, str):

        raise RuntimeError(
            "{} FAILED: {}".format(
                operation,
                response
            )
        )

    if isinstance(response, Exception):

        raise RuntimeError(
            "{} FAILED: {}".format(
                operation,
                str(response)
            )
        )

    if not isinstance(response, dict):

        raise RuntimeError(
            "{} returned unexpected response: {!r}".format(
                operation,
                response
            )
        )

    if response.get("Error"):

        raise RuntimeError(
            "{} FAILED: {}".format(
                operation,
                response["Error"]
            )
        )

    message = str(
        response.get("message", "")
    )

    if "error" in message.lower():
        raise RuntimeError(
            "{} FAILED: {}".format(
                operation,
                message
            )
        )

    logger.info(
        "%s RESPONSE: %s",
        operation,
        response
    )

    return response


# ============================================================
# 10. SUBSCRIBE / UNSUBSCRIBE HELPERS
# ============================================================


def subscribe_feed(params, description):

    logger.info(
        "SUBSCRIPTION REQUEST: %s | %s",
        description,
        params
    )

    # --------------------------------------------------------
    # CRITICAL: Validate NFO instrument token before subscribing.
    # Breeze SDK may return "subscribed successfully" even when
    # its instrument-master lookup has failed.
    # --------------------------------------------------------

    if params.get("exchange_code") == "NFO":

        token_result = breeze.get_stock_token_value(
            exchange_code=params["exchange_code"],
            stock_code=params["stock_code"],
            product_type=params["product_type"],
            expiry_date=params["expiry_date"],
            strike_price=params.get("strike_price", ""),
            right=params.get("right", ""),
            get_exchange_quotes=params["get_exchange_quotes"],
            get_market_depth=params["get_market_depth"]
        )

        if isinstance(token_result, Exception):
            raise RuntimeError(
                "TOKEN RESOLUTION FAILED for {}: {}".format(
                    description,
                    token_result
                )
            )

        if not isinstance(token_result, tuple) or len(token_result) != 2:
            raise RuntimeError(
                "Unexpected token result for {}: {!r}".format(
                    description,
                    token_result
                )
            )

        quote_token, depth_token = token_result

        if params["get_exchange_quotes"]:

            if (
                not quote_token
                or "False" in str(quote_token)
            ):
                raise RuntimeError(
                    "INVALID QUOTE TOKEN for {}: {!r}".format(
                        description,
                        quote_token
                    )
                )

        if params["get_market_depth"]:

            if (
                not depth_token
                or "False" in str(depth_token)
            ):
                raise RuntimeError(
                    "INVALID DEPTH TOKEN for {}: {!r}".format(
                        description,
                        depth_token
                    )
                )

        logger.info(
            "VALIDATED TOKENS | %s | QUOTE=%s | DEPTH=%s",
            description,
            quote_token,
            depth_token
        )

    # --------------------------------------------------------
    # Subscribe only after successful token validation.
    # --------------------------------------------------------

    response = breeze.subscribe_feeds(**params)

    check_response(
        response,
        "SUBSCRIBE " + description
    )

    logger.info(
        "SUBSCRIPTION REQUEST ACCEPTED: %s",
        description
    )

    time.sleep(
        SUBSCRIPTION_DELAY_SECONDS
    )

    return response


def unsubscribe_feed(params, description):

    logger.info(
        "UNSUBSCRIPTION REQUEST: %s",
        description
    )

    response = breeze.unsubscribe_feeds(
        **params
    )

    logger.info(
        "UNSUBSCRIPTION RESPONSE: %s | %s",
        description,
        response
    )

    time.sleep(
        SUBSCRIPTION_DELAY_SECONDS
    )

    return response


# ============================================================
# 11. CONNECT WEBSOCKET
# ============================================================

def connect_breeze():

    logger.info("Generating Breeze session.")

    breeze.generate_session(
        api_secret=API_SECRET,
        session_token=SESSION_TOKEN
    )

    # Register callback before connecting.
    breeze.on_ticks = on_ticks

    logger.info("Connecting Breeze WebSocket.")

    breeze.ws_connect()

    logger.info("Breeze WebSocket connection initiated.")


# ============================================================
# 12. NIFTY SPOT SUBSCRIPTION
# ============================================================

def subscribe_spot():

    params = {
        "exchange_code": "NSE",
        "stock_code": "NIFTY",
        "product_type": "cash",
        "get_exchange_quotes": True,
        "get_market_depth": False
    }

    subscribe_feed(
        params,
        "NIFTY 50 SPOT QUOTE"
    )


# ============================================================
# 13. NIFTY FUTURES SUBSCRIPTION
# ============================================================

def subscribe_futures():

    global active_futures

    if not ENABLE_FUTURES:
        return

    # Breeze NFO futures subscription.
    base_params = {
        "exchange_code": "NFO",
        "stock_code": "NIFTY",
        "product_type": "futures",
        "expiry_date": FUTURES_EXPIRY_DATE,
        "strike_price": "0",
        "right": "others"
    }

    quote_params = dict(base_params)

    quote_params.update({
        "get_exchange_quotes": True,
        "get_market_depth": False
    })

    try:

        subscribe_feed(
            quote_params,
            "NIFTY FUTURES QUOTE"
        )

        active_futures["quote"] = quote_params

    except Exception:

        logger.exception(
            "NIFTY FUTURES QUOTE subscription failed."
        )

    if ENABLE_FUTURES_DEPTH:

        depth_params = dict(base_params)

        depth_params.update({
            "get_exchange_quotes": False,
            "get_market_depth": True
        })

        try:

            subscribe_feed(
                depth_params,
                "NIFTY FUTURES DEPTH"
            )

            active_futures["depth"] = depth_params

        except Exception:

            logger.exception(
                "NIFTY FUTURES DEPTH subscription failed."
            )


# ============================================================
# 14. ATM CALCULATION
# ============================================================

def calculate_atm(spot_price):

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

        instruments.append({
            "strike": strike,
            "right": "Call"
        })

        instruments.append({
            "strike": strike,
            "right": "Put"
        })

    return instruments


# ============================================================
# 15. OPTION SUBSCRIPTION PARAMETERS
# ============================================================

def make_option_params(
    strike,
    right,
    event_type
):

    if event_type == "quote":

        get_quotes = True
        get_depth = False

    elif event_type == "depth":

        get_quotes = False
        get_depth = True

    else:

        raise ValueError(
            "Unknown event type: {}".format(event_type)
        )

    return {
        "exchange_code": "NFO",
        "stock_code": "NIFTY",
        "product_type": "options",
        "expiry_date": OPTION_EXPIRY_DATE,
        "strike_price": str(strike),
        "right": right,
        "get_exchange_quotes": get_quotes,
        "get_market_depth": get_depth
    }


# ============================================================
# 16. SUBSCRIBE ONE OPTION CONTRACT
# ============================================================

def subscribe_option_contract(
    strike,
    right
):

    key = (
        strike,
        right
    )

    if key in active_options:
        return True

    logger.info(
        "SUBSCRIBING OPTION CONTRACT: %s %s",
        strike,
        right
    )

    contract_status = {}

    # Subscribe quotes first.
    quote_params = make_option_params(
        strike,
        right,
        "quote"
    )

    description = "NIFTY {} {} QUOTE".format(
        strike,
        right.upper()
    )

    try:

        subscribe_feed(
            quote_params,
            description
        )

        contract_status["quote"] = quote_params

    except Exception:

        logger.exception(
            "QUOTE subscription failed: %s",
            description
        )

    # Subscribe depth separately.
    depth_params = make_option_params(
        strike,
        right,
        "depth"
    )

    description = "NIFTY {} {} DEPTH".format(
        strike,
        right.upper()
    )

    try:

        subscribe_feed(
            depth_params,
            description
        )

        contract_status["depth"] = depth_params

    except Exception:

        logger.exception(
            "DEPTH subscription failed: %s",
            description
        )

    if contract_status:

        active_options[key] = contract_status

    # A contract is considered complete only if both
    # subscriptions have been accepted.
    return (
        "quote" in contract_status
        and "depth" in contract_status
    )


# ============================================================
# 17. SUBSCRIBE OPTION UNIVERSE
# ============================================================

def subscribe_option_universe(atm):

    instruments = generate_option_universe(atm)

    successful = 0
    incomplete = 0

    logger.info(
        "OPTION UNIVERSE | ATM: %s | Contracts: %s",
        atm,
        len(instruments)
    )

    for instrument in instruments:

        strike = instrument["strike"]
        right = instrument["right"]

        complete = subscribe_option_contract(
            strike,
            right
        )

        if complete:
            successful += 1

        else:
            incomplete += 1

    logger.info(
        "OPTION SUBSCRIPTION RESULT | "
        "Complete: %s | Incomplete: %s | Active: %s",
        successful,
        incomplete,
        len(active_options)
    )

    return successful, incomplete


# ============================================================
# 18. UNSUBSCRIBE OLD OPTIONS
# ============================================================

def unsubscribe_old_options(new_atm):

    global active_options

    desired_keys = {
        (
            instrument["strike"],
            instrument["right"]
        )
        for instrument in generate_option_universe(new_atm)
    }

    # Only remove old subscriptions when all new contracts
    # are subscribed successfully.
    all_new_contracts_ready = all(
        key in active_options
        and "quote" in active_options[key]
        and "depth" in active_options[key]
        for key in desired_keys
    )

    if not all_new_contracts_ready:

        logger.warning(
            "New option universe is incomplete. "
            "Keeping old subscriptions until retries succeed."
        )

        return

    for key in list(active_options.keys()):

        if key in desired_keys:
            continue

        strike, right = key

        contract = active_options[key]

        for event_type in ["quote", "depth"]:

            params = contract.get(event_type)

            if not params:
                continue

            description = "NIFTY {} {} {}".format(
                strike,
                right.upper(),
                event_type.upper()
            )

            try:

                unsubscribe_feed(
                    params,
                    description
                )

            except Exception:

                logger.exception(
                    "Unsubscription failed: %s",
                    description
                )

        active_options.pop(key, None)


# ============================================================
# 19. INITIAL SPOT WAIT
# ============================================================

def wait_for_initial_spot():

    logger.info(
        "Waiting for first NIFTY spot update..."
    )

    start = time.time()

    while (
        time.time() - start
        < INITIAL_SPOT_WAIT_SECONDS
    ):

        with state_lock:
            spot = latest_spot_price

        if spot is not None:

            logger.info(
                "INITIAL NIFTY SPOT: %s",
                spot
            )

            return spot

        time.sleep(0.5)

    raise TimeoutError(
        "NIFTY spot feed did not arrive. "
        "Check the WebSocket and spot subscription."
    )


# ============================================================
# 20. ATM MONITOR
# ============================================================

def monitor_atm():

    current_atm = None

    last_recalculation = 0

    last_status_log = 0

    logger.info(
        "ATM MONITOR STARTED."
    )

    while not stop_event.is_set():

        now = time.time()

        if (
            now - last_recalculation
            >= ATM_RECALC_SECONDS
        ):

            last_recalculation = now

            with state_lock:

                spot = latest_spot_price
                spot_time = latest_spot_received_at

            if spot is None:
                logger.warning(
                    "No spot price available for ATM calculation."
                )

            else:

                spot_age = (
                    datetime.now(IST) - spot_time
                ).total_seconds()

                # Do not change ATM using stale spot data.
                if spot_age > STALE_FEED_SECONDS:

                    logger.warning(
                        "ATM calculation skipped. "
                        "Spot feed is stale by %.1f seconds.",
                        spot_age
                    )

                else:

                    new_atm = calculate_atm(spot)

                    if current_atm is None:

                        logger.info(
                            "INITIAL ATM: %s | SPOT: %s",
                            new_atm,
                            spot
                        )

                        current_atm = new_atm

                        subscribe_option_universe(
                            current_atm
                        )

                    elif new_atm != current_atm:

                        logger.info(
                            "ATM CHANGE | "
                            "OLD: %s | NEW: %s | SPOT: %s",
                            current_atm,
                            new_atm,
                            spot
                        )

                        subscribe_option_universe(
                            new_atm
                        )

                        time.sleep(
                            ATM_OVERLAP_SECONDS
                        )

                        unsubscribe_old_options(
                            new_atm
                        )

                        current_atm = new_atm

                    else:

                        # Also retries incomplete contracts.
                        subscribe_option_universe(
                            current_atm
                        )

                        logger.info(
                            "ATM UNCHANGED | ATM: %s | SPOT: %s",
                            current_atm,
                            spot
                        )

        # Periodic monitoring.
        if now - last_status_log >= 60:

            with state_lock:

                total_received = received_events
                total_dropped = dropped_events

                counts = dict(event_counts)

                last_times = dict(last_event_time)

            logger.info("=" * 65)

            logger.info(
                "HEALTH | Queue: %s | Received: %s | Dropped: %s",
                event_queue.qsize(),
                total_received,
                total_dropped
            )

            logger.info(
                "ACTIVE OPTION CONTRACTS: %s",
                len(active_options)
            )

            logger.info(
                "ACTIVE FUTURES FEEDS: %s",
                list(active_futures.keys())
            )

            logger.info(
                "CURRENT ATM: %s",
                current_atm
            )

            logger.info(
                "EVENT COUNTS: %s",
                counts
            )

            # Feed staleness by symbol and event type.
            current_time = datetime.now(IST)

            for key, event_time in last_times.items():

                age = (
                    current_time - event_time
                ).total_seconds()

                if age > STALE_FEED_SECONDS:

                    logger.warning(
                        "STALE FEED | %s | %s | %.1f seconds",
                        key[0],
                        key[1],
                        age
                    )

            logger.info("=" * 65)

            last_status_log = now

        stop_event.wait(0.5)


# ============================================================
# 21. MAIN
# ============================================================

def main():

    logger.info("=" * 70)

    logger.info(
        "NIFTY LIVE MARKET DATA COLLECTOR STARTING"
    )

    logger.info(
        "DATA ROOT: %s",
        DATA_ROOT
    )

    logger.info(
        "OPTION EXPIRY: %s",
        OPTION_EXPIRY_DATE
    )

    logger.info(
        "FUTURES EXPIRY: %s",
        FUTURES_EXPIRY_DATE
    )

    logger.info(
        "ATM RECALCULATION: %s seconds",
        ATM_RECALC_SECONDS
    )

    logger.info(
        "OPTION STRIKES: ATM +/- %s",
        STRIKES_EACH_SIDE
    )

    logger.info("=" * 70)

    DATA_ROOT.mkdir(
        parents=True,
        exist_ok=True
    )

    writer = threading.Thread(
        target=writer_worker,
        name="JSONL-WRITER",
        daemon=False
    )

    writer.start()

    try:

        # Connect.
        connect_breeze()

        # Subscribe spot first.
        subscribe_spot()

        # Subscribe futures.
        subscribe_futures()

        # Wait for spot.
        wait_for_initial_spot()

        # Start option subscriptions and ATM monitoring.
        monitor_atm()

    except KeyboardInterrupt:

        logger.info(
            "Keyboard interrupt received."
        )

    except Exception:

        logger.exception(
            "COLLECTOR STOPPED DUE TO ERROR."
        )

    finally:

        logger.info(
            "Stopping collector..."
        )

        stop_event.set()

        try:

            breeze.ws_disconnect()

            logger.info(
                "Breeze WebSocket disconnected."
            )

        except Exception:

            logger.exception(
                "WebSocket disconnect failed."
            )

        # Let writer finish all queued events.
        writer.join()

        with state_lock:

            logger.info(
                "FINAL RECEIVED EVENTS: %s",
                received_events
            )

            logger.info(
                "FINAL DROPPED EVENTS: %s",
                dropped_events
            )

        logger.info(
            "COLLECTOR STOPPED."
        )


if __name__ == "__main__":
    main()
