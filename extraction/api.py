import time
from project_config.settings import MAX_RETRIES, RETRY_BASE_SECONDS, REQUEST_SLEEP_SECONDS
from extraction.calendar_utils import breeze_timestamp

def call_historical_v2(breeze, params):
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            response = breeze.get_historical_data_v2(**params)
            if not isinstance(response, dict):
                raise RuntimeError("Unexpected Breeze response type: %r" % type(response))
            error = response.get("Error")
            status = response.get("Status")
            success = response.get("Success")
            if error not in (None, "", "None"):
                raise RuntimeError("Breeze API Error=%s Status=%s" % (error, status))
            if success is None:
                raise RuntimeError("Breeze response missing Success: %s" % response)
            time.sleep(REQUEST_SLEEP_SECONDS)
            return success
        except Exception as exc:
            last_error = exc
            if attempt + 1 >= MAX_RETRIES:
                break
            time.sleep(RETRY_BASE_SECONDS * (attempt + 1))
    raise RuntimeError("Breeze request failed after retries: %s" % last_error)

def normalize_rows(rows, instrument_meta):
    normalized = []
    for row in rows or []:
        item = dict(row)
        item.update(instrument_meta)
        normalized.append(item)
    return normalized

def request_chunk(breeze, base_params, start_dt, end_dt):
    params = dict(base_params)
    params["from_date"] = breeze_timestamp(start_dt)
    params["to_date"] = breeze_timestamp(end_dt)
    return call_historical_v2(breeze, params)
