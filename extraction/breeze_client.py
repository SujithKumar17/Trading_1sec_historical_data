import os
from dotenv import load_dotenv
from breeze_connect import BreezeConnect
from project_config.settings import ENV_FILE

def connect_breeze():
    load_dotenv(str(ENV_FILE))
    api_key = os.getenv("ICICI_BREEZE_API_KEY")
    api_secret = os.getenv("ICICI_BREEZE_SECRET_KEY")
    session_token = os.getenv("ICICI_BREEZE_SESSION_TOKEN")

    # Backward-compatible fallback for an older .env naming convention.
    api_key = api_key or os.getenv("BREEZE_API_KEY")
    api_secret = api_secret or os.getenv("BREEZE_API_SECRET")
    session_token = session_token or os.getenv("BREEZE_SESSION_TOKEN")

    missing = []
    if not api_key:
        missing.append("ICICI_BREEZE_API_KEY")
    if not api_secret:
        missing.append("ICICI_BREEZE_SECRET_KEY")
    if not session_token:
        missing.append("ICICI_BREEZE_SESSION_TOKEN")
    if missing:
        raise RuntimeError(
            "Missing Breeze credentials in .env: " + ", ".join(missing)
        )

    breeze = BreezeConnect(api_key=api_key)
    breeze.generate_session(
        api_secret=api_secret,
        session_token=session_token
    )
    return breeze
