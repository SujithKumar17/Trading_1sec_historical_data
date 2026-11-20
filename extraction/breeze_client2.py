
import os

from pathlib import Path
from dotenv import load_dotenv
from breeze_connect import BreezeConnect

BASE_DIR = Path(__file__).resolve().parents[1]

load_dotenv(BASE_DIR / ".env")


def create_breeze_client():

    API_KEY = os.getenv("ICICI_BREEZE_API_KEY")
    API_SECRET = os.getenv("ICICI_BREEZE_SECRET_KEY")
    SESSION_TOKEN = os.getenv("ICICI_BREEZE_SESSION_TOKEN")

    if not all([API_KEY, API_SECRET, SESSION_TOKEN]):
        raise EnvironmentError(
            "Missing ICICI Breeze credentials in .env"
        )

    breeze = BreezeConnect(api_key=API_KEY)

    breeze.generate_session(
        api_secret=API_SECRET,
        session_token=SESSION_TOKEN
    )

    print("ICICI Breeze authentication successful.")

    return breeze
