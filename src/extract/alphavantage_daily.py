from src.common.config import ALPHAVANTAGE_KEY
from src.common.http import get_json

BASE = "https://www.alphavantage.co/query"

def fetch_daily(symbol: str) -> dict:
    if not ALPHAVANTAGE_KEY:
        raise ValueError("Missing ALPHAVANTAGE_KEY in environment (.env).")

    params = {
        "function": "TIME_SERIES_DAILY",   # FREE endpoint
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": ALPHAVANTAGE_KEY,
    }
    return get_json(BASE, params=params)
