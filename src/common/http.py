import time
import requests

def get_json(url: str, params: dict, headers: dict | None = None, timeout: int = 30) -> dict:
    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    if resp.status_code == 429:
        time.sleep(10)
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    # be polite to free APIs
    time.sleep(1.0)
    return resp.json()
