from datetime import date
from src.common.config import AWS_REGION, S3_BUCKET
from src.extract.alphavantage_daily import fetch_daily
from src.load.s3_uploader import put_json

SYMBOLS = ["AAPL", "MSFT", "TSLA", "NVDA"]

def main():
    dt = date.today().isoformat()

    for sym in SYMBOLS:
        payload = fetch_daily(sym)

        # Basic API error detection (Alpha Vantage returns keys like "Note" / "Error Message")
        if "Error Message" in payload:
            raise RuntimeError(f"Alpha Vantage error for {sym}: {payload['Error Message']}")
        if "Note" in payload:
            raise RuntimeError(f"Rate limit hit (Alpha Vantage) while fetching {sym}: {payload['Note']}")

        key = f"raw/alphavantage/daily/symbol={sym}/dt={dt}/data.json"
        put_json(S3_BUCKET, key, payload, region=AWS_REGION)
        print(f"Uploaded {sym} -> s3://{S3_BUCKET}/{key}")

if __name__ == "__main__":
    main()
