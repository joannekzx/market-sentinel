import argparse
import json
import os
import tempfile
import boto3
import pandas as pd

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
S3_BUCKET = os.getenv("S3_BUCKET", "")

SYMBOLS = ["AAPL", "MSFT", "TSLA", "NVDA"]

def parse_av_daily_adjusted(payload: dict, symbol: str) -> pd.DataFrame:
    ts = payload.get("Time Series (Daily)", {})
    rows = []
    for dt, v in ts.items():
        rows.append({
            "symbol": symbol,
            "date": dt,
            "open": float(v["1. open"]),
            "high": float(v["2. high"]),
            "low": float(v["3. low"]),
            "close": float(v["4. close"]),
            "volume": int(float(v["5. volume"])),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError(f"No rows parsed for {symbol}. Missing Time Series (Daily)? Keys={list(payload.keys())}")
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df.sort_values(["symbol", "date"])

def main():
    parser = argparse.ArgumentParser(description="Curate Alpha Vantage raw JSON to curated CSV in S3.")
    parser.add_argument("--dt", required=True, help="Partition date like 2026-01-29")
    args = parser.parse_args()

    dt = args.dt
    print(f"[INFO] curate_prices_csv started dt={dt}")
    print(f"[INFO] S3_BUCKET={S3_BUCKET} AWS_REGION={AWS_REGION}")

    if not S3_BUCKET:
        raise ValueError("Missing S3_BUCKET in environment. Did you `set -a; source .env; set +a`?")

    s3 = boto3.client("s3", region_name=AWS_REGION)

    all_dfs = []
    for sym in SYMBOLS:
        raw_key = f"raw/alphavantage/daily/symbol={sym}/dt={dt}/data.json"
        print(f"[INFO] Reading s3://{S3_BUCKET}/{raw_key}")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=raw_key)
        payload = json.loads(obj["Body"].read().decode("utf-8"))

        if "Error Message" in payload:
            raise RuntimeError(f"Alpha Vantage error for {sym}: {payload['Error Message']}")
        if "Note" in payload:
            raise RuntimeError(f"Rate limit payload for {sym}: {payload['Note']}")
        if "Time Series (Daily)" not in payload:
            print(f"[WARN] Skipping {sym}: missing Time Series (Daily). Keys={list(payload.keys())}")
            print(f"[WARN] Payload preview: {payload}")
            continue

        all_dfs.append(parse_av_daily_adjusted(payload, sym))

    if not all_dfs:
        raise RuntimeError("No valid symbols found to curate. All payloads were non-data responses.")

    out = pd.concat(all_dfs, ignore_index=True)

    curated_key = f"curated/prices_daily_csv/dt={dt}/prices_daily.csv"
    print(f"[INFO] Uploading curated CSV -> s3://{S3_BUCKET}/{curated_key}")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        out.to_csv(f.name, index=False)
        tmp_path = f.name

    s3.upload_file(tmp_path, S3_BUCKET, curated_key)
    os.remove(tmp_path)

    print("[INFO] Done. Preview:")
    print(out.head(5).to_string(index=False))

if __name__ == "__main__":
    main()
