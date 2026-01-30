import json
import boto3

def put_json(bucket: str, key: str, payload: dict, region: str | None = None) -> None:
    if not bucket:
        raise ValueError("Missing S3_BUCKET in environment (.env).")

    s3 = boto3.client("s3", region_name=region)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )
