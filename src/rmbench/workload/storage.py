from __future__ import annotations

from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from rmbench.workload.engine import TABLE_NAMES

UPLOAD_ENDPOINT = "http://127.0.0.1:19000"
COMPOSE_ENDPOINT = "http://minio:9000"
BUCKET = "benchmark"
ACCESS_KEY = "access_key"
SECRET_KEY = "secret_key"


def _make_s3_client(*, endpoint_url: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            connect_timeout=5,
            retries={"max_attempts": 3, "mode": "standard"},
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def _delete_prefix(client, *, bucket: str, prefix: str) -> None:
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})


def upload_prefix(
    *,
    input_dir: Path,
    prefix: str,
    access_key: str = ACCESS_KEY,
    secret_key: str = SECRET_KEY,
    bucket: str = BUCKET,
    endpoint_url: str = UPLOAD_ENDPOINT,
) -> list[str]:
    if not input_dir.exists():
        raise ValueError(f"Missing {input_dir}. Generate the scale factor first.")

    client = _make_s3_client(endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key)
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)

    resolved_prefix = prefix.strip("/")
    _delete_prefix(client, bucket=bucket, prefix=resolved_prefix)

    uploaded_keys: list[str] = []
    for table_name in TABLE_NAMES:
        file_path = input_dir / f"{table_name}.csv.gz"
        if not file_path.exists():
            raise ValueError(f"Missing {file_path}.")
        object_key = f"{resolved_prefix}/{file_path.name}"
        client.upload_file(str(file_path), bucket, object_key)
        uploaded_keys.append(object_key)
    return uploaded_keys


def s3_source_root(*, prefix: str, endpoint_url: str, bucket: str = BUCKET) -> str:
    return f"{endpoint_url.rstrip('/')}/{bucket}/{prefix.strip('/')}"
