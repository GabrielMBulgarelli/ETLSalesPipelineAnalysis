"""S3-compatible storage operations used by the local ingestion foundation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from .config import AwsEtlConfig


def s3_client(config: AwsEtlConfig):
    return boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        region_name=config.region,
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        aws_session_token=config.aws_session_token,
        config=BotoConfig(s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"}),
    )


def ensure_bucket(client: Any, config: AwsEtlConfig) -> None:
    try:
        client.head_bucket(Bucket=config.bucket)
        return
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
    arguments: dict[str, Any] = {"Bucket": config.bucket}
    if config.region != "us-east-1":
        arguments["CreateBucketConfiguration"] = {"LocationConstraint": config.region}
    client.create_bucket(**arguments)


def expected_prefixes(datasets: Iterable[str]) -> list[str]:
    dataset_names = sorted(datasets)
    return [
        *(f"raw/{dataset}/" for dataset in dataset_names),
        *(f"processed/{dataset}/" for dataset in dataset_names),
        "curated/dimensions/",
        "curated/facts/",
        "curated/aggregations/",
        *(f"rejected/{dataset}/" for dataset in dataset_names),
        "quality/",
        "manifests/",
        "audit/",
        "staging/",
    ]


def ensure_prefixes(client: Any, bucket: str, prefixes: Iterable[str]) -> None:
    for prefix in prefixes:
        if not object_exists(client, bucket, prefix):
            client.put_object(Bucket=bucket, Key=prefix, Body=b"")


def object_exists(client: Any, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def upload_file_if_absent(client: Any, bucket: str, key: str, source: Path) -> bool:
    if object_exists(client, bucket, key):
        return False
    client.upload_file(str(source), bucket, key)
    return True


def put_json_immutable(client: Any, bucket: str, key: str, document: dict[str, Any]) -> None:
    body = json.dumps(document, indent=2, sort_keys=True).encode("utf-8") + b"\n"

    def add_conditional_header(params: dict[str, Any], **_: Any) -> None:
        params["headers"]["If-None-Match"] = "*"

    event_name = "before-call.s3.PutObject"
    client.meta.events.register_first(event_name, add_conditional_header)
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"PreconditionFailed", "412"}:
            raise FileExistsError(f"immutable S3 object already exists: s3://{bucket}/{key}") from exc
        raise
    finally:
        client.meta.events.unregister(event_name, add_conditional_header)


def get_json(client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"NoSuchKey", "404", "NotFound"}:
            return None
        raise
    document = json.loads(response["Body"].read().decode("utf-8"))
    if not isinstance(document, dict):
        raise ValueError(f"S3 JSON object must contain a mapping: s3://{bucket}/{key}")
    return document


def list_keys(client: Any, bucket: str, prefix: str = "") -> list[str]:
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    return sorted(keys)


def list_json_documents(client: Any, bucket: str, prefix: str) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for key in list_keys(client, bucket, prefix):
        if not key.endswith(".json"):
            continue
        document = get_json(client, bucket, key)
        if document is not None:
            documents.append(document)
    return documents
