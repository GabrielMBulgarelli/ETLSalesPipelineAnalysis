"""Staged Parquet output and completion-marker publication."""

from __future__ import annotations

from typing import Any, Iterable

from .storage import get_json, list_keys, put_json_immutable


def delete_prefix(client: Any, bucket: str, prefix: str) -> None:
    keys = list_keys(client, bucket, prefix)
    for offset in range(0, len(keys), 1000):
        client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": key} for key in keys[offset:offset + 1000]]})


def parquet_keys(client: Any, bucket: str, prefix: str) -> list[str]:
    return [key for key in list_keys(client, bucket, prefix) if key.endswith(".parquet")]


def write_staged(frame: Any, bucket: str, prefix: str) -> None:
    frame.coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket}/{prefix}")


def verify_staged(client: Any, bucket: str, prefixes: Iterable[str]) -> None:
    missing = [prefix for prefix in prefixes if not parquet_keys(client, bucket, prefix)]
    if missing:
        raise RuntimeError(f"staged output is incomplete: {', '.join(missing)}")


def copy_prefix(client: Any, bucket: str, source_prefix: str, target_prefix: str) -> None:
    for source_key in list_keys(client, bucket, source_prefix):
        suffix = source_key[len(source_prefix):]
        if suffix:
            client.copy_object(Bucket=bucket, Key=f"{target_prefix}{suffix}", CopySource={"Bucket": bucket, "Key": source_key})


def summary_key(config: Any, batch_id: str) -> str:
    return f"{config.quality_prefix}batch_id={batch_id}/processed-summary.json"


def existing_summary(client: Any, config: Any, batch_id: str) -> dict[str, Any] | None:
    return get_json(client, config.bucket, summary_key(config, batch_id))


def publish_summary(client: Any, config: Any, batch_id: str, summary: dict[str, Any]) -> None:
    put_json_immutable(client, config.bucket, summary_key(config, batch_id), summary)
