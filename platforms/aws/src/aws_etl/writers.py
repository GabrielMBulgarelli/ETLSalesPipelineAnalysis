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


def terminal_summary_key(config: Any, batch_id: str, stage: str) -> str:
    if stage not in {"validation", "curation"}:
        raise ValueError(f"unknown terminal stage: {stage}")
    return f"{config.quality_prefix}batch_id={batch_id}/{stage}-summary.json"


def existing_terminal_summary(client: Any, config: Any, batch_id: str, stage: str) -> dict[str, Any] | None:
    return get_json(client, config.bucket, terminal_summary_key(config, batch_id, stage))


def publish_terminal_summary(client: Any, config: Any, batch_id: str, stage: str, summary: dict[str, Any]) -> None:
    put_json_immutable(client, config.bucket, terminal_summary_key(config, batch_id, stage), summary)


def verify_marker_outputs(
    client: Any,
    config: Any,
    summary: dict[str, Any],
    expected_identity: dict[str, str],
    batch_id: str,
    contract_version: int,
) -> None:
    if not isinstance(summary.get("attempt_id"), str) or not summary["attempt_id"]:
        raise RuntimeError("completion marker does not declare an attempt identity")
    if summary.get("batch_id") != batch_id:
        raise RuntimeError("completion marker batch identity does not match the requested batch")
    if summary.get("contract_version") != contract_version:
        raise RuntimeError("completion marker contract version does not match the active contract")
    if summary.get("pipeline_version") != config.pipeline_version:
        raise RuntimeError("completion marker pipeline version does not match the active pipeline")
    if summary.get("source_content_identity") != expected_identity:
        raise RuntimeError("completion marker source identity does not match the manifested batch")
    outputs = summary.get("produced_datasets")
    if not isinstance(outputs, dict) or not outputs:
        raise RuntimeError("completion marker does not declare produced datasets")
    if not isinstance(summary.get("row_counts"), dict):
        raise RuntimeError("completion marker does not declare row counts")
    for dataset, output in sorted(outputs.items()):
        if (
            not isinstance(output, dict)
            or not isinstance(output.get("prefix"), str)
            or not isinstance(output.get("row_count"), int)
            or output["row_count"] < 0
        ):
            raise RuntimeError(f"completion marker has invalid output metadata for {dataset}")
        keys = parquet_keys(client, config.bucket, output["prefix"])
        if not keys:
            raise RuntimeError(f"completion marker/output mismatch: no Parquet exists for {dataset}")
