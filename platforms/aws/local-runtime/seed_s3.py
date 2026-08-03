#!/usr/bin/env python3
"""Bootstrap LocalStack S3 and seed the nine contracted Olist sources."""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from aws_etl.audit import audit_key, create_audit_record
from aws_etl.config import load_config
from aws_etl.manifests import (
    SOURCE_FILES,
    create_manifest,
    discover_source_files,
    immutable_identity_matches,
    inspect_source_file,
    manifest_key,
)
from aws_etl.replay import ReplayDecision, classify_replay, next_attempt_number
from aws_etl.storage import (
    ensure_bucket,
    ensure_prefixes,
    expected_prefixes,
    get_json,
    list_json_documents,
    list_keys,
    object_exists,
    put_json_immutable,
    s3_client,
    upload_file_if_absent,
)


BATCH_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, help="local YAML configuration file")
    parser.add_argument("--dataset-dir", type=Path, help="directory containing exactly nine Olist CSV files")
    parser.add_argument("--batch-id", help="stable batch identifier (defaults to a new identifier)")
    parser.add_argument("--batch-timestamp", help="immutable UTC timestamp for a new batch")
    parser.add_argument("--bootstrap-only", action="store_true", help="create only the bucket and prefixes")
    parser.add_argument("--status", action="store_true", help="inspect the bucket and required prefixes")
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def new_batch_id() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"batch-{timestamp}-{uuid4().hex[:8]}"


def validate_batch_id(batch_id: str) -> None:
    if BATCH_ID_PATTERN.fullmatch(batch_id) is None:
        raise ValueError("batch ID must be 1-128 characters using letters, numbers, period, underscore, or hyphen")


def normalize_timestamp(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != datetime.now(UTC).utcoffset():
        raise ValueError("batch timestamp must include the UTC timezone")
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def find_existing_batch_timestamp(history: list[dict[str, object]], batch_id: str) -> str | None:
    timestamps = {
        str(item.get("BatchTimestamp", item.get("batch_timestamp")))
        for item in history
        if item.get("BatchID", item.get("batch_id")) == batch_id
        and item.get("BatchTimestamp", item.get("batch_timestamp"))
    }
    if len(timestamps) > 1:
        raise ValueError(f"audit history has conflicting timestamps for batch ID {batch_id!r}")
    return next(iter(timestamps), None)


def print_status(client, bucket: str, prefixes: list[str], manifest_prefix: str, audit_prefix: str) -> None:
    missing = [prefix for prefix in prefixes if not object_exists(client, bucket, prefix)]
    print(f"Bucket: s3://{bucket}")
    print(f"Required prefixes: {len(prefixes) - len(missing)}/{len(prefixes)} present")
    print(f"Raw source objects: {len([key for key in list_keys(client, bucket, 'raw/') if key.endswith('.csv')])}")
    print(f"Manifest objects: {len(list_keys(client, bucket, manifest_prefix)) - 1}")
    print(f"Audit objects: {len(list_keys(client, bucket, audit_prefix)) - 1}")
    if missing:
        print("Missing prefixes:")
        print("\n".join(f"  {prefix}" for prefix in missing))
        raise RuntimeError("S3 foundation is incomplete")


def main() -> int:
    args = parse_args()
    try:
        config = load_config(args.config)
        client = s3_client(config)
        prefixes = expected_prefixes(SOURCE_FILES.values())
        if args.bootstrap_only:
            ensure_bucket(client, config)
            ensure_prefixes(client, config.bucket, prefixes)
            print(f"S3 foundation ready: s3://{config.bucket} ({len(prefixes)} prefixes)")
            return 0
        if args.status:
            print_status(client, config.bucket, prefixes, config.manifest_prefix, config.audit_prefix)
            return 0

        dataset_dir = args.dataset_dir or (Path(os.environ["DATASET_DIR"]) if os.environ.get("DATASET_DIR") else None)
        if dataset_dir is None:
            raise ValueError("DATASET_DIR is required")

        # Validate and hash every source before any raw, manifest, or audit publication.
        sources = [inspect_source_file(path) for path in discover_source_files(dataset_dir)]
        ensure_bucket(client, config)
        ensure_prefixes(client, config.bucket, prefixes)
        history = list_json_documents(client, config.bucket, config.audit_prefix)
        manifest_history = list_json_documents(client, config.bucket, config.manifest_prefix)
        batch_id = args.batch_id or os.environ.get("BATCH_ID") or new_batch_id()
        validate_batch_id(batch_id)
        requested_timestamp = args.batch_timestamp or os.environ.get("BATCH_TIMESTAMP")
        existing_timestamp = find_existing_batch_timestamp(history + manifest_history, batch_id)
        batch_timestamp = normalize_timestamp(requested_timestamp or existing_timestamp or utc_now())

        work: list[tuple[object, dict[str, object], str, ReplayDecision]] = []
        for source in sources:
            manifest = create_manifest(source, batch_id, batch_timestamp, config.pipeline_version)
            key = manifest_key(config.manifest_prefix, source.dataset, batch_id)
            manifest["manifest_object_path"] = key
            existing_manifest = get_json(client, config.bucket, key)
            if existing_manifest is not None and not immutable_identity_matches(existing_manifest, manifest):
                decision = ReplayDecision(
                    "fail-batch",
                    reason="immutable manifest content changed under an existing batch ID",
                )
            else:
                decision = classify_replay(manifest, history)
            work.append((source, manifest, key, decision))

        batch_failed = any(decision.outcome == "fail-batch" for _, _, _, decision in work)
        published_raw = 0
        results: list[str] = []
        for source, manifest, key, decision in work:
            assert hasattr(source, "dataset")
            if batch_failed and decision.outcome not in {"fail-batch", "no-op", "reused-failure"}:
                decision = ReplayDecision("fail-batch", reason="batch aborted because another dataset changed immutable identity")

            if not batch_failed and decision.outcome in {"initial-load", "new-content", "late-content", "retry"}:
                if upload_file_if_absent(client, config.bucket, str(manifest["source_object_path"]), source.path):
                    published_raw += 1
            if decision.outcome != "fail-batch" and not object_exists(client, config.bucket, key):
                put_json_immutable(client, config.bucket, key, manifest)

            attempt = next_attempt_number(source.dataset, batch_id, history)
            record = create_audit_record(manifest, decision, attempt)
            put_json_immutable(client, config.bucket, audit_key(config.audit_prefix, record), record)
            history.append(record)
            results.append(f"{source.dataset}: {decision.outcome} (attempt {attempt})")

        print(f"Batch ID: {batch_id}")
        print(f"Batch timestamp: {batch_timestamp}")
        print("\n".join(results))
        print(f"New raw objects published: {published_raw}")
        if batch_failed:
            raise RuntimeError("batch failed because immutable content changed under an existing batch ID")
        if any(decision.outcome == "reused-failure" for _, _, _, decision in work):
            raise RuntimeError("batch reused a deterministic prior failure without reprocessing")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
