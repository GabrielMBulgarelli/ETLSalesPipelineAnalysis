"""Raw manifest and explicit-schema CSV readers."""

from __future__ import annotations

from typing import Any

from .manifests import manifest_key
from .schemas import DATASETS, raw_schema
from .storage import get_json


def load_batch_manifests(client: Any, config: Any, batch_id: str) -> dict[str, dict[str, Any]]:
    manifests = {}
    for dataset in DATASETS:
        key = manifest_key(config.manifest_prefix, dataset, batch_id)
        manifest = get_json(client, config.bucket, key)
        if manifest is None:
            raise ValueError(f"missing_dataset: manifest is absent for {dataset} in batch {batch_id}")
        if manifest.get("dataset") != dataset or manifest.get("batch_id") != batch_id:
            raise ValueError(f"invalid manifest identity at s3://{config.bucket}/{key}")
        manifests[dataset] = manifest
    if len({item.get("batch_timestamp") for item in manifests.values()}) != 1:
        raise ValueError("batch manifests do not share one batch_timestamp")
    return manifests


def read_raw_csv(spark: Any, config: Any, dataset: str, manifest: dict[str, Any], raw_contract: dict[str, Any]):
    expected = [field["source_name"] for field in raw_contract["datasets"][dataset]["fields"]]
    frame = (spark.read.option("header", "true").option("mode", "PERMISSIVE").option("enforceSchema", "false")
             .schema(raw_schema(dataset, raw_contract)).csv(f"s3a://{config.bucket}/{manifest['source_object_path']}"))
    if frame.columns != expected:
        raise ValueError(f"missing_column: unexpected headers for {dataset}: {frame.columns}")
    return frame
