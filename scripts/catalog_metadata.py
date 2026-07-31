#!/usr/bin/env python3
"""Generate and validate curated AWS Glue catalog metadata."""

from __future__ import annotations

import argparse
import copy
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import yaml


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "contracts/schemas/curated/datasets.yaml"
CLOUD_DATABASE_PATH = ROOT / "platforms/aws/catalog/database.json"
CLOUD_TABLE_DIR = ROOT / "platforms/aws/catalog/tables"
LOCAL_TABLE_DIR = ROOT / "platforms/aws/runtime/local/catalog-manifests"

DATABASE_NAME = "ecommerce_sales_curated"
CLOUD_BUCKET = "${AWS_ETL_BUCKET}"
LOCAL_BUCKET = "ecommerce-sales-local"
INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
SERDE_LIBRARY = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
DECIMAL_PATTERN = re.compile(r"decimal\((\d+),(\d+)\)")
LAYERS = {
    "dim_customer": "dimensions",
    "dim_product": "dimensions",
    "dim_seller": "dimensions",
    "dim_geography": "dimensions",
    "dim_date": "dimensions",
    "dim_order_status": "dimensions",
    "fact_sales": "facts",
    "fact_reviews": "facts",
    "sales_by_state": "aggregations",
    "sales_by_category": "aggregations",
    "monthly_sales": "aggregations",
    "order_status": "aggregations",
    "cross_state_analysis": "aggregations",
    "seller_performance": "aggregations",
    "size_analysis": "aggregations",
    "payment_methods": "aggregations",
}
PROVIDER_SPECIFIC_KEYS = {
    "DatabaseName",
    "TableInput",
    "TableType",
    "StorageDescriptor",
    "PartitionKeys",
    "SerdeInfo",
    "InputFormat",
    "OutputFormat",
    "SerializationLibrary",
}
PROVIDER_SPECIFIC_VALUES = ("s3://", "${AWS_ETL_BUCKET}", "ParquetHiveSerDe")


class CatalogValidationError(ValueError):
    """Raised when committed catalog metadata differs from its contract."""


def _load_contract() -> dict[str, Any]:
    document = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or document.get("stage") != "curated":
        raise CatalogValidationError(f"invalid curated contract: {CONTRACT_PATH}")
    datasets = document.get("datasets")
    if not isinstance(datasets, dict) or set(datasets) != set(LAYERS):
        raise CatalogValidationError("curated contract datasets do not match the Phase 6 catalog inventory")
    return document


def _glue_type(logical_type: str) -> str:
    scalar_types = {
        "string": "string",
        "integer": "int",
        "boolean": "boolean",
        "timestamp": "timestamp",
        "date": "date",
        "decimal": "decimal(38,18)",
    }
    if logical_type in scalar_types:
        return scalar_types[logical_type]
    match = DECIMAL_PATTERN.fullmatch(logical_type)
    if match:
        return f"decimal({int(match.group(1))},{int(match.group(2))})"
    raise CatalogValidationError(f"unsupported curated logical type: {logical_type}")


def _database_document() -> dict[str, Any]:
    return {
        "DatabaseInput": {
            "Name": DATABASE_NAME,
            "Description": "Authoritative curated Parquet metadata for the ecommerce sales pipeline.",
        }
    }


def _table_document(dataset: str, definition: dict[str, Any], bucket: str) -> dict[str, Any]:
    if definition.get("partition_columns") != []:
        raise CatalogValidationError(f"Phase 6 requires {dataset} to remain unpartitioned")
    columns = [
        {
            "Name": field["name"],
            "Type": _glue_type(field["type"]),
            "Parameters": {"nullable": "true" if field["nullable"] else "false"},
        }
        for field in definition["fields"]
    ]
    location = f"s3://{bucket}/curated/{LAYERS[dataset]}/{dataset}/"
    return {
        "DatabaseName": DATABASE_NAME,
        "TableInput": {
            "Name": dataset,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"EXTERNAL": "TRUE", "classification": "parquet"},
            "StorageDescriptor": {
                "Columns": columns,
                "Location": location,
                "InputFormat": INPUT_FORMAT,
                "OutputFormat": OUTPUT_FORMAT,
                "SerdeInfo": {
                    "SerializationLibrary": SERDE_LIBRARY,
                    "Parameters": {"serialization.format": "1"},
                },
            },
            "PartitionKeys": [],
        },
    }


def _write_json(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def generate() -> None:
    contract = _load_contract()
    _write_json(CLOUD_DATABASE_PATH, _database_document())
    for dataset, definition in contract["datasets"].items():
        _write_json(CLOUD_TABLE_DIR / f"{dataset}.json", _table_document(dataset, definition, CLOUD_BUCKET))
        _write_json(LOCAL_TABLE_DIR / f"{dataset}.json", _table_document(dataset, definition, LOCAL_BUCKET))


def _read_json(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalogValidationError(f"invalid JSON: {path}") from exc
    if not isinstance(document, dict):
        raise CatalogValidationError(f"JSON root must be an object: {path}")
    return document


def _require_inventory(directory: Path, expected_names: set[str]) -> None:
    actual_names = {path.name for path in directory.glob("*.json")}
    if actual_names != expected_names:
        missing = sorted(expected_names - actual_names)
        unexpected = sorted(actual_names - expected_names)
        raise CatalogValidationError(
            f"catalog inventory mismatch in {directory}: missing={missing}, unexpected={unexpected}"
        )


def _normalize_location_bucket(document: dict[str, Any]) -> dict[str, Any]:
    normalized = copy.deepcopy(document)
    descriptor = normalized["TableInput"]["StorageDescriptor"]
    location = descriptor["Location"]
    parsed = urlsplit(location)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise CatalogValidationError(f"invalid catalog S3 location: {location}")
    descriptor["Location"] = urlunsplit((parsed.scheme, "<bucket>", parsed.path, "", ""))
    return normalized


def _check_no_provider_leak(value: Any, path: str = "contract") -> None:
    if isinstance(value, dict):
        leaked = PROVIDER_SPECIFIC_KEYS.intersection(value)
        if leaked:
            raise CatalogValidationError(f"provider-specific keys in shared {path}: {sorted(leaked)}")
        for key, child in value.items():
            _check_no_provider_leak(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _check_no_provider_leak(child, f"{path}[{index}]")
    elif isinstance(value, str) and any(marker in value for marker in PROVIDER_SPECIFIC_VALUES):
        raise CatalogValidationError(f"provider-specific value in shared {path}: {value}")


def validate() -> None:
    contract = _load_contract()
    _check_no_provider_leak(contract)
    if _read_json(CLOUD_DATABASE_PATH) != _database_document():
        raise CatalogValidationError(f"database definition differs from expected metadata: {CLOUD_DATABASE_PATH}")

    expected_names = {f"{dataset}.json" for dataset in contract["datasets"]}
    _require_inventory(CLOUD_TABLE_DIR, expected_names)
    _require_inventory(LOCAL_TABLE_DIR, expected_names)

    for dataset, definition in contract["datasets"].items():
        cloud_path = CLOUD_TABLE_DIR / f"{dataset}.json"
        local_path = LOCAL_TABLE_DIR / f"{dataset}.json"
        cloud = _read_json(cloud_path)
        local = _read_json(local_path)
        if cloud != _table_document(dataset, definition, CLOUD_BUCKET):
            raise CatalogValidationError(f"cloud table differs from curated contract: {cloud_path}")
        if local != _table_document(dataset, definition, LOCAL_BUCKET):
            raise CatalogValidationError(f"local manifest differs from curated contract: {local_path}")
        if _normalize_location_bucket(cloud) != _normalize_location_bucket(local):
            raise CatalogValidationError(f"cloud/local catalog structures differ for {dataset}")

    print(
        f"Catalog metadata valid: database={DATABASE_NAME}, tables={len(expected_names)}, "
        "partitions=0, cloud/local=aligned"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true", help="regenerate catalog JSON from the curated contract")
    args = parser.parse_args()
    try:
        if args.generate:
            generate()
        validate()
    except (CatalogValidationError, OSError, KeyError, TypeError) as exc:
        parser.exit(1, f"catalog validation failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
