"""Contract loading and explicit Spark schemas for all nine Olist datasets."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

DATASETS = ("customers", "orders", "order_items", "order_payments", "order_reviews", "products", "sellers", "geolocation", "category_translation")
RUNTIME_METADATA = ("batch_id", "source_file_id", "ingestion_timestamp", "processing_timestamp", "contract_version")
DECIMAL_PATTERN = re.compile(r"decimal\((\d+),(\d+)\)")


def load_contract(path: str | Path, expected_stage: str) -> dict[str, Any]:
    contract_path = Path(path)
    try:
        document = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"unreadable {expected_stage} contract: {contract_path}") from exc
    if not isinstance(document, dict) or document.get("stage") != expected_stage:
        raise ValueError(f"invalid {expected_stage} contract: {contract_path}")
    if document.get("contract_version") != 1:
        raise ValueError(f"unsupported {expected_stage} contract version")
    datasets = document.get("datasets")
    if not isinstance(datasets, dict):
        raise ValueError(f"{expected_stage} contract must declare a datasets mapping")
    if expected_stage in {"raw", "processed"} and tuple(datasets) != DATASETS:
        raise ValueError(f"{expected_stage} contract must declare all nine datasets in canonical order")
    return document


def spark_type(logical_type: str):
    from pyspark.sql.types import BooleanType, DateType, DecimalType, IntegerType, StringType, TimestampType

    scalar_types = {"string": StringType, "integer": IntegerType, "boolean": BooleanType, "timestamp": TimestampType, "date": DateType}
    if logical_type in scalar_types:
        return scalar_types[logical_type]()
    if logical_type == "decimal":
        return DecimalType(38, 18)
    match = DECIMAL_PATTERN.fullmatch(logical_type)
    if match:
        return DecimalType(int(match.group(1)), int(match.group(2)))
    raise ValueError(f"unsupported contract type: {logical_type}")


def raw_schema(dataset: str, raw_contract: dict[str, Any]):
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType([StructField(field["source_name"], StringType(), True) for field in raw_contract["datasets"][dataset]["fields"]])


def processed_schema(dataset: str, processed_contract: dict[str, Any]):
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

    fields = [StructField(field["name"], spark_type(field["type"]), bool(field["nullable"])) for field in processed_contract["datasets"][dataset]["fields"]]
    fields.extend([
        StructField("batch_id", StringType(), False),
        StructField("source_file_id", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("processing_timestamp", TimestampType(), False),
        StructField("contract_version", IntegerType(), False),
    ])
    return StructType(fields)


def assert_processed_schema(dataset: str, frame: Any, processed_contract: dict[str, Any]) -> None:
    expected = processed_schema(dataset, processed_contract)
    actual_pairs = [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]
    expected_pairs = [(field.name, field.dataType.simpleString()) for field in expected.fields]
    if actual_pairs != expected_pairs:
        raise ValueError(f"processed schema mismatch for {dataset}: expected {expected_pairs}, got {actual_pairs}")


def assert_curated_schema(dataset: str, frame: Any, curated_contract: dict[str, Any]) -> None:
    expected = [
        (field["name"], spark_type(field["type"]).simpleString())
        for field in curated_contract["datasets"][dataset]["fields"]
    ]
    actual = [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]
    if actual != expected:
        raise ValueError(f"curated schema mismatch for {dataset}: expected {expected}, got {actual}")
