"""Deterministic dataset-key deduplication."""

from __future__ import annotations

from typing import Any


class ConflictingBusinessKeyError(ValueError):
    pass


def deduplicate(frame: Any, dataset: str, processed_contract: dict[str, Any]):
    from pyspark.sql import functions as F

    keys = processed_contract["datasets"][dataset]["deduplication_key"]
    columns = sorted(column for column in frame.columns if not column.startswith("_raw__"))
    signed = frame.withColumn("_row_signature", F.sha2(F.to_json(F.struct(*[F.col(column) for column in columns])), 256)).cache()
    if signed.groupBy(*keys).agg(F.countDistinct("_row_signature").alias("versions")).filter("versions > 1").limit(1).count():
        signed.unpersist()
        raise ConflictingBusinessKeyError(f"conflicting_business_key: {dataset} contains conflicting key values")
    source_count = signed.count()
    result = signed.orderBy("_row_signature").dropDuplicates(keys).drop("_row_signature").cache()
    duplicate_count = source_count - result.count()
    signed.unpersist()
    return result, duplicate_count
