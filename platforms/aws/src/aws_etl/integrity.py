"""Processed-data row validation and cross-dataset referential integrity."""

from __future__ import annotations

from functools import reduce
from operator import and_, or_
from typing import Any


REASON_DESCRIPTIONS = {
    "required_key_null": "A required contracted field is null.",
    "invalid_monetary_value": "A monetary value is negative.",
    "invalid_review_score": "The review score is outside the inclusive range 1 through 5.",
    "invalid_range": "A numeric value is outside its valid range.",
    "impossible_timestamp": "Timestamp ordering is chronologically impossible.",
    "conflicting_business_key": "More than one row has the contracted business key.",
    "duplicate_fact_grain": "More than one row has the contracted fact grain.",
    "orphan_reference": "A foreign-key value has no accepted parent row.",
}

HASH_EXCLUDED_FIELDS = {
    "RecordHash", "RowEffectiveDate", "RowExpirationDate", "CurrentFlag", "LastUpdated",
}


def _any(conditions: list[Any]):
    from pyspark.sql import functions as F

    return reduce(or_, conditions, F.lit(False))


def _append_reason(frame: Any, condition: Any, code: str):
    """Append one reason without duplicating codes already attached to a row."""
    from pyspark.sql import functions as F

    addition = F.when(condition, F.array(F.lit(code))).otherwise(F.array().cast("array<string>"))
    descriptions = F.when(condition, F.array(F.lit(REASON_DESCRIPTIONS[code]))).otherwise(
        F.array().cast("array<string>")
    )
    return (
        frame.withColumn("rejection_codes", F.array_sort(F.array_distinct(F.concat("rejection_codes", addition))))
        .withColumn(
            "rejection_descriptions",
            F.transform(
                "rejection_codes",
                lambda item: F.element_at(
                    F.create_map(
                        *[
                            value
                            for reason in sorted(REASON_DESCRIPTIONS)
                            for value in (F.lit(reason), F.lit(REASON_DESCRIPTIONS[reason]))
                        ]
                    ),
                    item,
                ),
            ),
        )
    )


def initialize_rejections(frame: Any):
    from pyspark.sql import functions as F

    return frame.withColumn("rejection_codes", F.array().cast("array<string>")).withColumn(
        "rejection_descriptions", F.array().cast("array<string>")
    )


def validate_dataset(frame: Any, dataset: str, processed_contract: dict[str, Any]):
    """Attach all row-level and dataset-key reasons without emitting a row twice."""
    from pyspark.sql import functions as F

    definition = processed_contract["datasets"][dataset]
    classified = initialize_rejections(frame)
    required = [field["name"] for field in definition["fields"] if not field["nullable"]]
    classified = _append_reason(classified, _any([F.col(name).isNull() for name in required]), "required_key_null")

    if dataset == "order_items":
        classified = _append_reason(
            classified,
            (F.col("order_item_id") < 1) | (F.col("price") < 0) | (F.col("freight_value") < 0),
            "invalid_monetary_value",
        )
    elif dataset == "order_payments":
        classified = _append_reason(classified, F.col("payment_value") < 0, "invalid_monetary_value")
        classified = _append_reason(
            classified,
            (F.col("payment_sequential") < 1) | (F.col("payment_installments") < 0),
            "invalid_range",
        )
    elif dataset == "order_reviews":
        classified = _append_reason(classified, ~F.col("review_score").between(1, 5), "invalid_review_score")
        classified = _append_reason(
            classified,
            F.col("review_answer_timestamp") < F.col("review_creation_date"),
            "impossible_timestamp",
        )
    elif dataset == "orders":
        impossible = (
            (F.col("order_approved_at") < F.col("order_purchase_timestamp"))
            | (F.col("order_delivered_carrier_date") < F.col("order_approved_at"))
            | (F.col("order_delivered_customer_date") < F.col("order_delivered_carrier_date"))
        )
        classified = _append_reason(classified, impossible, "impossible_timestamp")
        classified = _append_reason(
            classified,
            ~F.col("order_month").between(1, 12) | ~F.col("order_day").between(1, 31),
            "invalid_range",
        )
    elif dataset == "products":
        numeric = [
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ]
        classified = _append_reason(
            classified,
            _any([F.col(name).isNotNull() & (F.col(name) < 0) for name in numeric]),
            "invalid_range",
        )
    elif dataset == "geolocation":
        classified = _append_reason(
            classified,
            ~F.col("geolocation_lat").between(-90, 90) | ~F.col("geolocation_lng").between(-180, 180),
            "invalid_range",
        )

    keys = definition["business_key"]
    duplicates = frame.groupBy(*keys).count().filter(F.col("count") > 1).select(*keys)
    duplicate_condition = reduce(
        and_,
        [F.col(f"source.{name}").eqNullSafe(F.col(f"duplicate.{name}")) for name in keys],
    )
    classified = (
        classified.alias("source")
        .join(duplicates.alias("duplicate"), duplicate_condition, "left")
        .withColumn("_duplicate_business_key", F.col(f"duplicate.{keys[0]}").isNotNull())
        .select("source.*", "_duplicate_business_key")
    )
    duplicate_reason = "duplicate_fact_grain" if dataset in {"order_items", "order_reviews"} else "conflicting_business_key"
    return _append_reason(classified, F.col("_duplicate_business_key"), duplicate_reason).drop(
        "_duplicate_business_key"
    )


def apply_reference_rule(child: Any, parent: Any, child_field: str, parent_field: str):
    """Reject child keys absent from the already-accepted parent key set."""
    from pyspark.sql import functions as F

    valid_keys = parent.select(F.col(parent_field).alias("_valid_parent_key")).distinct()
    joined = child.join(valid_keys, child[child_field] == valid_keys["_valid_parent_key"], "left")
    return _append_reason(joined, F.col("_valid_parent_key").isNull(), "orphan_reference").drop(
        "_valid_parent_key"
    )


def split_rows(frame: Any) -> tuple[Any, Any]:
    from pyspark.sql import functions as F

    return (
        frame.filter(F.size("rejection_codes") == 0).drop("rejection_codes", "rejection_descriptions"),
        frame.filter(F.size("rejection_codes") > 0),
    )


def conform_curated(frame: Any, dataset: str, curated_contract: dict[str, Any]):
    """Cast to the contract and append its stable canonical full-record hash."""
    from pyspark.sql import functions as F

    from aws_etl.schemas import spark_type

    fields = curated_contract["datasets"][dataset]["fields"]
    data_fields = [field for field in fields if field["name"] != "RecordHash"]
    conformed = frame.select(
        *(F.col(field["name"]).cast(spark_type(field["type"])).alias(field["name"]) for field in data_fields)
    )
    if any(field["name"] == "RecordHash" for field in fields):
        canonical = []
        for field in data_fields:
            name, logical_type = field["name"], field["type"]
            value = F.col(name)
            if name in HASH_EXCLUDED_FIELDS:
                continue
            if logical_type == "decimal" or logical_type.startswith("decimal("):
                # DecimalType carries the contract's fixed scale. Its string
                # representation preserves that scale and avoids Java Formatter,
                # which cannot format Spark Decimal objects directly.
                value = F.when(value.isNull(), F.lit(None).cast("string")).otherwise(
                    value.cast("string")
                )
            elif logical_type == "timestamp":
                value = F.when(value.isNull(), F.lit(None).cast("string")).otherwise(
                    F.date_format(value, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
                )
            elif logical_type == "date":
                value = F.when(value.isNull(), F.lit(None).cast("string")).otherwise(
                    F.date_format(value, "yyyy-MM-dd")
                )
            canonical.append(value.alias(name))
        document = F.to_json(F.struct(*canonical), {"ignoreNullFields": "false"})
        conformed = conformed.withColumn("RecordHash", F.sha2(document, 256))
    return conformed.select(*(field["name"] for field in fields))


def assert_unique_grain(frame: Any, dataset: str, fields: list[str]) -> None:
    from pyspark.sql import functions as F

    if frame.groupBy(*fields).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"duplicate_fact_grain: {dataset} has duplicate grain {fields}")


def assert_curated_content(frame: Any, dataset: str, curated_contract: dict[str, Any]) -> None:
    """Verify required values and the canonical lowercase SHA-256 fingerprint."""
    from pyspark.sql import functions as F

    fields = curated_contract["datasets"][dataset]["fields"]
    required = [field["name"] for field in fields if not field["nullable"]]
    if frame.filter(_any([F.col(name).isNull() for name in required])).limit(1).count():
        raise ValueError(f"curated required field is null for {dataset}")
    if "RecordHash" in required and frame.filter(~F.col("RecordHash").rlike("^[0-9a-f]{64}$")).limit(1).count():
        raise ValueError(f"invalid RecordHash for {dataset}")
