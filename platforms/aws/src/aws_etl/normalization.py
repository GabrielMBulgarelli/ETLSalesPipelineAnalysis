"""Canonical field mapping, explicit parsing, and processed derivations."""

from __future__ import annotations

from typing import Any

from .schemas import RUNTIME_METADATA, spark_type

SPARK_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


def normalize_raw(frame: Any, dataset: str, raw_contract: dict[str, Any]):
    from pyspark.sql import functions as F

    expressions = []
    for field in raw_contract["datasets"][dataset]["fields"]:
        source, name = field["source_name"], field["name"]
        clean = F.when(F.trim(F.col(source)) == "", F.lit(None)).otherwise(F.trim(F.col(source)))
        if field["type"] == "timestamp":
            typed = F.to_timestamp(clean, SPARK_TIMESTAMP_FORMAT)
        elif field["type"] == "string":
            typed = clean
        else:
            typed = clean.cast(spark_type(field["type"]))
        expressions.extend((clean.alias(f"_raw__{name}"), typed.alias(name)))
    return frame.select(*expressions)


def derive_orders(frame: Any, processed_contract: dict[str, Any]):
    from pyspark.sql import functions as F

    rules = processed_contract["datasets"]["orders"]["status_derivation"]
    source_status = F.lower(F.col(rules["source_field"]))
    status = None
    for source, target in rules["terminal_source_values"].items():
        status = F.when(source_status == source, target) if status is None else status.when(source_status == source, target)
    for item in rules["timestamp_precedence"]:
        status = status.when(F.col(item["when_present"]).isNotNull(), item["value"])
    status = status.otherwise(rules["default"])
    shipping = F.when(F.col("order_approved_at").isNotNull() & F.col("order_delivered_carrier_date").isNotNull(), F.datediff("order_delivered_carrier_date", "order_approved_at"))
    delivery = F.when(F.col("order_delivered_carrier_date").isNotNull() & F.col("order_delivered_customer_date").isNotNull(), F.datediff("order_delivered_customer_date", "order_delivered_carrier_date"))
    total = F.when(F.col("order_purchase_timestamp").isNotNull() & F.col("order_delivered_customer_date").isNotNull(), F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
    delayed = F.when(F.col("order_delivered_customer_date").isNotNull() & F.col("order_estimated_delivery_date").isNotNull(), F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"))
    return (frame.withColumn("order_status", status).withColumn("order_year", F.year("order_purchase_timestamp"))
            .withColumn("order_month", F.month("order_purchase_timestamp")).withColumn("order_day", F.dayofmonth("order_purchase_timestamp"))
            .withColumn("shipping_time_days", shipping).withColumn("delivery_time_days", delivery).withColumn("total_delivery_time_days", total)
            .withColumn("is_delayed", delayed).withColumn("delay_days", F.when(F.col("is_delayed"), F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")).otherwise(0).cast("int"))
            .withColumn("is_approved", F.col("order_approved_at").isNotNull()).withColumn("is_shipped", F.col("order_delivered_carrier_date").isNotNull())
            .withColumn("is_delivered", F.col("order_delivered_customer_date").isNotNull()).withColumn("shipping_days", F.coalesce("shipping_time_days", F.lit(-1)).cast("int"))
            .withColumn("delivery_days", F.coalesce("delivery_time_days", F.lit(-1)).cast("int")).withColumn("total_days", F.coalesce("total_delivery_time_days", F.lit(-1)).cast("int")))


def enrich_products(products: Any, translations: Any, fallback: str):
    from pyspark.sql import functions as F

    lookup = translations.select("product_category_name", "product_category_name_english")
    return (products.withColumn("product_category_name", F.coalesce("product_category_name", F.lit(fallback)))
            .join(lookup, "product_category_name", "left")
            .withColumn("product_category_name_english", F.coalesce("product_category_name_english", F.lit(fallback))))


def append_runtime_metadata(frame: Any, manifest: dict[str, Any], processing_timestamp: str, contract_version: int):
    from pyspark.sql import functions as F

    return (frame.withColumn("batch_id", F.lit(manifest["batch_id"])).withColumn("source_file_id", F.lit(manifest["source_file_id"]))
            .withColumn("ingestion_timestamp", F.lit(manifest["batch_timestamp"]).cast("timestamp"))
            .withColumn("processing_timestamp", F.lit(processing_timestamp).cast("timestamp"))
            .withColumn("contract_version", F.lit(contract_version).cast("int")))


def select_processed_columns(frame: Any, dataset: str, processed_contract: dict[str, Any]):
    from pyspark.sql import functions as F

    business = [F.col(field["name"]).cast(spark_type(field["type"])).alias(field["name"]) for field in processed_contract["datasets"][dataset]["fields"]]
    return frame.select(*business, *[F.col(name) for name in RUNTIME_METADATA])
