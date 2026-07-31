"""Stable row-level rejection classification."""

from __future__ import annotations

from functools import reduce
from operator import or_
from typing import Any

REASON_DESCRIPTIONS = {
    "required_key_null": "A required business-key value is null or blank.",
    "invalid_parse": "A required value is blank or cannot be parsed as its contracted type.",
    "invalid_monetary_value": "A monetary value is negative.",
    "invalid_review_score": "The review score is outside the inclusive range 1 through 5.",
    "impossible_timestamp": "Timestamp ordering is chronologically impossible.",
}


def _any(conditions: list[Any]):
    from pyspark.sql import functions as F

    return reduce(or_, conditions, F.lit(False))


def classify_rows(frame: Any, dataset: str, raw_contract: dict[str, Any]):
    from pyspark.sql import functions as F

    contract = raw_contract["datasets"][dataset]
    keys = set(contract["business_key"])
    conditions = {"required_key_null": _any([F.col(name).isNull() for name in keys])}
    parse_failures = []
    for field in contract["fields"]:
        name = field["name"]
        if name not in keys and not field["nullable"]:
            parse_failures.append(F.col(name).isNull())
        if field["type"] != "string":
            parse_failures.append(F.col(f"_raw__{name}").isNotNull() & F.col(name).isNull())
    conditions["invalid_parse"] = _any(parse_failures)
    if dataset == "order_items":
        conditions["invalid_monetary_value"] = (F.col("price") < 0) | (F.col("freight_value") < 0)
    elif dataset == "order_payments":
        conditions["invalid_monetary_value"] = F.col("payment_value") < 0
    if dataset == "order_reviews":
        conditions["invalid_review_score"] = ~F.col("review_score").between(1, 5)
        conditions["impossible_timestamp"] = F.col("review_answer_timestamp") < F.col("review_creation_date")
    elif dataset == "orders":
        conditions["impossible_timestamp"] = ((F.col("order_approved_at") < F.col("order_purchase_timestamp"))
                                               | (F.col("order_delivered_carrier_date") < F.col("order_approved_at"))
                                               | (F.col("order_delivered_customer_date") < F.col("order_delivered_carrier_date")))
    codes = sorted(conditions)
    classified = (frame.withColumn("rejection_codes", F.array_compact(F.array(*[F.when(conditions[code], code) for code in codes])))
                  .withColumn("rejection_descriptions", F.array_compact(F.array(*[F.when(conditions[code], REASON_DESCRIPTIONS[code]) for code in codes]))))
    return (classified.filter(F.size("rejection_codes") == 0).drop("rejection_codes", "rejection_descriptions"),
            classified.filter(F.size("rejection_codes") > 0))
