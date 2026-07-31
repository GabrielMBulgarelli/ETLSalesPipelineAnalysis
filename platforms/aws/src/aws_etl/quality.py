"""Batch-level processed-data quality orchestration."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

from aws_etl.integrity import apply_reference_rule, split_rows, validate_dataset


@dataclass(frozen=True)
class QualityResult:
    valid: dict[str, Any]
    rejected: dict[str, Any]
    summary: dict[str, Any]


def _reason_counts(frame: Any) -> dict[str, int]:
    from pyspark.sql import functions as F

    return {
        row["reason"]: int(row["count"])
        for row in frame.select(F.explode("rejection_codes").alias("reason")).groupBy("reason").count().collect()
    }


def validate_processed(
    frames: dict[str, Any],
    processed_contract: dict[str, Any],
    quality_contract: dict[str, Any],
    reference_contract: dict[str, Any],
    exact_duplicate_warnings: int = 0,
) -> QualityResult:
    """Validate all datasets, resolving parents before children."""
    expected = tuple(processed_contract["datasets"])
    missing = sorted(set(expected) - set(frames))
    if missing:
        raise ValueError(f"missing_dataset: {', '.join(missing)}")

    classified = {
        name: validate_dataset(frames[name], name, processed_contract).cache()
        for name in expected
    }
    valid: dict[str, Any] = {}
    rejected: dict[str, Any] = {}
    rules_by_child: dict[str, list[dict[str, Any]]] = {}
    for rule in reference_contract["rules"]:
        child_dataset, child_field = rule["child"].split(".", 1)
        parent_dataset, parent_field = rule["parent"].split(".", 1)
        rules_by_child.setdefault(child_dataset, []).append(
            {"child_field": child_field, "parent_dataset": parent_dataset, "parent_field": parent_field}
        )

    # This ordering guarantees every child sees only the parent's accepted keys.
    resolution_order = (
        "customers", "products", "sellers", "geolocation", "category_translation",
        "orders", "order_items", "order_payments", "order_reviews",
    )
    for dataset in resolution_order:
        current = classified[dataset]
        for rule in rules_by_child.get(dataset, []):
            current = apply_reference_rule(
                current,
                valid[rule["parent_dataset"]],
                rule["child_field"],
                rule["parent_field"],
            )
        valid[dataset], rejected[dataset] = split_rows(current)
        valid[dataset] = valid[dataset].cache()
        rejected[dataset] = rejected[dataset].cache()

    severity_for = {rule["id"]: rule["severity"] for rule in quality_contract["rules"]}
    reason_counts: Counter[str] = Counter()
    dataset_level_failures: Counter[str] = Counter()
    rejected_rows = 0
    dataset_summaries: dict[str, Any] = {}
    for dataset in expected:
        total = frames[dataset].count()
        rejected_count = rejected[dataset].count()
        counts = _reason_counts(rejected[dataset]) if rejected_count else {}
        rejected_rows += rejected_count
        reason_counts.update(counts)
        for reason in counts:
            if severity_for[reason] == "reject-dataset":
                dataset_level_failures[reason] += 1
        dataset_summaries[dataset] = {
            "input_rows": total,
            "valid_rows": total - rejected_count,
            "rejected_rows": rejected_count,
            "reasons": dict(sorted(counts.items())),
        }

    severity_counts = Counter({"warning": int(exact_duplicate_warnings)})
    for reason, count in reason_counts.items():
        severity = severity_for[reason]
        # Dataset-blocking violations are one gate failure per affected dataset,
        # not one rejected row per duplicate member.
        severity_counts[severity] += count if severity == "reject-row" else dataset_level_failures[reason]
    if severity_counts["fail-batch"] or severity_counts["reject-dataset"]:
        outcome = "FAILED"
    elif rejected_rows:
        outcome = "PASSED_WITH_REJECTIONS"
    else:
        outcome = "PASSED"

    summary = {
        "outcome": outcome,
        "input_rows": sum(item["input_rows"] for item in dataset_summaries.values()),
        "valid_rows": sum(item["valid_rows"] for item in dataset_summaries.values()),
        "rejected_rows": rejected_rows,
        "severity_counts": {
            severity: int(severity_counts[severity])
            for severity in ("warning", "reject-row", "reject-dataset", "fail-batch")
        },
        "reason_counts": dict(sorted(reason_counts.items())),
        "datasets": dataset_summaries,
    }
    return QualityResult(valid=valid, rejected=rejected, summary=summary)
