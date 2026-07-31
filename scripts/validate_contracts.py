#!/usr/bin/env python3
"""Validate provider-neutral contract version 1 and a deterministic fixture."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml


CONTRACT_VERSION = 1
SEVERITY_ORDER = ("warning", "reject-row", "reject-dataset", "fail-batch")
REQUIRED_MANIFEST_FIELDS = (
    "batch_id",
    "batch_timestamp",
    "dataset",
    "source_file_id",
    "content_sha256",
)
RAW_DATASETS = {
    "category_translation",
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "products",
    "sellers",
}
CURATED_DATASETS = {
    "cross_state_analysis",
    "dim_customer",
    "dim_date",
    "dim_geography",
    "dim_order_status",
    "dim_product",
    "dim_seller",
    "fact_reviews",
    "fact_sales",
    "monthly_sales",
    "order_status",
    "payment_methods",
    "sales_by_category",
    "sales_by_state",
    "seller_performance",
    "size_analysis",
}
RULE_FILES = {
    "business-keys",
    "fact-grains",
    "incremental-processing",
    "quality-thresholds",
    "referential-integrity",
    "scd2",
}
EXPECTED_STAGES = {
    "aggregations",
    "audit",
    "dimensions",
    "facts",
    "processed",
    "quality",
}
FIELD_PROPERTIES = {"name", "type", "nullable", "invalid_behavior"}
LOGICAL_TYPE = re.compile(
    r"^(?:string|integer|decimal(?:\(\d+,\d+\))?|timestamp|date|boolean)$"
)
PROVIDER_URI = re.compile(r"(?i)(?:abfss?|wasbs?|s3a?)://")
AUDIT_ENUMERATIONS = {
    "replay_outcomes": [
        "initial-load", "new-content", "late-content", "no-op", "retry", "reused-failure"
    ],
    "validator_statuses": [
        "accepted", "accepted-with-warnings", "accepted-with-rejections",
        "reject-dataset", "fail-batch",
    ],
    "pipeline_outcomes": ["PASSED", "PASSED_WITH_REJECTIONS", "FAILED"],
    "terminal_statuses": ["SUCCEEDED", "SKIPPED", "FAILED"],
}
QUALITY_TO_PIPELINE = {
    "warning": "PASSED",
    "reject-row": "PASSED_WITH_REJECTIONS",
    "reject-dataset": "FAILED",
    "fail-batch": "FAILED",
}
VALIDATOR_TO_PIPELINE = {
    "accepted": "PASSED",
    "accepted-with-warnings": "PASSED",
    "accepted-with-rejections": "PASSED_WITH_REJECTIONS",
    "reject-dataset": "FAILED",
    "fail-batch": "FAILED",
}
NONZERO_SEVERITIES = ["reject-dataset", "fail-batch"]
AUDIT_FIELDS = (
    "SubmissionID", "AttemptNumber", "SubmittedAt", "BatchID", "BatchTimestamp",
    "Dataset", "SourceFileID", "ContentSHA256", "ReplayOutcome", "ValidatorStatus",
    "PipelineOutcome", "TerminalStatus", "Retryable", "ReusedSuccessSubmissionID",
    "ReusedFailureSubmissionID",
)


class ContractError(ValueError):
    """A deterministic structural contract failure."""

    def __init__(self, rule: str, dataset: str = "contracts") -> None:
        super().__init__(rule)
        self.rule = rule
        self.dataset = dataset


def _violation(
    dataset: str, row: int | None, rule: str, severity: str
) -> dict[str, Any]:
    return {"dataset": dataset, "row": row, "rule": rule, "severity": severity}


def _failure_report(
    rule: str, fixture: str, dataset: str = "contracts"
) -> dict[str, Any]:
    violation = _violation(dataset, None, rule, "fail-batch")
    return {
        "contract_version": CONTRACT_VERSION,
        "fixture": fixture,
        "status": "fail-batch",
        "severity_counts": {"fail-batch": 1},
        "rejected_rows": 0,
        "dataset_failures": [],
        "violations": [violation],
    }


def _parse(value: str, field: dict[str, Any]) -> Any:
    kind = str(field["type"])
    if kind == "string":
        return value
    if kind == "integer":
        return int(value)
    if kind == "decimal" or kind.startswith("decimal("):
        return Decimal(value)
    if kind == "timestamp":
        return datetime.strptime(value, field.get("format", "%Y-%m-%d %H:%M:%S"))
    raise ValueError(f"unsupported raw type: {kind}")


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise ContractError("unreadable_contract") from error
    if not isinstance(document, dict):
        raise ContractError("invalid_contract")
    return document


def _load_json(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ContractError("unreadable_contract") from error
    if not isinstance(document, dict):
        raise ContractError("invalid_contract")
    return document


def _require(condition: bool, rule: str, dataset: str = "contracts") -> None:
    if not condition:
        raise ContractError(rule, dataset)


def _field_names(definition: dict[str, Any]) -> set[str]:
    return {
        str(field["name"])
        for field in definition["fields"]
        if isinstance(field, dict) and "name" in field
    }


def _validate_catalog(
    stage: str, document: dict[str, Any], expected_datasets: set[str]
) -> None:
    _require(document.get("contract_version") == CONTRACT_VERSION, "invalid_contract_version")
    _require(document.get("stage") == stage, "invalid_contract")
    datasets = document.get("datasets")
    _require(isinstance(datasets, dict), "invalid_contract")
    _require(set(datasets) == expected_datasets, "missing_dataset")

    for dataset, definition in sorted(datasets.items()):
        _require(isinstance(definition, dict), "invalid_contract", dataset)
        fields = definition.get("fields")
        _require(isinstance(fields, list) and fields, "invalid_contract", dataset)
        names: list[str] = []
        source_names: list[str] = []
        for field in fields:
            _require(isinstance(field, dict), "invalid_contract", dataset)
            _require(FIELD_PROPERTIES <= set(field), "invalid_contract", dataset)
            _require(isinstance(field["name"], str) and field["name"], "invalid_contract", dataset)
            _require(
                isinstance(field.get("source_name"), str) and field["source_name"],
                "invalid_contract",
                dataset,
            )
            _require(
                isinstance(field["nullable"], bool)
                and field["invalid_behavior"] in SEVERITY_ORDER,
                "invalid_contract",
                dataset,
            )
            _require(
                bool(LOGICAL_TYPE.fullmatch(str(field["type"]))),
                "invalid_contract",
                dataset,
            )
            names.append(field["name"])
            source_names.append(field["source_name"])
        _require(len(names) == len(set(names)), "invalid_contract", dataset)
        if stage == "raw":
            _require(len(source_names) == len(set(source_names)), "invalid_contract", dataset)
            _require(
                isinstance(definition.get("filename"), str) and definition["filename"],
                "invalid_contract",
                dataset,
            )
        _require(isinstance(definition.get("partition_columns"), list), "invalid_contract", dataset)

        declared = set(names)
        for property_name in ("business_key", "deduplication_key", "grain", "partition_columns"):
            if property_name in definition:
                value = definition[property_name]
                _require(isinstance(value, list), "invalid_contract", dataset)
                _require(set(value) <= declared, "undeclared_field", dataset)

        if stage in {"raw", "processed"}:
            _require(bool(definition.get("business_key")), "invalid_contract", dataset)
        if stage == "processed":
            _require(bool(definition.get("deduplication_key")), "invalid_contract", dataset)
        if stage == "curated":
            _require(
                bool(definition.get("business_key") or definition.get("grain")),
                "invalid_contract",
                dataset,
            )


def _split_reference(reference: Any) -> tuple[str, str]:
    _require(isinstance(reference, str) and reference.count(".") == 1, "invalid_contract")
    dataset, field = reference.split(".", 1)
    return dataset, field


def _validate_audit_catalog(document: dict[str, Any]) -> None:
    _validate_catalog("audit", document, {"batch_submission_audit"})
    _require(
        document.get("persistence_scope") == "contract-and-expected-evidence-only",
        "invalid_contract",
    )
    _require(document.get("enumerations") == AUDIT_ENUMERATIONS, "invalid_contract")
    definition = document["datasets"]["batch_submission_audit"]
    _require(
        definition.get("grain") == ["Dataset", "BatchID", "AttemptNumber"],
        "invalid_contract",
        "batch_submission_audit",
    )
    _require(_field_names(definition) == set(AUDIT_FIELDS), "invalid_contract")
    constraints = definition.get("constraints", {})
    attempt = constraints.get("attempt_number", {})
    _require(
        constraints.get("submission_id") == "globally-unique"
        and attempt.get("starts_at") == 1
        and attempt.get("unique_within") == ["Dataset", "BatchID"]
        and attempt.get("ordering") == "strictly-monotonically-increasing"
        and constraints.get("latest_attempt") == "maximum-attempt-number"
        and constraints.get("retryable") == "explicit-non-null"
        and constraints.get("reused_success_submission_id")
        == "successful-output-reuse-only"
        and constraints.get("reused_failure_submission_id")
        == "deterministic-failure-reuse-only",
        "invalid_contract",
        "batch_submission_audit",
    )
    fields = {field["name"]: field for field in definition["fields"]}
    for name, enumeration in (
        ("ReplayOutcome", "replay_outcomes"),
        ("ValidatorStatus", "validator_statuses"),
        ("PipelineOutcome", "pipeline_outcomes"),
        ("TerminalStatus", "terminal_statuses"),
    ):
        _require(fields[name].get("enumeration") == enumeration, "invalid_contract")
    _require(fields["Retryable"].get("nullable") is False, "invalid_contract")
    _require(fields["ReusedSuccessSubmissionID"].get("nullable") is True, "invalid_contract")
    _require(fields["ReusedFailureSubmissionID"].get("nullable") is True, "invalid_contract")


def _validate_rules(
    rules: dict[str, dict[str, Any]], catalogs: dict[str, dict[str, Any]]
) -> dict[str, str]:
    for document in rules.values():
        _require(document.get("contract_version") == CONTRACT_VERSION, "invalid_contract_version")
        document_rules = document.get("rules")
        _require(isinstance(document_rules, list) and document_rules, "invalid_contract")
        for rule in document_rules:
            _require(isinstance(rule, dict) and isinstance(rule.get("id"), str), "invalid_contract")
            _require(rule.get("severity") in SEVERITY_ORDER, "invalid_severity")

    quality_rules = rules["quality-thresholds"].get("rules")
    _require(isinstance(quality_rules, list), "invalid_contract")
    severities = {rule["id"]: rule["severity"] for rule in quality_rules}
    _require(len(severities) == len(quality_rules), "invalid_contract")
    quality = rules["quality-thresholds"]
    _require(quality.get("pipeline_outcomes") == QUALITY_TO_PIPELINE, "invalid_contract")
    _require(quality.get("nonzero_validator_severities") == NONZERO_SEVERITIES, "invalid_contract")
    _require(quality.get("optional_datasets") == [], "invalid_contract")

    business_datasets = rules["business-keys"]["rules"][0].get("datasets")
    _require(set(business_datasets or []) == RAW_DATASETS, "missing_dataset")
    deduplication = rules["business-keys"].get("deduplication", {})
    for policy in ("exact_duplicate", "conflicting_key"):
        _require(
            isinstance(deduplication.get(policy), dict)
            and deduplication[policy].get("severity") in SEVERITY_ORDER,
            "invalid_severity",
        )
    payment = rules["business-keys"].get("representative_payment", {})
    _require(
        payment.get("order_by") == ["payment_sequential", "payment_type"]
        and payment.get("select") == "first"
        and payment.get("usage") == "descriptive-item-price-attribution"
        and payment.get("tendered_value_allocation") == "excluded"
        and payment.get("future_payment_fact_grain")
        == ["order_id", "payment_sequential"],
        "invalid_contract",
        "order_payments",
    )

    raw = catalogs["raw"]["datasets"]
    processed = catalogs["processed"]["datasets"]
    for dataset in sorted(RAW_DATASETS):
        _require(
            processed[dataset].get("business_key") == raw[dataset].get("business_key")
            and processed[dataset].get("deduplication_key")
            == processed[dataset].get("business_key"),
            "invalid_contract",
            dataset,
        )

    orders = processed["orders"]
    order_fields = _field_names(orders)
    status_derivation = orders.get("status_derivation", {})
    _require(
        status_derivation.get("target") == "order_status"
        and status_derivation.get("source_field") == "order_status"
        and status_derivation.get("default") == "CREATED",
        "invalid_contract",
        "orders",
    )
    terminal_values = status_derivation.get("terminal_source_values")
    _require(
        terminal_values
        == {
            "canceled": "CANCELLED",
            "cancelled": "CANCELLED",
            "unavailable": "UNAVAILABLE",
        },
        "invalid_contract",
        "orders",
    )
    precedence = status_derivation.get("timestamp_precedence")
    _require(
        precedence
        == [
            {"when_present": "order_delivered_customer_date", "value": "DELIVERED"},
            {"when_present": "order_delivered_carrier_date", "value": "SHIPPED"},
            {"when_present": "order_approved_at", "value": "APPROVED"},
        ],
        "invalid_contract",
        "orders",
    )
    delivery_metrics = orders.get("delivery_metrics")
    _require(
        isinstance(delivery_metrics, dict)
        and set(delivery_metrics)
        == {
            "shipping_time_days",
            "delivery_time_days",
            "total_delivery_time_days",
            "delay_days",
        },
        "invalid_contract",
        "orders",
    )
    for target, sources in delivery_metrics.items():
        _require(
            target in order_fields
            and isinstance(sources, list)
            and len(sources) == 2
            and set(sources) <= order_fields,
            "undeclared_field",
            "orders",
        )

    curated = catalogs["curated"]["datasets"]
    for rule in rules["fact-grains"]["rules"]:
        dataset = rule.get("dataset")
        _require(dataset in curated, "missing_dataset", str(dataset))
        fields = rule.get("fields")
        _require(isinstance(fields, list), "invalid_contract", dataset)
        _require(set(fields) <= _field_names(curated[dataset]), "undeclared_field", dataset)
        _require(fields == curated[dataset].get("grain"), "invalid_contract", dataset)

    for rule in rules["referential-integrity"]["rules"]:
        child_dataset, child_field = _split_reference(rule.get("child"))
        parent_dataset, parent_field = _split_reference(rule.get("parent"))
        _require(child_dataset in raw and parent_dataset in raw, "missing_dataset")
        _require(child_field in _field_names(raw[child_dataset]), "undeclared_field", child_dataset)
        _require(parent_field in _field_names(raw[parent_dataset]), "undeclared_field", parent_dataset)
        _require(
            raw[parent_dataset].get("business_key") == [parent_field],
            "invalid_contract",
            parent_dataset,
        )

    incremental = rules["incremental-processing"]
    _require(
        incremental.get("manifest_fields") == list(REQUIRED_MANIFEST_FIELDS),
        "invalid_contract",
    )
    constraints = incremental.get("manifest_constraints", {})
    _require(
        constraints.get("batch_timestamp_timezone") == "UTC"
        and constraints.get("content_hash") == "SHA-256",
        "invalid_contract",
    )
    _require(
        incremental.get("latest_attempt")
        == {
            "group_by": ["dataset", "batch_id"],
            "select_by": "maximum-attempt-number",
            "collection_order_authoritative": False,
        }
        and incremental.get("replay_classification")
        == {
            "latest_succeeded": "no-op",
            "latest_skipped_after_successful_reuse": "no-op",
            "latest_failed_retryable": "retry",
            "latest_failed_not_retryable": "reused-failure",
            "latest_failed_retryability_absent": "reused-failure",
        }
        and incremental.get("retry", {}).get("requires_explicit_retryable") is True
        and incremental.get("audit_evidence")
        == {"every_submission": True, "durable_persistence": "deferred-to-provider-runtime"},
        "invalid_contract",
    )
    changed_content = next(
        (rule for rule in incremental["rules"] if rule["id"] == "changed_content_same_batch"),
        {},
    )
    _require(
        changed_content.get("severity") == "fail-batch"
        and changed_content.get("outcome") == "fail-batch",
        "invalid_contract",
    )

    scd2 = rules["scd2"]
    scd_rules = scd2.get("rules", [])
    _require(
        scd2.get("mode") == "snapshot-only"
        and scd2.get("historical_runtime_destination") == "historical-warehouse-runtime"
        and scd2.get("first_planned_implementation") == "Redshift Serverless"
        and len(scd_rules) == 1
        and scd_rules[0].get("enabled") is False
        and scd_rules[0].get("deferred_to") == "historical-warehouse-runtime"
        and scd2.get("deferred")
        == [
            "change-detection",
            "historical-effective-boundaries",
            "late-arriving-dimension-resolution",
            "fact-time-resolution",
        ],
        "invalid_contract",
    )
    return severities


def _json_pointer(document: Any, pointer: str) -> Any:
    _require(isinstance(pointer, str) and pointer.startswith("/"), "invalid_contract")
    current = document
    for raw_part in pointer.lstrip("/").split("/"):
        part = raw_part.replace("~1", "/").replace("~0", "~")
        _require(isinstance(current, dict) and part in current, "invalid_contract")
        current = current[part]
    return current


def _count_snapshot_value(value: Any) -> int:
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict) and isinstance(value.get("row_count"), int):
        return value["row_count"]
    raise ContractError("invalid_contract")


def _validate_expected_audit(
    document: dict[str, Any], audit_catalog: dict[str, Any]
) -> None:
    _require(document.get("schema") == "batch_submission_audit", "invalid_contract")
    _require(
        document.get("evidence_scope") == "contract-and-expected-evidence-only"
        and document.get("durable_persistence") is False,
        "invalid_contract",
    )
    _require(document.get("enumerations") == AUDIT_ENUMERATIONS, "invalid_contract")
    records = document.get("records")
    _require(isinstance(records, list) and records, "invalid_contract")
    fields = _field_names(audit_catalog["datasets"]["batch_submission_audit"])
    submission_ids: set[str] = set()
    records_by_id: dict[str, dict[str, Any]] = {}
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}

    for record in records:
        _require(isinstance(record, dict) and set(record) == fields, "invalid_contract")
        submission_id = record.get("SubmissionID")
        _require(
            isinstance(submission_id, str)
            and submission_id
            and submission_id not in submission_ids,
            "invalid_contract",
            "batch_submission_audit",
        )
        submission_ids.add(submission_id)
        records_by_id[submission_id] = record
        attempt = record.get("AttemptNumber")
        _require(isinstance(attempt, int) and not isinstance(attempt, bool) and attempt >= 1, "invalid_contract")
        _require(isinstance(record.get("Retryable"), bool), "invalid_contract")
        _require(
            isinstance(record.get("ContentSHA256"), str)
            and re.fullmatch(r"[0-9a-fA-F]{64}", record["ContentSHA256"]) is not None,
            "invalid_contract",
        )
        for field, domain in (
            ("ReplayOutcome", "replay_outcomes"),
            ("ValidatorStatus", "validator_statuses"),
            ("PipelineOutcome", "pipeline_outcomes"),
            ("TerminalStatus", "terminal_statuses"),
        ):
            _require(record.get(field) in AUDIT_ENUMERATIONS[domain], "invalid_contract")
        _require(
            record["PipelineOutcome"] == VALIDATOR_TO_PIPELINE[record["ValidatorStatus"]],
            "invalid_contract",
        )
        if record["PipelineOutcome"] == "FAILED":
            _require(record["TerminalStatus"] == "FAILED", "invalid_contract")
        else:
            _require(record["TerminalStatus"] in {"SUCCEEDED", "SKIPPED"}, "invalid_contract")
        _require(
            not (
                record["ReusedSuccessSubmissionID"] is not None
                and record["ReusedFailureSubmissionID"] is not None
            ),
            "invalid_contract",
        )
        group = (str(record["Dataset"]), str(record["BatchID"]))
        groups.setdefault(group, []).append(record)

    for attempts in groups.values():
        numbers = sorted(record["AttemptNumber"] for record in attempts)
        _require(numbers[0] == 1 and len(numbers) == len(set(numbers)), "invalid_contract")
        _require(all(left < right for left, right in zip(numbers, numbers[1:])), "invalid_contract")
        latest = max(attempts, key=lambda record: record["AttemptNumber"])
        _require(latest["AttemptNumber"] == max(numbers), "invalid_contract")

    saw_success_reuse = False
    saw_failure_reuse = False
    for record in records:
        success_id = record["ReusedSuccessSubmissionID"]
        failure_id = record["ReusedFailureSubmissionID"]
        if success_id is not None:
            prior = records_by_id.get(success_id)
            _require(prior is not None, "invalid_contract")
            _require(
                record["ReplayOutcome"] == "no-op"
                and record["TerminalStatus"] == "SKIPPED"
                and prior["TerminalStatus"] == "SUCCEEDED"
                and prior["PipelineOutcome"] in {"PASSED", "PASSED_WITH_REJECTIONS"}
                and prior["Dataset"] == record["Dataset"]
                and prior["BatchID"] == record["BatchID"]
                and prior["AttemptNumber"] < record["AttemptNumber"]
                and prior["ContentSHA256"] == record["ContentSHA256"],
                "invalid_contract",
            )
            saw_success_reuse = True
        if failure_id is not None:
            prior = records_by_id.get(failure_id)
            _require(prior is not None, "invalid_contract")
            _require(
                record["ReplayOutcome"] == "reused-failure"
                and record["PipelineOutcome"] == "FAILED"
                and prior["PipelineOutcome"] == "FAILED"
                and prior["Retryable"] is False
                and prior["Dataset"] == record["Dataset"]
                and prior["BatchID"] == record["BatchID"]
                and prior["AttemptNumber"] < record["AttemptNumber"]
                and prior["ContentSHA256"] == record["ContentSHA256"],
                "invalid_contract",
            )
            saw_failure_reuse = True
    _require(saw_success_reuse and saw_failure_reuse, "invalid_contract")


def _validate_expected_outputs(
    contracts_root: Path, catalogs: dict[str, dict[str, Any]]
) -> None:
    expected_root = contracts_root / "expected"
    baseline_snapshot = _load_json(expected_root / "baseline_snapshot.json")
    _require(
        baseline_snapshot.get("contract_version") == CONTRACT_VERSION,
        "invalid_contract_version",
    )

    stage_paths = {
        path.parent.name: path
        for path in expected_root.glob("*/baseline.json")
        if path.parent.name in EXPECTED_STAGES
    }
    _require(set(stage_paths) == EXPECTED_STAGES, "missing_dataset")
    for stage, path in sorted(stage_paths.items()):
        document = _load_json(path)
        _require(document.get("contract_version") == CONTRACT_VERSION, "invalid_contract_version")
        _require(document.get("fixture") == "baseline", "invalid_contract")
        source = path.parent / str(document.get("source_snapshot", ""))
        _require(source.resolve() == (expected_root / "baseline_snapshot.json").resolve(), "invalid_contract")
        _require(source.is_file(), "unreadable_contract")

        if "json_pointer" in document:
            target = _json_pointer(baseline_snapshot, document["json_pointer"])
            for dataset, count in document.get("row_counts", {}).items():
                _require(isinstance(target, dict) and dataset in target, "missing_dataset", dataset)
                _require(_count_snapshot_value(target[dataset]) == count, "invalid_contract", dataset)

        if stage == "facts":
            for dataset, expectation in document.get("datasets", {}).items():
                _require(dataset in catalogs["curated"]["datasets"], "missing_dataset", dataset)
                target = _json_pointer(baseline_snapshot, expectation["json_pointer"])
                _require(_count_snapshot_value(target) == expectation["row_count"], "invalid_contract", dataset)
                _require(
                    expectation["grain"]
                    == catalogs["curated"]["datasets"][dataset].get("grain"),
                    "invalid_contract",
                    dataset,
                )

        if stage == "quality":
            target = _json_pointer(baseline_snapshot, document["json_pointer"])
            _require(target.get("rejected_rows") == document.get("rejected_rows"), "invalid_contract")
        if stage == "audit":
            _validate_expected_audit(document, catalogs["audit"])


def _load_contract_package(contracts_root: Path) -> dict[str, Any]:
    for namespace in ("schemas", "rules", "expected"):
        for path in sorted((contracts_root / namespace).rglob("*")):
            if path.is_file() and path.suffix in {".yaml", ".json"}:
                try:
                    text = path.read_text(encoding="utf-8")
                except OSError as error:
                    raise ContractError("unreadable_contract") from error
                _require(not PROVIDER_URI.search(text), "provider_specific_uri")

    catalogs = {
        stage: _load_yaml(contracts_root / "schemas" / stage / "datasets.yaml")
        for stage in ("raw", "processed", "curated")
    }
    catalogs["audit"] = _load_yaml(contracts_root / "schemas" / "audit" / "datasets.yaml")
    _validate_catalog("raw", catalogs["raw"], RAW_DATASETS)
    _validate_catalog("processed", catalogs["processed"], RAW_DATASETS)
    _validate_catalog("curated", catalogs["curated"], CURATED_DATASETS)
    _validate_audit_catalog(catalogs["audit"])

    rule_paths = {
        path.stem: path for path in (contracts_root / "rules").glob("*.yaml")
    }
    _require(set(rule_paths) == RULE_FILES, "missing_dataset")
    rules = {name: _load_yaml(path) for name, path in sorted(rule_paths.items())}
    severities = _validate_rules(rules, catalogs)
    _validate_expected_outputs(contracts_root, catalogs)
    return {"catalogs": catalogs, "rules": rules, "severities": severities}


def validate_fixture(contracts_root: Path, fixture: str) -> dict[str, Any]:
    """Return a stable validation report; row quarantine does not fail the batch."""
    contracts_root = Path(contracts_root)
    try:
        package = _load_contract_package(contracts_root)
    except ContractError as error:
        return _failure_report(error.rule, fixture, error.dataset)

    catalog = package["catalogs"]["raw"]
    rules = package["rules"]
    severities = package["severities"]
    fixture_root = contracts_root / "fixtures" / fixture / "raw"
    violations: list[dict[str, Any]] = []
    rows_by_dataset: dict[str, list[dict[str, str]]] = {}
    keys_by_dataset: dict[str, set[tuple[str, ...]]] = {}

    deduplication = rules["business-keys"]["deduplication"]
    exact_duplicate_severity = deduplication["exact_duplicate"]["severity"]
    conflict_severity = deduplication["conflicting_key"]["severity"]

    for dataset in sorted(catalog["datasets"]):
        definition = catalog["datasets"][dataset]
        source = fixture_root / definition["filename"]
        if not source.is_file():
            violations.append(
                _violation(dataset, None, "missing_dataset", severities["missing_dataset"])
            )
            continue
        try:
            with source.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                required_headers = [field["source_name"] for field in definition["fields"]]
                missing = sorted(set(required_headers) - set(reader.fieldnames or []))
                if missing:
                    violations.append(
                        _violation(dataset, None, "missing_column", severities["missing_column"])
                    )
                    continue
                rows = list(reader)
        except (OSError, csv.Error):
            violations.append(
                _violation(dataset, None, "missing_dataset", severities["missing_dataset"])
            )
            continue

        rows_by_dataset[dataset] = rows
        business_key = definition["business_key"]
        seen: dict[tuple[str, ...], tuple[str, ...]] = {}
        valid_keys: set[tuple[str, ...]] = set()
        fields = definition["fields"]
        source_names = [field["source_name"] for field in fields]

        for number, row in enumerate(rows, 2):
            row_tuple = tuple(row[name] for name in source_names)
            key = tuple(row[name] for name in business_key)
            if key in seen:
                severity = (
                    exact_duplicate_severity if seen[key] == row_tuple else conflict_severity
                )
                rule = (
                    "exact_duplicate"
                    if severity == exact_duplicate_severity
                    else "conflicting_business_key"
                )
                violations.append(_violation(dataset, number, rule, severity))
                continue
            seen[key] = row_tuple

            parsed: dict[str, Any] = {}
            rejected = False
            for field in fields:
                name = field["source_name"]
                value = row[name]
                if value == "":
                    if not field["nullable"]:
                        violations.append(
                            _violation(
                                dataset,
                                number,
                                "required_key_null",
                                severities["required_key_null"],
                            )
                        )
                        rejected = True
                    continue
                try:
                    parsed[name] = _parse(value, field)
                except (ValueError, InvalidOperation):
                    violations.append(
                        _violation(
                            dataset,
                            number,
                            "invalid_parse",
                            severities["invalid_parse"],
                        )
                    )
                    rejected = True

            for name in ("price", "freight_value", "payment_value"):
                if name in parsed and parsed[name] < 0:
                    violations.append(
                        _violation(
                            dataset,
                            number,
                            "invalid_monetary_value",
                            severities["invalid_monetary_value"],
                        )
                    )
                    rejected = True
            if dataset == "order_reviews" and "review_score" in parsed:
                if not 1 <= parsed["review_score"] <= 5:
                    violations.append(
                        _violation(
                            dataset,
                            number,
                            "invalid_review_score",
                            severities["invalid_review_score"],
                        )
                    )
                    rejected = True

            timestamp_pairs: tuple[tuple[str, str], ...] = ()
            if dataset == "orders":
                timestamp_pairs = (
                    ("order_purchase_timestamp", "order_approved_at"),
                    ("order_approved_at", "order_delivered_carrier_date"),
                    ("order_delivered_carrier_date", "order_delivered_customer_date"),
                    ("order_purchase_timestamp", "order_estimated_delivery_date"),
                )
            elif dataset == "order_reviews":
                timestamp_pairs = (("review_creation_date", "review_answer_timestamp"),)
            if any(
                left in parsed and right in parsed and parsed[right] < parsed[left]
                for left, right in timestamp_pairs
            ):
                violations.append(
                    _violation(
                        dataset,
                        number,
                        "impossible_timestamp",
                        severities["impossible_timestamp"],
                    )
                )
                rejected = True

            if not rejected and all(key):
                valid_keys.add(key)
        keys_by_dataset[dataset] = valid_keys

    for reference in rules["referential-integrity"]["rules"]:
        child, child_field = _split_reference(reference["child"])
        parent, parent_field = _split_reference(reference["parent"])
        if child not in rows_by_dataset or parent not in keys_by_dataset:
            continue
        parent_values = {key[0] for key in keys_by_dataset[parent]}
        for number, row in enumerate(rows_by_dataset[child], 2):
            if row[child_field] and row[child_field] not in parent_values:
                violations.append(
                    _violation(
                        child,
                        number,
                        "orphan_reference",
                        reference["severity"],
                    )
                )

    violations.sort(
        key=lambda item: (
            SEVERITY_ORDER.index(item["severity"]),
            item["dataset"],
            item["row"] or 0,
            item["rule"],
        )
    )
    counts = Counter(item["severity"] for item in violations)
    severity_counts = {
        severity: counts[severity] for severity in SEVERITY_ORDER if counts[severity]
    }
    rejected_rows = {
        (item["dataset"], item["row"])
        for item in violations
        if item["severity"] == "reject-row" and item["row"] is not None
    }
    dataset_failures = sorted(
        {
            item["dataset"]
            for item in violations
            if item["severity"] == "reject-dataset"
        }
    )
    if counts["fail-batch"]:
        status = "fail-batch"
    elif counts["reject-dataset"]:
        status = "reject-dataset"
    elif counts["reject-row"]:
        status = "accepted-with-rejections"
    elif counts["warning"]:
        status = "accepted-with-warnings"
    else:
        status = "accepted"
    return {
        "contract_version": CONTRACT_VERSION,
        "fixture": fixture,
        "status": status,
        "severity_counts": severity_counts,
        "rejected_rows": len(rejected_rows),
        "dataset_failures": dataset_failures,
        "violations": violations,
    }


def classify_incremental_manifest(
    manifest: dict[str, Any], history: list[dict[str, Any]]
) -> str:
    """Classify an immutable provider-neutral batch manifest."""
    if any(not manifest.get(field) for field in REQUIRED_MANIFEST_FIELDS):
        return "fail-batch"
    if not re.fullmatch(r"[0-9a-fA-F]{64}", str(manifest["content_sha256"])):
        return "fail-batch"
    audit_names = {
        "batch_id": "BatchID",
        "batch_timestamp": "BatchTimestamp",
        "dataset": "Dataset",
        "source_file_id": "SourceFileID",
        "content_sha256": "ContentSHA256",
    }

    def value(item: dict[str, Any], field: str) -> Any:
        return item.get(audit_names[field], item.get(field))

    same_batch = [
        item
        for item in history
        if value(item, "batch_id") == manifest["batch_id"]
        and value(item, "dataset") == manifest["dataset"]
    ]
    if same_batch:
        if any(
            any(value(item, field) != manifest.get(field) for field in REQUIRED_MANIFEST_FIELDS)
            for item in same_batch
        ):
            return "fail-batch"
        attempts = [item.get("AttemptNumber", item.get("attempt_number")) for item in same_batch]
        if any(not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 1 for attempt in attempts):
            return "fail-batch"
        if len(attempts) != len(set(attempts)):
            return "fail-batch"
        latest = max(same_batch, key=lambda item: item.get("AttemptNumber", item.get("attempt_number")))
        terminal = latest.get("TerminalStatus", str(latest.get("status", "")).upper())
        replay = latest.get("ReplayOutcome", latest.get("replay_outcome"))
        if terminal == "SUCCEEDED":
            return "no-op"
        if terminal == "SKIPPED" and replay == "no-op" and latest.get("ReusedSuccessSubmissionID"):
            return "no-op"
        if terminal == "FAILED":
            return "retry" if latest.get("Retryable") is True else "reused-failure"
        if terminal == "SKIPPED" and replay == "reused-failure":
            return "reused-failure"
        return "fail-batch"
    succeeded = [
        item
        for item in history
        if item.get("TerminalStatus", str(item.get("status", "")).upper()) == "SUCCEEDED"
        and value(item, "dataset") == manifest["dataset"]
    ]
    if any(value(item, "content_sha256") == manifest["content_sha256"] for item in succeeded):
        return "no-op"
    if not succeeded:
        return "initial-load"
    prior_timestamps = [
        item.get("max_event_timestamp")
        for item in succeeded
        if item.get("max_event_timestamp")
    ]
    current = manifest.get("max_event_timestamp")
    if current and prior_timestamps and current < max(prior_timestamps):
        return "late-content"
    return "new-content"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", default="baseline")
    parser.add_argument(
        "--contracts-root",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "contracts",
    )
    args = parser.parse_args()
    report = validate_fixture(args.contracts_root, args.fixture)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if report["status"] in {"reject-dataset", "fail-batch"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
