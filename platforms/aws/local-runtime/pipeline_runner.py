#!/usr/bin/env python3
"""Authoritative local Phase 7 runner for the complete AWS ETL pipeline."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from time import sleep
from typing import Any, Callable
from uuid import uuid4

from aws_etl.audit import audit_key, create_audit_record
from aws_etl.config import AwsEtlConfig, load_config
from aws_etl.manifests import (
    SOURCE_FILES,
    create_manifest,
    discover_source_files,
    immutable_identity_matches,
    inspect_source_file,
    manifest_key,
)
from aws_etl.orchestration import (
    PROVIDER_EVIDENCE_KEYS,
    RETRY_BACKOFF_RATE,
    RETRY_INTERVAL_SECONDS,
    RETRY_MAX_ATTEMPTS,
    TRANSIENT_GLUE_ERRORS,
    acquire_execution_claim,
    evidence_sha256,
    immutable_put_json,
    is_transient_glue_error,
)
from aws_etl.replay import ReplayDecision, classify_replay, next_attempt_number
from aws_etl.storage import (
    ensure_bucket,
    ensure_prefixes,
    expected_prefixes,
    get_json,
    list_json_documents,
    object_exists,
    put_json_immutable,
    s3_client,
    upload_file_if_absent,
)
from aws_etl.writers import terminal_summary_key, verify_marker_outputs


ROOT = Path(__file__).resolve().parents[3]
RUN_GLUE = Path(__file__).with_name("run_glue_job.sh")
BATCH_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
STAGES = ("ProcessRaw", "ValidateProcessed", "BuildCurated")


class DeterministicPipelineFailure(RuntimeError):
    def __init__(self, stage: str, code: str, message: str, job_run_id: str | None = None):
        super().__init__(message)
        self.stage, self.code, self.job_run_id = stage, code, job_run_id


class GlueExecutionFailure(RuntimeError):
    def __init__(self, stage: str, code: str, message: str, job_run_id: str | None):
        super().__init__(message)
        self.stage, self.code, self.job_run_id = stage, code, job_run_id


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def normalize_timestamp(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != datetime.now(UTC).utcoffset():
        raise ValueError("batch timestamp must include the UTC timezone")
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def new_batch_id() -> str:
    return f"batch-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%fZ')}-{uuid4().hex[:8]}"


def _existing_timestamp(documents: list[dict[str, Any]], batch_id: str) -> str | None:
    values = {
        str(item.get("BatchTimestamp", item.get("batch_timestamp")))
        for item in documents
        if item.get("BatchID", item.get("batch_id")) == batch_id
        and item.get("BatchTimestamp", item.get("batch_timestamp"))
    }
    if len(values) > 1:
        raise DeterministicPipelineFailure("InitializeBatch", "CONFLICTING_BATCH_TIMESTAMP", "batch has conflicting immutable timestamps")
    return next(iter(values), None)


def _storage(config: AwsEtlConfig) -> dict[str, str]:
    return {
        "bucket": config.bucket,
        "raw_prefix": config.raw_prefix,
        "processed_prefix": config.processed_prefix,
        "curated_prefix": config.curated_prefix,
        "rejected_prefix": config.rejected_prefix,
        "quality_prefix": config.quality_prefix,
        "staging_prefix": config.staging_prefix,
        "manifest_prefix": config.manifest_prefix,
        "audit_prefix": config.audit_prefix,
    }


def _records(manifest: dict[str, Any], decision: ReplayDecision, attempt: int, submission_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    completion = create_audit_record(manifest, decision, attempt)
    completion["SubmissionID"] = submission_id
    failure = dict(completion)
    failure.update({"ValidatorStatus": "fail-batch", "PipelineOutcome": "FAILED", "TerminalStatus": "FAILED", "Retryable": False})
    return completion, failure


def initialize_envelope(
    client: Any,
    config: AwsEtlConfig,
    dataset_dir: Path,
    batch_id: str,
    batch_timestamp: str | None,
    execution_id: str,
) -> tuple[dict[str, Any], list[tuple[Any, dict[str, Any], str, ReplayDecision]]]:
    if BATCH_ID_PATTERN.fullmatch(batch_id) is None:
        raise ValueError("batch ID must be 1-128 characters using letters, numbers, period, underscore, or hyphen")
    sources = [inspect_source_file(path) for path in discover_source_files(dataset_dir)]
    ensure_bucket(client, config)
    ensure_prefixes(client, config.bucket, expected_prefixes(SOURCE_FILES.values()))
    history = list_json_documents(client, config.bucket, config.audit_prefix)
    manifest_history = list_json_documents(client, config.bucket, config.manifest_prefix)
    timestamp = normalize_timestamp(batch_timestamp or _existing_timestamp(history + manifest_history, batch_id) or utc_now())

    work: list[tuple[Any, dict[str, Any], str, ReplayDecision]] = []
    for source in sources:
        manifest = create_manifest(source, batch_id, timestamp, config.pipeline_version)
        key = manifest_key(config.manifest_prefix, source.dataset, batch_id)
        manifest["manifest_object_path"] = key
        existing = get_json(client, config.bucket, key)
        decision = (
            ReplayDecision("fail-batch", reason="immutable manifest content changed under an existing batch ID")
            if existing is not None and not immutable_identity_matches(existing, manifest)
            else classify_replay(manifest, history)
        )
        work.append((source, manifest, key, decision))

    if any(decision.outcome == "fail-batch" for *_, decision in work):
        work = [
            (source, manifest, key, decision if decision.outcome in {"fail-batch", "reused-failure", "no-op"} else ReplayDecision("fail-batch", reason="batch aborted by immutable identity failure"))
            for source, manifest, key, decision in work
        ]

    submissions: list[dict[str, Any]] = []
    for _, manifest, _, decision in work:
        attempt = next_attempt_number(str(manifest["dataset"]), batch_id, history)
        submission_id = str(uuid4())
        completion, failure = _records(manifest, decision, attempt, submission_id)
        key = audit_key(config.audit_prefix, completion)
        submissions.append({
            "dataset": manifest["dataset"],
            "submission_id": submission_id,
            "attempt_number": attempt,
            "manifest": manifest,
            "replay_outcome": decision.outcome,
            "completion_record": completion,
            "failure_record": failure,
            "completion_audit_key": key,
            "failure_audit_key": key,
            "completion_evidence_hash": evidence_sha256(completion),
        })

    outcomes = {submission["replay_outcome"] for submission in submissions}
    replay_class = "REUSED_FAILURE" if outcomes & {"reused-failure", "fail-batch"} else "NO_OP" if outcomes == {"no-op"} else "CONTINUE"
    claim_attempt = max(submission["attempt_number"] for submission in submissions)
    identity = {str(manifest["dataset"]): str(manifest["content_sha256"]) for _, manifest, _, _ in sorted(work, key=lambda item: str(item[1]["dataset"]))}
    claim = {"batch_id": batch_id, "attempt": claim_attempt, "execution_id": execution_id, "manifest_content_sha256": identity}
    claim_key = f"{config.staging_prefix}orchestration/claims/batch_id={batch_id}/attempt={claim_attempt:06d}/claim.json"
    envelope = {
        "batch": {"id": batch_id, "timestamp": timestamp, "contract_version": config.contract_version, "pipeline_version": config.pipeline_version},
        "storage": _storage(config),
        "submissions": submissions,
        "orchestration": {
            "execution_id": execution_id,
            "replay_class": replay_class,
            "claim_key": claim_key,
            "claim": claim,
            "claim_hash": evidence_sha256(claim),
            "validation_marker_key": terminal_summary_key(config, batch_id, "validation"),
            "stages": {},
        },
    }
    return envelope, work


def _validate_marker(
    client: Any,
    config: AwsEtlConfig,
    marker: dict[str, Any],
    identity: dict[str, str],
    batch_id: str,
    *,
    expected_outputs: set[str],
) -> None:
    verify_marker_outputs(client, config, marker, identity, batch_id, int(config.contract_version))
    if set(marker["produced_datasets"]) != expected_outputs:
        raise DeterministicPipelineFailure("ClassifyReplay", "MARKER_OUTPUT_MISMATCH", "completion marker declares unexpected outputs")


def next_stage(client: Any, config: AwsEtlConfig, envelope: dict[str, Any]) -> str:
    batch_id = envelope["batch"]["id"]
    identity = {item["dataset"]: item["manifest"]["content_sha256"] for item in envelope["submissions"]}
    processed_key = f"{config.quality_prefix}batch_id={batch_id}/processed-summary.json"
    validation_key = terminal_summary_key(config, batch_id, "validation")
    curation_key = terminal_summary_key(config, batch_id, "curation")
    processed, validation, curation = (get_json(client, config.bucket, key) for key in (processed_key, validation_key, curation_key))
    if processed is None:
        if validation is not None or curation is not None:
            raise DeterministicPipelineFailure("ClassifyReplay", "OUT_OF_ORDER_MARKER", "downstream marker exists without processed completion")
        return "PROCESS_RAW"
    processed_outputs = {f"{layer}:{dataset}" for layer in ("processed", "rejected") for dataset in SOURCE_FILES.values()}
    _validate_marker(client, config, processed, identity, batch_id, expected_outputs=processed_outputs)
    if validation is None:
        if curation is not None:
            raise DeterministicPipelineFailure("ClassifyReplay", "OUT_OF_ORDER_MARKER", "curation marker exists without validation completion")
        return "VALIDATE_PROCESSED"
    validation_outputs = {f"{kind}:{dataset}" for kind in ("valid", "rejected") for dataset in SOURCE_FILES.values()}
    _validate_marker(client, config, validation, identity, batch_id, expected_outputs=validation_outputs)
    if validation.get("terminal_outcome") == "FAILED":
        raise DeterministicPipelineFailure("ValidateProcessed", "QUALITY_FAILED", "validation marker has FAILED terminal outcome")
    if validation.get("terminal_outcome") not in {"PASSED", "PASSED_WITH_REJECTIONS"}:
        raise DeterministicPipelineFailure("ValidateProcessed", "INVALID_QUALITY_OUTCOME", "validation marker has invalid terminal outcome")
    if curation is None:
        return "BUILD_CURATED"
    _validate_marker(client, config, curation, identity, batch_id, expected_outputs={
        "dim_customer", "dim_date", "dim_geography", "dim_order_status", "dim_product", "dim_seller",
        "fact_reviews", "fact_sales", "cross_state_analysis", "monthly_sales", "order_status", "payment_methods",
        "sales_by_category", "sales_by_state", "seller_performance", "size_analysis",
    })
    return "COMPLETE"


def execute_with_retry(stage: str, operation: Callable[[], str], *, sleeper: Callable[[float], None] = sleep) -> tuple[str, int]:
    retries = 0
    while True:
        try:
            return operation(), retries
        except GlueExecutionFailure as exc:
            if not is_transient_glue_error(exc.code) or retries >= RETRY_MAX_ATTEMPTS:
                exc.retry_count = retries  # type: ignore[attr-defined]
                raise
            sleeper(RETRY_INTERVAL_SECONDS * (RETRY_BACKOFF_RATE ** retries))
            retries += 1


def run_local_glue(stage: str, batch_id: str, client: Any, config: AwsEtlConfig) -> str:
    glue_job = {"ProcessRaw": "process_raw", "ValidateProcessed": "validate_processed", "BuildCurated": "build_curated"}[stage]
    job_run_id = f"local-{stage.lower()}-{uuid4()}"
    injected = int(os.environ.get(f"AWS_ETL_SIMULATE_{stage.upper()}_TRANSIENT_FAILURES", "0"))
    counter_key = f"_AWS_ETL_{stage}_ATTEMPT"
    attempt = int(os.environ.get(counter_key, "0"))
    os.environ[counter_key] = str(attempt + 1)
    if attempt < injected:
        raise GlueExecutionFailure(stage, TRANSIENT_GLUE_ERRORS[1], "injected transient Glue service failure", job_run_id)
    result = subprocess.run(["bash", str(RUN_GLUE)], cwd=ROOT, env={**os.environ, "BATCH_ID": batch_id, "GLUE_JOB": glue_job}, check=False)
    if result.returncode:
        if stage == "ValidateProcessed":
            marker = get_json(client, config.bucket, terminal_summary_key(config, batch_id, "validation"))
            if marker is not None and marker.get("terminal_outcome") == "FAILED":
                raise DeterministicPipelineFailure(stage, "QUALITY_FAILED", "validation marker has FAILED terminal outcome", job_run_id)
        raise GlueExecutionFailure(stage, "LocalGlueJobFailed", f"{glue_job} exited {result.returncode}", job_run_id)
    return job_run_id


def run_local_warehouse(batch_id: str) -> None:
    result = subprocess.run(
        ["bash", str(RUN_GLUE)], cwd=ROOT,
        env={**os.environ, "BATCH_ID": batch_id, "GLUE_JOB": "load_warehouse", "AWS_ETL_WAREHOUSE_MODE": "load"},
        check=False,
    )
    if result.returncode:
        raise SystemExit(result.returncode)


def _provider(exc: BaseException, execution_id: str, retry_count: int = 0) -> dict[str, Any]:
    evidence = {
        "FailureStage": getattr(exc, "stage", "InitializeBatch"),
        "GlueJobRunID": getattr(exc, "job_run_id", None),
        "FailureCode": getattr(exc, "code", type(exc).__name__),
        "FailureMessage": str(exc),
        "ExecutionID": execution_id,
        "RetryCount": retry_count,
    }
    assert set(evidence) == PROVIDER_EVIDENCE_KEYS
    return evidence


def _is_retryable_failure(failure: BaseException | None) -> bool:
    return isinstance(failure, GlueExecutionFailure) and (
        is_transient_glue_error(failure.code) or failure.code == "ORCHESTRATION_CONTENTION"
    )


def record_terminal(client: Any, config: AwsEtlConfig, envelope: dict[str, Any], *, failure: BaseException | None = None) -> None:
    owner = envelope["orchestration"]["execution_id"]
    retryable = _is_retryable_failure(failure)
    retry_count = int(getattr(failure, "retry_count", 0)) if failure else 0
    for submission in envelope["submissions"]:
        if failure is None:
            record = submission["completion_record"]
        else:
            record = {**submission["failure_record"], "Retryable": retryable, "ProviderEvidence": _provider(failure, owner, retry_count)}
        immutable_put_json(client, bucket=config.bucket, key=submission["failure_audit_key" if failure else "completion_audit_key"], document=record, owner=owner, submission_id=submission["submission_id"])


def publish_inputs(client: Any, config: AwsEtlConfig, work: list[tuple[Any, dict[str, Any], str, ReplayDecision]]) -> None:
    for source, manifest, key, decision in work:
        if decision.outcome in {"initial-load", "new-content", "late-content", "retry"}:
            upload_file_if_absent(client, config.bucket, str(manifest["source_object_path"]), source.path)
        if decision.outcome != "fail-batch" and not object_exists(client, config.bucket, key):
            put_json_immutable(client, config.bucket, key, manifest)


def self_check() -> None:
    assert is_transient_glue_error("Glue.InternalServiceException")
    for code in ("Glue.AWSGlueException", "States.ALL", "reject-dataset", "fail-batch", "AccessDeniedException"):
        assert not is_transient_glue_error(code)
    attempts = 0
    def transient_then_success() -> str:
        nonlocal attempts
        attempts += 1
        if attempts <= RETRY_MAX_ATTEMPTS:
            raise GlueExecutionFailure("ProcessRaw", TRANSIENT_GLUE_ERRORS[0], "transient", "jr-self-check")
        return "jr-success"
    result, retries = execute_with_retry("ProcessRaw", transient_then_success, sleeper=lambda _: None)
    assert result == "jr-success" and retries == RETRY_MAX_ATTEMPTS
    attempts = 0
    def deterministic() -> str:
        nonlocal attempts
        attempts += 1
        raise GlueExecutionFailure("ValidateProcessed", "reject-dataset", "deterministic", "jr-self-check")
    try:
        execute_with_retry("ValidateProcessed", deterministic, sleeper=lambda _: None)
    except GlueExecutionFailure:
        pass
    else:
        raise AssertionError("deterministic failure was unexpectedly accepted")
    assert attempts == 1
    assert _is_retryable_failure(GlueExecutionFailure("InitializeBatch", "ORCHESTRATION_CONTENTION", "contended", None))
    assert not _is_retryable_failure(DeterministicPipelineFailure("ValidateProcessed", "QUALITY_FAILED", "failed"))
    print("Validated local retry classification: bounded transient retry and single deterministic attempt")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("config.yaml"))
    parser.add_argument("--dataset-dir", type=Path)
    parser.add_argument("--batch-id")
    parser.add_argument("--batch-timestamp")
    parser.add_argument("--execution-id")
    parser.add_argument("--self-check", action="store_true")
    parser.add_argument("--load-warehouse", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_check:
        self_check()
        return 0
    dataset_dir = args.dataset_dir or (Path(os.environ["DATASET_DIR"]) if os.environ.get("DATASET_DIR") else None)
    if dataset_dir is None:
        raise ValueError("DATASET_DIR is required")
    config = load_config(args.config)
    client = s3_client(config)
    execution_id = args.execution_id or os.environ.get("EXECUTION_ID") or f"local-execution-{uuid4()}"
    batch_id = args.batch_id or os.environ.get("BATCH_ID") or new_batch_id()
    envelope, work = initialize_envelope(client, config, dataset_dir, batch_id, args.batch_timestamp or os.environ.get("BATCH_TIMESTAMP"), execution_id)
    claim_acquired = False
    try:
        acquire_execution_claim(client, bucket=config.bucket, key=envelope["orchestration"]["claim_key"], claim=envelope["orchestration"]["claim"], execution_id=execution_id)
        claim_acquired = True
        publish_inputs(client, config, work)
        if envelope["orchestration"]["replay_class"] == "REUSED_FAILURE":
            raise DeterministicPipelineFailure("ClassifyReplay", "REUSED_FAILURE", "prior deterministic failure reused without Glue execution")
        if envelope["orchestration"]["replay_class"] == "NO_OP":
            record_terminal(client, config, envelope)
            print(f"Batch {batch_id}: completed replay is a no-op")
            if args.load_warehouse or os.environ.get("WAREHOUSE") == "1":
                run_local_warehouse(batch_id)
            return 0
        stage = next_stage(client, config, envelope)
        envelope["orchestration"]["next_stage"] = stage
        stage_index = {
            "PROCESS_RAW": 0,
            "VALIDATE_PROCESSED": 1,
            "BUILD_CURATED": 2,
            "COMPLETE": len(STAGES),
        }
        if stage not in stage_index:
            raise DeterministicPipelineFailure("ClassifyReplay", "INVALID_NEXT_STAGE", f"unsupported next stage {stage!r}")
        start = stage_index[stage]
        for current in STAGES[start:]:
            job_run_id, retries = execute_with_retry(current, lambda current=current: run_local_glue(current, batch_id, client, config))
            envelope["orchestration"]["stages"][current] = {"GlueJobRunID": job_run_id, "RetryCount": retries}
            if current == "ValidateProcessed":
                validation = get_json(client, config.bucket, envelope["orchestration"]["validation_marker_key"])
                if validation is None or validation.get("terminal_outcome") == "FAILED":
                    raise DeterministicPipelineFailure(current, "QUALITY_FAILED", "validation did not publish a passing completion marker", job_run_id)
        record_terminal(client, config, envelope)
        print(f"Batch {batch_id}: pipeline completed ({len(envelope['submissions'])} submissions)")
        if args.load_warehouse or os.environ.get("WAREHOUSE") == "1":
            run_local_warehouse(batch_id)
        return 0
    except RuntimeError as exc:
        if not claim_acquired and "immutable evidence conflict" in str(exc) and not isinstance(exc, (DeterministicPipelineFailure, GlueExecutionFailure)):
            exc = GlueExecutionFailure("InitializeBatch", "ORCHESTRATION_CONTENTION", "execution claim is owned by another execution", None)
        elif "immutable evidence conflict" in str(exc):
            raise
        record_terminal(client, config, envelope, failure=exc)
        raise exc


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
