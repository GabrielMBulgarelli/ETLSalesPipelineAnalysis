"""Immutable provider-neutral submission audit evidence."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from .replay import ReplayDecision


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def create_audit_record(
    manifest: dict[str, Any],
    decision: ReplayDecision,
    attempt_number: int,
) -> dict[str, Any]:
    submission_id = str(uuid4())
    if decision.outcome in {"initial-load", "new-content", "late-content", "retry"}:
        validator_status = "accepted"
        pipeline_outcome = "PASSED"
        terminal_status = "SUCCEEDED"
        retryable = False
    elif decision.outcome == "no-op":
        validator_status = decision.prior_validator_status or "accepted"
        pipeline_outcome = decision.prior_pipeline_outcome or "PASSED"
        terminal_status = "SKIPPED"
        retryable = False
    elif decision.outcome == "reused-failure":
        validator_status = decision.prior_validator_status or "reject-dataset"
        pipeline_outcome = decision.prior_pipeline_outcome or "FAILED"
        terminal_status = "FAILED"
        retryable = False
    elif decision.outcome == "fail-batch":
        validator_status = "fail-batch"
        pipeline_outcome = "FAILED"
        terminal_status = "FAILED"
        retryable = False
    else:
        raise ValueError(f"unsupported replay outcome: {decision.outcome}")

    replay_outcome = "new-content" if decision.outcome == "fail-batch" else decision.outcome
    return {
        "SubmissionID": submission_id,
        "AttemptNumber": attempt_number,
        "SubmittedAt": utc_now(),
        "BatchID": manifest["batch_id"],
        "BatchTimestamp": manifest["batch_timestamp"],
        "Dataset": manifest["dataset"],
        "SourceFileID": manifest["source_file_id"],
        "ContentSHA256": manifest["content_sha256"],
        "ReplayOutcome": replay_outcome,
        "ValidatorStatus": validator_status,
        "PipelineOutcome": pipeline_outcome,
        "TerminalStatus": terminal_status,
        "Retryable": retryable,
        "ReusedSuccessSubmissionID": decision.reused_success_submission_id,
        "ReusedFailureSubmissionID": decision.reused_failure_submission_id,
        "SourceObjectPath": manifest["source_object_path"],
        "ManifestObjectPath": manifest.get("manifest_object_path"),
        "PipelineVersion": manifest["pipeline_version"],
        "FileSize": manifest["file_size"],
        "SourceModificationTimestamp": manifest["source_modification_timestamp"],
        "Reason": decision.reason,
    }


def audit_key(audit_prefix: str, record: dict[str, Any]) -> str:
    return (
        f"{audit_prefix}dataset={record['Dataset']}/batch_id={record['BatchID']}/"
        f"attempt={record['AttemptNumber']:06d}/submission_id={record['SubmissionID']}.json"
    )
