"""Provider-neutral manifest replay classification."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .manifests import IMMUTABLE_MANIFEST_FIELDS


AUDIT_NAMES = {
    "batch_id": "BatchID",
    "batch_timestamp": "BatchTimestamp",
    "dataset": "Dataset",
    "source_file_id": "SourceFileID",
    "content_sha256": "ContentSHA256",
}


@dataclass(frozen=True)
class ReplayDecision:
    outcome: str
    reused_success_submission_id: str | None = None
    reused_failure_submission_id: str | None = None
    prior_validator_status: str | None = None
    prior_pipeline_outcome: str | None = None
    reason: str | None = None


def _value(item: dict[str, Any], field: str) -> Any:
    return item.get(AUDIT_NAMES[field], item.get(field))


def _attempt(item: dict[str, Any]) -> Any:
    return item.get("AttemptNumber", item.get("attempt_number"))


def _latest(items: list[dict[str, Any]]) -> dict[str, Any]:
    attempts = [_attempt(item) for item in items]
    if any(not isinstance(value, int) or isinstance(value, bool) or value < 1 for value in attempts):
        raise ValueError("audit history contains an invalid AttemptNumber")
    if len(attempts) != len(set(attempts)):
        raise ValueError("audit history contains duplicate AttemptNumber values")
    return max(items, key=_attempt)


def classify_replay(manifest: dict[str, Any], history: list[dict[str, Any]]) -> ReplayDecision:
    if any(not manifest.get(field) for field in IMMUTABLE_MANIFEST_FIELDS):
        return ReplayDecision("fail-batch", reason="manifest is missing an immutable identity field")
    if re.fullmatch(r"[0-9a-f]{64}", str(manifest["content_sha256"])) is None:
        return ReplayDecision("fail-batch", reason="content_sha256 is not a lowercase SHA-256 value")

    same_batch = [
        item
        for item in history
        if _value(item, "dataset") == manifest["dataset"]
        and _value(item, "batch_id") == manifest["batch_id"]
    ]
    if same_batch:
        if any(
            any(_value(item, field) != manifest[field] for field in IMMUTABLE_MANIFEST_FIELDS)
            for item in same_batch
        ):
            return ReplayDecision("fail-batch", reason="immutable manifest identity changed under an existing batch ID")
        try:
            latest = _latest(same_batch)
        except ValueError as exc:
            return ReplayDecision("fail-batch", reason=str(exc))
        terminal = latest.get("TerminalStatus", str(latest.get("status", "")).upper())
        replay = latest.get("ReplayOutcome", latest.get("replay_outcome"))
        if terminal == "SUCCEEDED":
            return ReplayDecision(
                "no-op",
                reused_success_submission_id=latest.get("SubmissionID"),
                prior_validator_status=latest.get("ValidatorStatus"),
                prior_pipeline_outcome=latest.get("PipelineOutcome"),
            )
        if terminal == "SKIPPED" and replay == "no-op":
            reused = latest.get("ReusedSuccessSubmissionID")
            if reused:
                return ReplayDecision(
                    "no-op",
                    reused_success_submission_id=reused,
                    prior_validator_status=latest.get("ValidatorStatus"),
                    prior_pipeline_outcome=latest.get("PipelineOutcome"),
                )
        if terminal == "FAILED":
            if latest.get("Retryable") is True:
                return ReplayDecision("retry")
            reused = latest.get("ReusedFailureSubmissionID") or latest.get("SubmissionID")
            return ReplayDecision(
                "reused-failure",
                reused_failure_submission_id=reused,
                prior_validator_status=latest.get("ValidatorStatus"),
                prior_pipeline_outcome=latest.get("PipelineOutcome"),
            )
        if terminal == "SKIPPED" and replay == "reused-failure":
            reused = latest.get("ReusedFailureSubmissionID") or latest.get("SubmissionID")
            return ReplayDecision(
                "reused-failure",
                reused_failure_submission_id=reused,
                prior_validator_status=latest.get("ValidatorStatus"),
                prior_pipeline_outcome=latest.get("PipelineOutcome"),
            )
        return ReplayDecision("fail-batch", reason="latest audit attempt has no recognized terminal state")

    successful = [
        item
        for item in history
        if _value(item, "dataset") == manifest["dataset"]
        and item.get("TerminalStatus", str(item.get("status", "")).upper()) == "SUCCEEDED"
    ]
    matching = [item for item in successful if _value(item, "content_sha256") == manifest["content_sha256"]]
    if matching:
        latest_match = max(matching, key=lambda item: str(item.get("SubmittedAt", "")))
        return ReplayDecision(
            "no-op",
            reused_success_submission_id=latest_match.get("SubmissionID"),
            prior_validator_status=latest_match.get("ValidatorStatus"),
            prior_pipeline_outcome=latest_match.get("PipelineOutcome"),
        )
    if not successful:
        return ReplayDecision("initial-load")
    return ReplayDecision("new-content")


def next_attempt_number(dataset: str, batch_id: str, history: list[dict[str, Any]]) -> int:
    matching = [
        item
        for item in history
        if _value(item, "dataset") == dataset and _value(item, "batch_id") == batch_id
    ]
    if not matching:
        return 1
    return int(_attempt(_latest(matching))) + 1
