"""Phase 7 orchestration contracts, immutable evidence, and ASL validation."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from botocore.exceptions import ClientError, ParamValidationError


TRANSIENT_GLUE_ERRORS = (
    "Glue.ConcurrentRunsExceededException",
    "Glue.InternalServiceException",
    "Glue.OperationTimeoutException",
)
RETRY_INTERVAL_SECONDS = 5
RETRY_BACKOFF_RATE = 2.0
RETRY_MAX_ATTEMPTS = 2
GLUE_JOB_TOKENS = {
    "ProcessRaw": "${ProcessRawGlueJobName}",
    "ValidateProcessed": "${ValidateProcessedGlueJobName}",
    "BuildCurated": "${BuildCuratedGlueJobName}",
}
REQUIRED_ENVELOPE_KEYS = {"batch", "storage", "submissions", "orchestration"}
PROVIDER_EVIDENCE_KEYS = {
    "FailureStage",
    "GlueJobRunID",
    "FailureCode",
    "FailureMessage",
    "ExecutionID",
    "RetryCount",
}


class StateMachineValidationError(ValueError):
    """Raised when the pipeline ASL definition violates the Phase 7 contract."""


def canonical_json(document: dict[str, Any]) -> bytes:
    return json.dumps(document, separators=(",", ":"), sort_keys=True).encode("utf-8")


def evidence_sha256(document: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(document)).hexdigest()


def is_transient_glue_error(error_name: str) -> bool:
    """Return whether an execution failure is inside the deliberately narrow retry boundary."""
    return error_name in TRANSIENT_GLUE_ERRORS


def immutable_put_json(
    client: Any,
    *,
    bucket: str,
    key: str,
    document: dict[str, Any],
    owner: str,
    submission_id: str = "",
) -> bool:
    """Create JSON once, accepting an existing object only when identity and payload match.

    Returns ``True`` for a new write and ``False`` for a verified idempotent replay.
    A conflicting immutable object is a deterministic error.
    """
    digest = evidence_sha256(document)
    metadata = {"owner": owner, "evidence-sha256": digest}
    if submission_id:
        metadata["submission-id"] = submission_id
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=canonical_json(document) + b"\n",
            ContentType="application/json",
            Metadata=metadata,
            IfNoneMatch="*",
        )
        return True
    except ParamValidationError as exc:
        raise RuntimeError(
            "the installed S3 service model does not accept PutObject IfNoneMatch; "
            "immutable orchestration writes cannot be weakened"
        ) from exc
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code not in {"PreconditionFailed", "ConditionalRequestConflict", "412", "409"} and status not in {409, 412}:
            raise

    existing = client.head_object(Bucket=bucket, Key=key).get("Metadata", {})
    same_payload = existing.get("evidence-sha256") == digest
    same_submission = not submission_id or existing.get("submission-id") == submission_id
    same_owner = existing.get("owner") == owner
    if same_payload and same_submission and same_owner:
        return False
    raise RuntimeError(f"immutable evidence conflict at s3://{bucket}/{key}")


def acquire_execution_claim(
    client: Any,
    *,
    bucket: str,
    key: str,
    claim: dict[str, Any],
    execution_id: str,
) -> bool:
    """Acquire a create-only execution claim, or resume it for the same owner."""
    return immutable_put_json(
        client,
        bucket=bucket,
        key=key,
        document=claim,
        owner=execution_id,
    )


def _destinations(state: dict[str, Any]) -> set[str]:
    destinations: set[str] = set()
    for key in ("Next", "Default"):
        if isinstance(state.get(key), str):
            destinations.add(state[key])
    for collection in ("Choices", "Catch"):
        for item in state.get(collection, []):
            if isinstance(item, dict) and isinstance(item.get("Next"), str):
                destinations.add(item["Next"])
    return destinations


def _walk_states(states: dict[str, Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    for name, state in states.items():
        yield name, state
        processor = state.get("ItemProcessor")
        if isinstance(processor, dict) and isinstance(processor.get("States"), dict):
            for child_name, child in _walk_states(processor["States"]):
                yield f"{name}.{child_name}", child


def _validate_graph(definition: dict[str, Any]) -> None:
    states = definition.get("States")
    start_at = definition.get("StartAt")
    if not isinstance(states, dict) or not states:
        raise StateMachineValidationError("States must be a non-empty object")
    if not isinstance(start_at, str) or start_at not in states:
        raise StateMachineValidationError("StartAt must reference a declared state")

    reachable: set[str] = set()
    pending = [start_at]
    while pending:
        name = pending.pop()
        if name in reachable:
            continue
        reachable.add(name)
        destinations = _destinations(states[name])
        missing = destinations.difference(states)
        if missing:
            raise StateMachineValidationError(f"state {name!r} references missing states: {sorted(missing)}")
        pending.extend(destinations.difference(reachable))
    if set(states) != reachable:
        raise StateMachineValidationError(f"unreachable states: {sorted(set(states) - reachable)}")

    terminals = {name for name, state in states.items() if state.get("Type") in {"Succeed", "Fail"} or state.get("End") is True}
    reverse: dict[str, set[str]] = {name: set() for name in states}
    for name, state in states.items():
        for destination in _destinations(state):
            reverse[destination].add(name)
    can_terminate = set(terminals)
    pending = list(terminals)
    while pending:
        for predecessor in reverse[pending.pop()]:
            if predecessor not in can_terminate:
                can_terminate.add(predecessor)
                pending.append(predecessor)
    if not terminals or set(states) != can_terminate:
        raise StateMachineValidationError("every state must reach a terminal state")


def validate_state_machine(definition: dict[str, Any]) -> None:
    """Validate graph shape plus the settled Phase 7 service-integration contract."""
    _validate_graph(definition)
    states = definition["States"]
    required = {
        "InitializeBatch", "ClassifyReplay", "ProcessRaw", "ValidateProcessed",
        "QualityDecision", "BuildCurated", "RecordCompletion", "RecordFailure",
        "Succeed", "Fail",
    }
    if missing := required.difference(states):
        raise StateMachineValidationError(f"missing required states: {sorted(missing)}")

    glue_states = {name: states[name] for name in GLUE_JOB_TOKENS}
    for name, state in glue_states.items():
        if state.get("Resource") != "arn:aws:states:::glue:startJobRun.sync":
            raise StateMachineValidationError(f"{name} must use synchronous Glue integration")
        if state.get("Parameters", {}).get("JobName") != GLUE_JOB_TOKENS[name]:
            raise StateMachineValidationError(f"{name} has the wrong deployment token")
        retries = state.get("Retry")
        expected = [{
            "ErrorEquals": list(TRANSIENT_GLUE_ERRORS),
            "IntervalSeconds": RETRY_INTERVAL_SECONDS,
            "BackoffRate": RETRY_BACKOFF_RATE,
            "MaxAttempts": RETRY_MAX_ATTEMPTS,
        }]
        if retries != expected:
            raise StateMachineValidationError(f"{name} retry policy is not the settled Phase 7 policy")
        arguments = state.get("Parameters", {}).get("Arguments", {})
        expected_arguments = {
            "--BATCH_ID.$": "$.batch.id",
            "--SUBMISSIONS_JSON.$": "States.JsonToString($.submissions)",
            "--BUCKET.$": "$.storage.bucket",
            "--RAW_PREFIX.$": "$.storage.raw_prefix",
            "--PROCESSED_PREFIX.$": "$.storage.processed_prefix",
            "--CURATED_PREFIX.$": "$.storage.curated_prefix",
            "--REJECTED_PREFIX.$": "$.storage.rejected_prefix",
            "--QUALITY_PREFIX.$": "$.storage.quality_prefix",
            "--STAGING_PREFIX.$": "$.storage.staging_prefix",
            "--MANIFEST_PREFIX.$": "$.storage.manifest_prefix",
            "--AUDIT_PREFIX.$": "$.storage.audit_prefix",
            "--CONTRACT_VERSION.$": "$.batch.contract_version",
            "--PIPELINE_VERSION.$": "$.batch.pipeline_version",
        }
        if arguments != expected_arguments:
            raise StateMachineValidationError(f"{name} does not preserve the complete Glue job envelope")

    for name, state in _walk_states(states):
        for retry in state.get("Retry", []):
            errors = retry.get("ErrorEquals", [])
            if any(error in {"States.ALL", "States.TaskFailed", "Glue.AWSGlueException"} for error in errors):
                raise StateMachineValidationError(f"{name} contains a forbidden retry wildcard")

    put_states = [
        state for _, state in _walk_states(states)
        if state.get("Resource") == "arn:aws:states:::aws-sdk:s3:putObject"
    ]
    if len(put_states) < 3 or any(state.get("Parameters", {}).get("IfNoneMatch") != "*" for state in put_states):
        raise StateMachineValidationError("claims and audit evidence require conditional S3 PutObject writes")

    for map_name in ("RecordCompletion", "RecordFailure"):
        selector = states[map_name].get("ItemSelector", {})
        for root in REQUIRED_ENVELOPE_KEYS:
            if f"{root}.$" not in selector:
                raise StateMachineValidationError(f"{map_name} does not preserve envelope member {root!r}")

    rendered = json.dumps(definition)
    for root in REQUIRED_ENVELOPE_KEYS:
        if f"$.{root}" not in rendered:
            raise StateMachineValidationError(f"nested envelope member {root!r} is not preserved")
    if "ProviderEvidence" not in rendered:
        raise StateMachineValidationError("AWS failure details must be nested under ProviderEvidence")


def load_and_validate(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        definition = json.load(handle)
    if not isinstance(definition, dict):
        raise StateMachineValidationError("ASL document must be a JSON object")
    validate_state_machine(definition)
    return definition


def validate_if_none_match_model() -> None:
    """Fail if this runtime's current S3 model cannot express the required request."""
    import boto3
    operation = boto3.session.Session()._session.get_service_model("s3").operation_model("PutObject")
    if "IfNoneMatch" not in operation.input_shape.members:
        raise StateMachineValidationError("current S3 PutObject model does not accept IfNoneMatch")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the AWS ETL Step Functions ASL definition.")
    parser.add_argument("definition", type=Path)
    args = parser.parse_args()
    try:
        definition = load_and_validate(args.definition)
        validate_if_none_match_model()
    except (OSError, json.JSONDecodeError, StateMachineValidationError) as error:
        parser.error(str(error))
    print(f"Validated {args.definition}: {len(definition['States'])} states; conditional PutObject model supported")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
