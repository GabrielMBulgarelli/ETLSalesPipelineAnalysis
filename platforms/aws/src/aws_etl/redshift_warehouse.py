"""Deployment-ready Redshift Serverless warehouse loader (not used by the local PostgreSQL substitute)."""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

from .integrity import assert_curated_content, assert_unique_grain
from .orchestration import canonical_json, immutable_put_json
from .schemas import assert_curated_schema
from .storage import get_json, list_keys
from .warehouse import (
    AGGREGATES,
    DATASETS,
    DIMENSIONS,
    _read_marker,
    _verify_record_hash,
    _verify_relationships,
    _verify_representative_payment,
)


POLICY_ID = "redshift-scd2-v1"
HISTORICAL = {
    "dim_customer": ("CustomerUniqueID", "CustomerZipCodePrefix", "CustomerCity", "CustomerState"),
    "dim_product": ("ProductCategoryName", "ProductCategoryNameEnglish", "ProductWeightG", "ProductLengthCm", "ProductHeightCm", "ProductWidthCm", "ProductVolumeCm3", "SizeCategory"),
    "dim_seller": ("SellerZipCodePrefix", "SellerCity", "SellerState"),
    "dim_geography": ("City", "State", "Latitude", "Longitude", "Region"),
}
RETRYABLE_DATA_API_ERRORS = frozenset({"InternalServerException", "ThrottlingException", "ServiceUnavailableException"})
TERMINAL_STATUSES = frozenset({"FINISHED", "FAILED", "ABORTED"})
MAX_DATA_API_BATCH_STATEMENTS = 40


class DeterministicWarehouseFailure(RuntimeError):
    """Nonretryable data, identity, or warehouse-history failure."""


def _sha(document: Any) -> str:
    return hashlib.sha256(canonical_json(document)).hexdigest()


def deterministic_client_token(fingerprint: str, attempt_id: str, operation: str, dataset: str = "") -> str:
    """A stable token for every retriable request; execution identity is deliberately excluded."""
    return _sha({"fingerprint": fingerprint, "load_attempt_id": attempt_id, "operation": operation, "dataset": dataset})


def _dataset_hash(context: Any, prefix: str) -> str:
    objects = []
    paginator = context.client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=context.config.bucket, Prefix=prefix):
        objects.extend({"key": item["Key"], "etag": item["ETag"].strip('"').lower(), "size": int(item["Size"])}
                       for item in page.get("Contents", []) if item["Key"].endswith(".parquet"))
    if not objects:
        raise DeterministicWarehouseFailure(f"curated dataset has no Parquet files: {prefix}")
    return _sha(sorted(objects, key=lambda item: item["key"]))


def _publication_identity(context: Any, marker: dict[str, Any], marker_body: bytes) -> tuple[str, list[dict[str, Any]]]:
    datasets = [{"dataset": name, "curated_hash": _dataset_hash(context, marker["produced_datasets"][name]["prefix"]),
                 "row_count": int(marker["produced_datasets"][name]["row_count"])} for name in DATASETS]
    identity = {"batch_id": context.batch_id, "contract_version": int(marker["contract_version"]),
                "pipeline_version": str(marker["pipeline_version"]),
                "curation_marker_sha256": hashlib.sha256(marker_body).hexdigest(),
                "datasets": datasets, "warehouse_policy_id": POLICY_ID}
    return _sha(identity), datasets


def _data_api(context: Any):
    if context.config.endpoint_url:
        raise RuntimeError("Redshift Serverless is not emulated locally; use the PostgreSQL warehouse substitute")
    return boto3.client("redshift-data", region_name=context.config.region)


def _request(client: Any, method: str, request: dict[str, Any], *, retryable: bool = True) -> dict[str, Any]:
    try:
        return getattr(client, method)(**request)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if retryable and code in RETRYABLE_DATA_API_ERRORS:
            raise RuntimeError(f"retryable Redshift Data API failure: {code}") from exc
        raise DeterministicWarehouseFailure(f"nonretryable Redshift Data API failure: {code}") from exc


def _wait(client: Any, statement_id: str, *, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        description = _request(client, "describe_statement", {"Id": statement_id}, retryable=True)
        if description["Status"] in TERMINAL_STATUSES:
            if description["Status"] != "FINISHED":
                raise DeterministicWarehouseFailure(
                    f"Redshift statement {statement_id} {description['Status']}: {description.get('Error', '')}"
                )
            return description
        if time.monotonic() >= deadline:
            # A local timeout is not failure evidence. Re-describe, preserve the ID, then apply the approved cancel policy.
            description = _request(client, "describe_statement", {"Id": statement_id}, retryable=True)
            if description["Status"] in TERMINAL_STATUSES:
                continue
            _request(client, "cancel_statement", {"Id": statement_id}, retryable=True)
            raise RuntimeError(f"Redshift statement {statement_id} exceeded the approved timeout and was cancellation-requested")
        time.sleep(1)


def _execute(client: Any, common: dict[str, str], sql: str, token: str, parameters: list[dict[str, str]] | None = None) -> str:
    request: dict[str, Any] = {**common, "Sql": sql, "ClientToken": token}
    if parameters:
        request["Parameters"] = parameters
    statement_id = _request(client, "execute_statement", request)["Id"]
    _wait(client, statement_id, timeout_seconds=900)
    return statement_id


def _result(client: Any, statement_id: str) -> list[list[dict[str, Any]]]:
    return _request(client, "get_statement_result", {"Id": statement_id})["Records"]


def _registry(client: Any, common: dict[str, str], batch_id: str, fingerprint: str,
              attempt_id: str, operation: str) -> str | None:
    statement_id = _execute(client, common,
        "SELECT \"PublicationFingerprint\" FROM audit.completed_publications WHERE \"BatchID\"=:batch_id",
        deterministic_client_token(fingerprint, attempt_id, operation), [{"name": "batch_id", "value": batch_id}])
    records = _result(client, statement_id)
    if len(records) > 1:
        raise DeterministicWarehouseFailure("duplicate completed Redshift publication rows")
    return None if not records else str(records[0][0]["stringValue"])


def _validate_frames(context: Any, marker: dict[str, Any], contract: dict[str, Any]) -> dict[str, Any]:
    frames = {}
    for dataset in DATASETS:
        frame = context.spark.read.parquet(f"s3a://{context.config.bucket}/{marker['produced_datasets'][dataset]['prefix']}").cache()
        assert_curated_schema(dataset, frame, contract)
        assert_curated_content(frame, dataset, contract)
        grain = contract["datasets"][dataset].get("grain") or contract["datasets"][dataset].get("business_key")
        assert_unique_grain(frame, dataset, grain)
        if frame.count() != int(marker["produced_datasets"][dataset]["row_count"]):
            raise DeterministicWarehouseFailure(f"curation marker row count mismatch for {dataset}")
        _verify_record_hash(frame, dataset, contract)
        frames[dataset] = frame
    _verify_relationships(frames)
    frames["_marker"] = marker
    _verify_representative_payment(context, frames)
    del frames["_marker"]
    if "payment_value" in {column.lower() for column in frames["payment_methods"].columns}:
        raise DeterministicWarehouseFailure("payment_methods must remain item-price attribution only")
    return frames


def _stage(context: Any, frames: dict[str, Any], contract: dict[str, Any], marker: dict[str, Any],
           attempt_id: str, fingerprint: str, client: Any, common: dict[str, str], copy_role_arn: str) -> list[str]:
    from pyspark.sql import functions as F

    batch_effective_at = next(iter(context.manifests.values()))["batch_timestamp"]
    statement_ids = []
    for dataset in DATASETS:
        frame = frames[dataset]
        contracted = [field["name"] for field in contract["datasets"][dataset]["fields"]]
        if dataset in HISTORICAL:
            tracked = F.to_json(F.struct(*[F.col(name) for name in HISTORICAL[dataset]]), {"ignoreNullFields": "false"})
            frame = frame.withColumn("SCD2TrackedHash", F.sha2(tracked, 256))
            contracted.append("SCD2TrackedHash")
        staged = frame.withColumn("LoadAttemptID", F.lit(attempt_id)).withColumn("SourceBatchID", F.lit(context.batch_id)).withColumn(
            "WarehouseEffectiveAt", F.to_timestamp(F.lit(batch_effective_at)))
        prefix = f"{context.config.staging_prefix}warehouse/redshift/load_attempt_id={attempt_id}/{dataset}/"
        staged.select(*contracted, "LoadAttemptID", "SourceBatchID", "WarehouseEffectiveAt").write.mode("overwrite").parquet(
            f"s3a://{context.config.bucket}/{prefix}")
        entries = [{"url": f"s3://{context.config.bucket}/{key}", "mandatory": True}
                   for key in list_keys(context.client, context.config.bucket, prefix) if key.endswith(".parquet")]
        if not entries:
            raise DeterministicWarehouseFailure(f"warehouse staging produced no Parquet for {dataset}")
        manifest_key = f"{prefix}copy-manifest.json"
        immutable_put_json(context.client, bucket=context.config.bucket, key=manifest_key,
                           document={"entries": entries}, owner=attempt_id)
        sql = (f'DELETE FROM staging.{dataset} WHERE "LoadAttemptID"=\'{attempt_id}\';\n'
               f"COPY staging.{dataset} FROM 's3://{context.config.bucket}/{manifest_key}' IAM_ROLE '{copy_role_arn}' FORMAT AS PARQUET MANIFEST;")
        statements = [part.strip() for part in sql.split(";\n") if part.strip()]
        if len(statements) > MAX_DATA_API_BATCH_STATEMENTS:
            raise RuntimeError("Data API batch statement limit exceeded")
        response = _request(client, "batch_execute_statement", {**common, "Sqls": statements,
            "ClientToken": deterministic_client_token(fingerprint, attempt_id, "copy", dataset)})
        statement_ids.append(response["Id"])
        _wait(client, response["Id"], timeout_seconds=900)
    return statement_ids


def _stage_identity(client: Any, common: dict[str, str], batch_id: str, attempt_id: str,
                    fingerprint: str, datasets: list[dict[str, Any]]) -> list[str]:
    values, parameters = [], []
    for ordinal, item in enumerate(datasets, 1):
        values.append(f"(:attempt{ordinal},:batch{ordinal},:fingerprint{ordinal},:ordinal{ordinal},:name{ordinal},:hash{ordinal},:count{ordinal})")
        parameters.extend(({"name": f"attempt{ordinal}", "value": attempt_id}, {"name": f"batch{ordinal}", "value": batch_id},
                           {"name": f"fingerprint{ordinal}", "value": fingerprint},
                           {"name": f"ordinal{ordinal}", "value": str(ordinal)}, {"name": f"name{ordinal}", "value": item["dataset"]},
                           {"name": f"hash{ordinal}", "value": item["curated_hash"]}, {"name": f"count{ordinal}", "value": str(item["row_count"])}))
    delete_id = _execute(client, common,
        'DELETE FROM staging.publication_datasets WHERE "LoadAttemptID"=:delete_attempt',
        deterministic_client_token(fingerprint, attempt_id, "stage-identity-delete"),
        [{"name": "delete_attempt", "value": attempt_id}])
    insert_id = _execute(client, common, 'INSERT INTO staging.publication_datasets VALUES ' + ",".join(values),
                         deterministic_client_token(fingerprint, attempt_id, "stage-identity-insert"), parameters)
    return [delete_id, insert_id]


def run_redshift_warehouse(
    context: Any,
    contract: dict[str, Any],
    *,
    load_attempt_id: str | None = None,
    retry_count: int = 0,
) -> dict[str, Any]:
    """Validate, stage, COPY, and atomically publish; S3 completion follows Redshift commit."""
    marker, marker_body = _read_marker(context)
    if marker.get("terminal_outcome") not in {"PASSED", "PASSED_WITH_REJECTIONS"}:
        raise DeterministicWarehouseFailure("Redshift publication requires a passing immutable curation marker")
    if set(marker.get("produced_datasets", {})) != set(DATASETS):
        raise DeterministicWarehouseFailure("curation marker must declare all 16 datasets")
    fingerprint, datasets = _publication_identity(context, marker, marker_body)
    attempt_id = load_attempt_id or os.environ.get("CUSTOMER_LOAD_ATTEMPT_ID", str(uuid4()))
    glue_job_run_id = os.environ.get("AWS_GLUE_JOB_RUN_ID", "unknown")
    retry_count = int(retry_count)
    workgroup = os.environ["CUSTOMER_REDSHIFT_WORKGROUP"]
    database = os.environ["CUSTOMER_REDSHIFT_DATABASE"]
    copy_role_arn = os.environ["CUSTOMER_REDSHIFT_COPY_ROLE_ARN"]
    client, common = _data_api(context), {"WorkgroupName": workgroup, "Database": database}
    marker_key = f"{context.config.audit_prefix}warehouse/redshift/completed/batch_id={context.batch_id}/publication.json"
    s3_marker = get_json(context.client, context.config.bucket, marker_key)
    existing = _registry(client, common, context.batch_id, fingerprint, attempt_id, "registry-initial")
    if s3_marker is not None and existing is None:
        raise DeterministicWarehouseFailure("S3 Redshift marker exists but authoritative Redshift registry is missing")
    if existing is not None and existing != fingerprint:
        raise DeterministicWarehouseFailure("Redshift publication fingerprint conflict")
    completion = {"batch_id": context.batch_id, "publication_fingerprint": fingerprint, "warehouse_policy_id": POLICY_ID,
                  "contract_version": int(marker["contract_version"]), "pipeline_version": str(marker["pipeline_version"]),
                  "curation_marker_sha256": hashlib.sha256(marker_body).hexdigest(), "datasets": datasets}
    if s3_marker is not None and s3_marker != completion:
        raise DeterministicWarehouseFailure("S3 and Redshift completion evidence disagree")
    if existing == fingerprint:
        immutable_put_json(context.client, bucket=context.config.bucket, key=marker_key, document=completion,
                           owner="redshift-completed-publication")
        return {"outcome": "NO_OP", "load_attempt_id": attempt_id, "publication_fingerprint": fingerprint,
                "statement_ids": [], "glue_job_run_id": glue_job_run_id, "retry_count": retry_count}
    immutable_put_json(
        context.client,
        bucket=context.config.bucket,
        key=f"{context.config.staging_prefix}warehouse/redshift/load_attempt_id={attempt_id}/attempt-identity.json",
        document={
            "batch_id": context.batch_id,
            "load_attempt_id": attempt_id,
            "publication_fingerprint": fingerprint,
            "glue_job_run_id": glue_job_run_id,
            "execution_timestamp": context.processing_timestamp,
            "retry_count": retry_count,
            "datasets": datasets,
        },
        owner=attempt_id,
    )
    frames = _validate_frames(context, marker, contract)
    statement_ids: list[str] = []
    try:
        statement_ids.extend(_stage(context, frames, contract, marker, attempt_id, fingerprint, client, common, copy_role_arn))
        statement_ids.extend(_stage_identity(client, common, context.batch_id, attempt_id, fingerprint, datasets))
        immediately_before = _registry(client, common, context.batch_id, fingerprint, attempt_id, "registry-prepublish")
        if immediately_before is not None:
            if immediately_before != fingerprint:
                raise DeterministicWarehouseFailure("Redshift publication fingerprint conflict before publication")
            immutable_put_json(context.client, bucket=context.config.bucket, key=marker_key, document=completion,
                               owner="redshift-completed-publication")
            return {"outcome": "NO_OP", "load_attempt_id": attempt_id, "publication_fingerprint": fingerprint,
                    "statement_ids": statement_ids, "glue_job_run_id": glue_job_run_id, "retry_count": retry_count}
        parameters = [{"name": "batch_id", "value": context.batch_id}, {"name": "attempt_id", "value": attempt_id},
                      {"name": "fingerprint", "value": fingerprint}, {"name": "contract", "value": str(marker["contract_version"])},
                      {"name": "pipeline", "value": str(marker["pipeline_version"])},
                      {"name": "marker_sha", "value": hashlib.sha256(marker_body).hexdigest()},
                      {"name": "effective", "value": next(iter(context.manifests.values()))["batch_timestamp"]},
                      {"name": "datasets", "value": json.dumps(datasets, separators=(",", ":"))},
                      {"name": "event_id", "value": str(uuid4())}]
        call = "CALL audit.publish_warehouse(:batch_id,:attempt_id,:fingerprint,:contract,:pipeline,:marker_sha,:effective,:datasets,:event_id)"
        try:
            statement_ids.append(_execute(client, common, call,
                                          deterministic_client_token(fingerprint, attempt_id, "publish"), parameters))
        except RuntimeError:
            recovered = _registry(client, common, context.batch_id, fingerprint, attempt_id, "registry-ambiguous-recovery")
            if recovered != fingerprint:
                raise
        if _registry(client, common, context.batch_id, fingerprint, attempt_id, "registry-postpublish") != fingerprint:
            raise DeterministicWarehouseFailure("publication CALL completed without matching Redshift registry evidence")
        immutable_put_json(context.client, bucket=context.config.bucket, key=marker_key, document=completion,
                           owner="redshift-completed-publication")
        return {"outcome": "COMPLETED", "load_attempt_id": attempt_id, "publication_fingerprint": fingerprint,
                "statement_ids": statement_ids, "glue_job_run_id": glue_job_run_id, "retry_count": retry_count}
    except Exception as exc:
        failure = {"event_type": "FAILED", "batch_id": context.batch_id, "load_attempt_id": attempt_id,
                   "publication_fingerprint": fingerprint, "statement_ids": statement_ids,
                   "glue_job_run_id": glue_job_run_id, "retry_count": retry_count,
                   "retryable": not isinstance(exc, DeterministicWarehouseFailure), "failure_stage": "LoadWarehouse",
                   "failure_code": type(exc).__name__, "failure_message": str(exc),
                   "event_timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z")}
        immutable_put_json(context.client, bucket=context.config.bucket,
                           key=f"{context.config.audit_prefix}warehouse/redshift/attempts/{attempt_id}.json",
                           document=failure, owner=attempt_id)
        raise
    finally:
        for frame in frames.values():
            frame.unpersist()
