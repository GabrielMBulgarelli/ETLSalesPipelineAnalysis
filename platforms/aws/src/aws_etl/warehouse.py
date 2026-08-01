"""Validated, replay-safe publication of curated snapshots to local PostgreSQL."""

from __future__ import annotations

import hashlib
import json
import os
import platform
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from .aggregations import build_aggregations
from .facts import representative_payments
from .integrity import assert_curated_content, assert_unique_grain, conform_curated
from .job_context import JobContext, manifest_fingerprints
from .schemas import assert_curated_schema
from .writers import existing_terminal_summary, terminal_summary_key, verify_marker_outputs


DIMENSIONS = ("dim_customer", "dim_product", "dim_seller", "dim_geography", "dim_date", "dim_order_status")
FACTS = ("fact_sales", "fact_reviews")
AGGREGATES = (
    "sales_by_state", "sales_by_category", "monthly_sales", "order_status",
    "cross_state_analysis", "seller_performance", "size_analysis", "payment_methods",
)
DATASETS = DIMENSIONS + FACTS + AGGREGATES


class PublicationConflict(RuntimeError):
    """A BatchID already has a different immutable publication identity."""

SURROGATES = {
    "dim_customer": "CustomerKey", "dim_product": "ProductKey", "dim_seller": "SellerKey",
    "dim_geography": "GeographyKey", "dim_date": "DateSurrogateKey",
    "dim_order_status": "OrderStatusKey",
}
FACT_REFERENCES = {
    "fact_sales": (
        ("CustomerKey", "dim_customer", "CustomerID"),
        ("ProductKey", "dim_product", "ProductID"),
        ("SellerKey", "dim_seller", "SellerID"),
        ("DateSurrogateKey", "dim_date", "DateKey"),
        ("OrderStatusKey", "dim_order_status", "StatusID"),
        ("GeographyKey", "dim_geography", "ZipCodePrefix"),
    ),
    "fact_reviews": (
        ("CustomerKey", "dim_customer", "CustomerID"),
        ("DateSurrogateKey", "dim_date", "DateKey"),
    ),
}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _canonical_sha(document: Any) -> str:
    body = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def _jdbc_options() -> dict[str, str]:
    password = os.environ.get("POSTGRES_ETL_PASSWORD")
    if not password:
        raise ValueError("POSTGRES_ETL_PASSWORD is required")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    if host in {"127.0.0.1", "localhost"}:
        raise ValueError("container warehouse loading must use the PostgreSQL Compose service name")
    port = os.environ.get("POSTGRES_PORT", "5432")
    database = os.environ.get("POSTGRES_DB", "ecommerce_sales")
    user = os.environ.get("POSTGRES_ETL_USER", "ecommerce_etl")
    if user != "ecommerce_etl":
        raise ValueError("warehouse loading must use the non-superuser ecommerce_etl role")
    return {
        "url": f"jdbc:postgresql://{host}:{port}/{database}",
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }


def _connect(context: JobContext, options: dict[str, str]):
    context.spark._jvm.java.lang.Class.forName(options["driver"])
    return context.spark._jvm.java.sql.DriverManager.getConnection(
        options["url"], options["user"], options["password"]
    )


def _execute(connection: Any, sql: str, values: tuple[Any, ...] = ()) -> None:
    statement = connection.prepareStatement(sql)
    try:
        for index, value in enumerate(values, 1):
            if value is None:
                statement.setNull(index, 12)
            elif isinstance(value, int):
                statement.setInt(index, value)
            else:
                statement.setString(index, str(value))
        statement.executeUpdate()
    finally:
        statement.close()


def _query_one(connection: Any, sql: str, values: tuple[Any, ...] = ()) -> tuple[Any, ...] | None:
    statement = connection.prepareStatement(sql)
    try:
        for index, value in enumerate(values, 1):
            statement.setString(index, str(value))
        result = statement.executeQuery()
        try:
            if not result.next():
                return None
            metadata = result.getMetaData()
            return tuple(result.getObject(index) for index in range(1, metadata.getColumnCount() + 1))
        finally:
            result.close()
    finally:
        statement.close()


def _event(
    context: JobContext,
    options: dict[str, str],
    *,
    attempt_id: str,
    batch_id: str,
    curation_attempt: str | None,
    event_type: str,
    fingerprint: str | None,
    marker_sha: str | None,
    contract_version: int | None,
    pipeline_version: str | None,
    details: dict[str, Any],
    connection: Any | None = None,
) -> str:
    event_id = str(uuid4())
    own_connection = connection is None
    target = connection or _connect(context, options)
    if own_connection:
        target.setAutoCommit(False)
    loader = {
        "python": platform.python_version(),
        "spark": context.spark.version,
        "role": options["user"],
        "runtime": "pinned-aws-glue-container",
    }
    try:
        _execute(
            target,
            'INSERT INTO audit.warehouse_load_events '
            '("EventID", "LoadAttemptID", "BatchID", "CurationAttemptID", "EventTimestamp", '
            '"EventType", "PublicationFingerprint", "MarkerSHA256", "ContractVersion", '
            '"PipelineVersion", "LoaderDetails", "Details") '
            'VALUES (?, ?, ?, ?, ?::timestamptz, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb)',
            (
                event_id, attempt_id, batch_id, curation_attempt, _utc_now(), event_type,
                fingerprint, marker_sha, contract_version, pipeline_version,
                json.dumps(loader, sort_keys=True), json.dumps(details, sort_keys=True),
            ),
        )
        if own_connection:
            target.commit()
        return event_id
    except Exception:
        if own_connection:
            target.rollback()
        raise
    finally:
        if own_connection:
            target.close()


def _read_marker(context: JobContext) -> tuple[dict[str, Any], bytes]:
    key = terminal_summary_key(context.config, context.batch_id, "curation")
    response = context.client.get_object(Bucket=context.config.bucket, Key=key)
    body = response["Body"].read()
    marker = json.loads(body.decode("utf-8"))
    if not isinstance(marker, dict):
        raise RuntimeError("curation completion marker is not a JSON object")
    return marker, body


def _dataset_hash(context: JobContext, prefix: str) -> str:
    objects: list[dict[str, Any]] = []
    paginator = context.client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=context.config.bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".parquet"):
                objects.append({
                    "key": item["Key"], "etag": item["ETag"].strip('"').lower(),
                    "size": int(item["Size"]),
                })
    if not objects:
        raise RuntimeError(f"curated dataset has no Parquet objects: {prefix}")
    return _canonical_sha(sorted(objects, key=lambda item: item["key"]))


def publication_evidence(context: JobContext, marker: dict[str, Any], marker_body: bytes) -> tuple[str, str, list[dict[str, Any]]]:
    marker_sha = hashlib.sha256(marker_body).hexdigest()
    datasets = []
    for dataset in DATASETS:
        metadata = marker["produced_datasets"][dataset]
        datasets.append({
            "dataset": dataset,
            "curated_hash": _dataset_hash(context, metadata["prefix"]),
            "row_count": int(metadata["row_count"]),
        })
    identity = {
        "batch_id": context.batch_id,
        "contract_version": int(marker["contract_version"]),
        "pipeline_version": str(marker["pipeline_version"]),
        "curation_marker_sha256": marker_sha,
        "datasets": datasets,
    }
    fingerprint = _canonical_sha(identity)
    override = os.environ.get("AWS_ETL_WAREHOUSE_FINGERPRINT_OVERRIDE")
    if override:
        if os.environ.get("AWS_ETL_WAREHOUSE_ALLOW_INJECTION") != "1" or len(override) != 64:
            raise ValueError("warehouse fingerprint override is restricted to explicit local failure injection")
        fingerprint = override.lower()
    return fingerprint, marker_sha, datasets


def _assert_same_rows(expected: Any, actual: Any, dataset: str) -> None:
    if expected.exceptAll(actual).limit(1).count() or actual.exceptAll(expected).limit(1).count():
        raise RuntimeError(f"warehouse reconciliation mismatch for {dataset}")


def _verify_record_hash(frame: Any, dataset: str, contract: dict[str, Any]) -> None:
    from pyspark.sql import functions as F

    fields = [item["name"] for item in contract["datasets"][dataset]["fields"]]
    if "RecordHash" not in fields:
        return
    grain = contract["datasets"][dataset].get("grain") or contract["datasets"][dataset]["business_key"]
    stored = frame.select(*grain, F.col("RecordHash").alias("_stored"))
    recomputed = conform_curated(frame.drop("RecordHash"), dataset, contract).select(
        *grain, F.col("RecordHash").alias("_computed")
    )
    condition = None
    for key in grain:
        part = F.col(f"stored.{key}").eqNullSafe(F.col(f"recomputed.{key}"))
        condition = part if condition is None else condition & part
    if stored.alias("stored").join(recomputed.alias("recomputed"), condition, "inner").filter(
        F.col("_stored") != F.col("_computed")
    ).limit(1).count():
        raise RuntimeError(f"RecordHash mismatch for {dataset}")


def _verify_relationships(frames: dict[str, Any]) -> None:
    relationships = (
        ("fact_sales", "CustomerID", "dim_customer", "CustomerID"),
        ("fact_sales", "ProductID", "dim_product", "ProductID"),
        ("fact_sales", "SellerID", "dim_seller", "SellerID"),
        ("fact_sales", "DateKey", "dim_date", "DateKey"),
        ("fact_sales", "StatusID", "dim_order_status", "StatusID"),
        ("fact_sales", "ZipCodePrefix", "dim_geography", "ZipCodePrefix"),
        ("fact_reviews", "CustomerID", "dim_customer", "CustomerID"),
        ("fact_reviews", "DateKey", "dim_date", "DateKey"),
    )
    for child, child_key, parent, parent_key in relationships:
        keys = frames[parent].select(parent_key).distinct()
        if frames[child].join(keys, frames[child][child_key] == keys[parent_key], "left_anti").limit(1).count():
            raise RuntimeError(f"orphan relationship {child}.{child_key} -> {parent}.{parent_key}")


def _verify_representative_payment(context: JobContext, frames: dict[str, Any]) -> None:
    from pyspark.sql import functions as F

    validation = existing_terminal_summary(context.client, context.config, context.batch_id, "validation")
    if validation is None or validation.get("attempt_id") != frames["_marker"].get("validation_attempt_id"):
        raise RuntimeError("curation marker validation attempt does not match the immutable validation marker")
    if validation.get("terminal_outcome") not in {"PASSED", "PASSED_WITH_REJECTIONS"}:
        raise RuntimeError("representative-payment validation requires a passing validation marker")
    prefix = validation["produced_datasets"]["valid:order_payments"]["prefix"]
    payments = context.spark.read.parquet(f"s3a://{context.config.bucket}/{prefix}")
    expected = representative_payments(payments).select(
        F.col("order_id").alias("OrderID"), F.col("PaymentType").alias("ExpectedPaymentType")
    )
    actual = frames["fact_sales"].select("OrderID", "PaymentType").distinct()
    if actual.join(expected, "OrderID", "left").filter(
        ~F.col("PaymentType").eqNullSafe(F.col("ExpectedPaymentType"))
    ).limit(1).count():
        raise RuntimeError("representative-payment semantics mismatch")


def _validate_frames(context: JobContext, marker: dict[str, Any], contract: dict[str, Any], frames: dict[str, Any]) -> None:
    for dataset in DATASETS:
        frame = frames[dataset]
        assert_curated_schema(dataset, frame, contract)
        assert_curated_content(frame, dataset, contract)
        grain = contract["datasets"][dataset].get("grain") or contract["datasets"][dataset].get("business_key")
        assert_unique_grain(frame, dataset, grain)
        if frame.count() != int(marker["produced_datasets"][dataset]["row_count"]):
            raise RuntimeError(f"curation marker row count mismatch for {dataset}")
        _verify_record_hash(frame, dataset, contract)
    _verify_relationships(frames)
    frames["_marker"] = marker
    _verify_representative_payment(context, frames)
    frames.pop("_marker")
    batch_timestamp = next(iter(context.manifests.values()))["batch_timestamp"]
    expected = build_aggregations(frames["fact_sales"], {name: frames[name] for name in DIMENSIONS}, batch_timestamp)
    for dataset in AGGREGATES:
        conformed = conform_curated(expected[dataset], dataset, contract)
        _assert_same_rows(conformed, frames[dataset].select(*conformed.columns), dataset)
    if "payment_value" in {name.lower() for name in frames["payment_methods"].columns}:
        raise RuntimeError("payment_methods must not contain payment_value")


def _curated_frames(context: JobContext, marker: dict[str, Any]) -> dict[str, Any]:
    return {
        dataset: context.spark.read.parquet(
            f"s3a://{context.config.bucket}/{marker['produced_datasets'][dataset]['prefix']}"
        ).cache()
        for dataset in DATASETS
    }


def _jdbc_frame(context: JobContext, options: dict[str, str], query: str):
    return context.spark.read.format("jdbc").options(**options).option("query", query).load()


def _stage(context: JobContext, options: dict[str, str], frames: dict[str, Any], attempt_id: str) -> dict[str, Any]:
    from pyspark.sql import functions as F

    staged = {}
    for dataset in DATASETS:
        frames[dataset].withColumn("LoadAttemptID", F.lit(attempt_id)).select(
            "LoadAttemptID", *frames[dataset].columns
        ).write.format("jdbc").options(**options).option("dbtable", f"staging.{dataset}").mode("append").save()
        quoted = attempt_id.replace("'", "''")
        staged[dataset] = _jdbc_frame(
            context, options, f'SELECT * FROM staging.{dataset} WHERE "LoadAttemptID" = \'{quoted}\''
        ).drop("LoadAttemptID").cache()
    return staged


def _quoted(names: list[str]) -> str:
    return ", ".join(f'"{name}"' for name in names)


def _publish_snapshot(
    context: JobContext,
    options: dict[str, str],
    contract: dict[str, Any],
    marker: dict[str, Any],
    attempt_id: str,
    fingerprint: str,
    marker_sha: str,
    dataset_evidence: list[dict[str, Any]],
) -> str:
    connection = _connect(context, options)
    connection.setAutoCommit(False)
    try:
        _query_one(connection, "SELECT pg_advisory_xact_lock(hashtextextended('ecommerce-sales-warehouse-publication', 0))")
        existing = _query_one(
            connection,
            'SELECT "PublicationFingerprint" FROM audit.completed_publications WHERE "BatchID" = ?',
            (context.batch_id,),
        )
        event_common = dict(
            context=context, options=options, attempt_id=attempt_id, batch_id=context.batch_id,
            curation_attempt=str(marker["attempt_id"]), fingerprint=fingerprint,
            marker_sha=marker_sha, contract_version=int(marker["contract_version"]),
            pipeline_version=str(marker["pipeline_version"]), connection=connection,
        )
        if existing is not None:
            if str(existing[0]) == fingerprint:
                _event(event_type="NO_OP", details={"reason": "matching completed publication"}, **event_common)
                connection.commit()
                return "NO_OP"
            _event(event_type="CONFLICT", details={"completed_fingerprint": str(existing[0])}, **event_common)
            connection.commit()
            raise PublicationConflict("conflicting completed publication for BatchID")

        for dataset in DIMENSIONS:
            fields = [item["name"] for item in contract["datasets"][dataset]["fields"]]
            keys = contract["datasets"][dataset]["business_key"]
            updates = [name for name in fields if name not in keys]
            _execute(
                connection,
                f'INSERT INTO warehouse.{dataset} ({_quoted(fields)}, "SourceBatchID") '
                f'SELECT {_quoted(fields)}, ? FROM staging.{dataset} WHERE "LoadAttemptID" = ? '
                f'ON CONFLICT ({_quoted(keys)}) DO UPDATE SET '
                + ", ".join(f'"{name}" = EXCLUDED."{name}"' for name in updates)
                + ', "SourceBatchID" = EXCLUDED."SourceBatchID"',
                (context.batch_id, attempt_id),
            )

        _execute(connection, "DELETE FROM warehouse.fact_reviews")
        _execute(connection, "DELETE FROM warehouse.fact_sales")
        for dataset in FACTS:
            fields = [item["name"] for item in contract["datasets"][dataset]["fields"]]
            references = FACT_REFERENCES[dataset]
            aliases = [f"d{index}" for index in range(len(references))]
            joins = " ".join(
                f'JOIN warehouse.{dimension} {alias} ON {alias}."{business_key}" = s."{business_key}"'
                for alias, (_, dimension, business_key) in zip(aliases, references)
            )
            surrogate_columns = [item[0] for item in references]
            surrogate_select = ", ".join(
                f'{alias}."{SURROGATES[dimension]}"'
                for alias, (_, dimension, _) in zip(aliases, references)
            )
            _execute(
                connection,
                f'INSERT INTO warehouse.{dataset} ({_quoted(surrogate_columns + fields)}, "SourceBatchID") '
                f'SELECT {surrogate_select}, ' + ", ".join(f's."{name}"' for name in fields) + f', ? '
                f'FROM staging.{dataset} s {joins} WHERE s."LoadAttemptID" = ?',
                (context.batch_id, attempt_id),
            )
        if os.environ.get("AWS_ETL_WAREHOUSE_FAIL_AFTER") == "facts":
            raise RuntimeError("injected warehouse publication failure after facts")

        for dataset in DIMENSIONS:
            keys = contract["datasets"][dataset]["business_key"]
            match = " AND ".join(f's."{name}" = d."{name}"' for name in keys)
            _execute(
                connection,
                f'DELETE FROM warehouse.{dataset} d WHERE NOT EXISTS '
                f'(SELECT 1 FROM staging.{dataset} s WHERE s."LoadAttemptID" = ? AND {match})',
                (attempt_id,),
            )

        for dataset in AGGREGATES:
            fields = [item["name"] for item in contract["datasets"][dataset]["fields"]]
            _execute(connection, f"DELETE FROM analytics.{dataset}")
            _execute(
                connection,
                f'INSERT INTO analytics.{dataset} ({_quoted(fields)}, "SourceBatchID") '
                f'SELECT {_quoted(fields)}, ? FROM staging.{dataset} WHERE "LoadAttemptID" = ?',
                (context.batch_id, attempt_id),
            )
        if os.environ.get("AWS_ETL_WAREHOUSE_FAIL_AFTER") == "aggregates":
            raise RuntimeError("injected warehouse publication failure after aggregates")

        event_id = _event(event_type="COMPLETED", details={"datasets": len(DATASETS)}, **event_common)
        completed_at = _utc_now()
        _execute(
            connection,
            'INSERT INTO audit.completed_publications '
            '("BatchID", "PublicationFingerprint", "ContractVersion", "PipelineVersion", '
            '"MarkerSHA256", "DatasetEvidence", "CompletedEventID", "CompletedAt") '
            'VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?::timestamptz)',
            (
                context.batch_id, fingerprint, int(marker["contract_version"]), marker["pipeline_version"],
                marker_sha, json.dumps(dataset_evidence, separators=(",", ":")), event_id, completed_at,
            ),
        )
        _execute(
            connection,
            'INSERT INTO audit.current_snapshot ("Singleton", "BatchID", "PublicationFingerprint", "PublishedAt") '
            'VALUES (true, ?, ?, ?::timestamptz) ON CONFLICT ("Singleton") DO UPDATE SET '
            '"BatchID" = EXCLUDED."BatchID", "PublicationFingerprint" = EXCLUDED."PublicationFingerprint", '
            '"PublishedAt" = EXCLUDED."PublishedAt"',
            (context.batch_id, fingerprint, completed_at),
        )
        connection.commit()
        return "COMPLETED"
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _cleanup_attempt(context: JobContext, options: dict[str, str], attempt_id: str) -> None:
    connection = _connect(context, options)
    connection.setAutoCommit(False)
    try:
        for dataset in DATASETS:
            _execute(connection, f'DELETE FROM staging.{dataset} WHERE "LoadAttemptID" = ?', (attempt_id,))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _validate_published(
    context: JobContext,
    options: dict[str, str],
    contract: dict[str, Any],
    marker: dict[str, Any],
    frames: dict[str, Any],
    fingerprint: str,
) -> None:
    connection = _connect(context, options)
    try:
        existing = _query_one(
            connection,
            'SELECT "PublicationFingerprint" FROM audit.completed_publications WHERE "BatchID" = ?',
            (context.batch_id,),
        )
        if existing is None or str(existing[0]) != fingerprint:
            raise RuntimeError("completed publication registry does not match curated evidence")
    finally:
        connection.close()
    for dataset in DATASETS:
        schema = "analytics" if dataset in AGGREGATES else "warehouse"
        columns = [item["name"] for item in contract["datasets"][dataset]["fields"]]
        actual = _jdbc_frame(context, options, f"SELECT {_quoted(columns)} FROM {schema}.{dataset}")
        expected = frames[dataset].select(*columns)
        _assert_same_rows(expected, actual.select(*columns), dataset)
    print(f"Validated PostgreSQL snapshot for {context.batch_id}: {len(DATASETS)} datasets")


def run_warehouse(context: JobContext, contract: dict[str, Any], *, mode: str = "load") -> str:
    options = _jdbc_options()
    marker, marker_body = _read_marker(context)
    if marker.get("terminal_outcome") not in {"PASSED", "PASSED_WITH_REJECTIONS"}:
        raise RuntimeError(f"warehouse load blocked by curation outcome {marker.get('terminal_outcome')}")
    if marker.get("batch_id") != context.batch_id:
        raise RuntimeError("curation marker BatchID mismatch")
    if int(marker.get("contract_version", -1)) != int(contract["contract_version"]):
        raise RuntimeError("curation marker contract version mismatch")
    if marker.get("pipeline_version") != context.config.pipeline_version:
        raise RuntimeError("curation marker pipeline version mismatch")
    verify_marker_outputs(
        context.client, context.config, marker, manifest_fingerprints(context.manifests),
        context.batch_id, int(contract["contract_version"]),
    )
    if tuple(marker.get("produced_datasets", {}).keys()) != DATASETS and set(marker.get("produced_datasets", {})) != set(DATASETS):
        raise RuntimeError("curation marker does not declare all 16 warehouse datasets")
    fingerprint, marker_sha, dataset_evidence = publication_evidence(context, marker, marker_body)
    frames = _curated_frames(context, marker)
    try:
        _validate_frames(context, marker, contract, frames)
        if mode == "validate":
            _validate_published(context, options, contract, marker, frames, fingerprint)
            return "VALIDATED"
        if mode != "load":
            raise ValueError(f"unsupported warehouse mode {mode!r}")
        attempt_id = str(uuid4())
        _event(
            context, options, attempt_id=attempt_id, batch_id=context.batch_id,
            curation_attempt=str(marker["attempt_id"]), event_type="STARTED", fingerprint=fingerprint,
            marker_sha=marker_sha, contract_version=int(marker["contract_version"]),
            pipeline_version=str(marker["pipeline_version"]), details={"mode": "load"},
        )
        try:
            staged = _stage(context, options, frames, attempt_id)
            try:
                _validate_frames(context, marker, contract, staged)
            finally:
                for frame in staged.values():
                    frame.unpersist()
            result = _publish_snapshot(
                context, options, contract, marker, attempt_id, fingerprint, marker_sha, dataset_evidence
            )
        except Exception as exc:
            if isinstance(exc, PublicationConflict):
                raise
            _event(
                context, options, attempt_id=attempt_id, batch_id=context.batch_id,
                curation_attempt=str(marker["attempt_id"]), event_type="FAILED", fingerprint=fingerprint,
                marker_sha=marker_sha, contract_version=int(marker["contract_version"]),
                pipeline_version=str(marker["pipeline_version"]), details={"error": str(exc)},
            )
            raise
        _cleanup_attempt(context, options, attempt_id)
        print(f"PostgreSQL warehouse load {result} for {context.batch_id}: {fingerprint}")
        return result
    finally:
        for frame in frames.values():
            frame.unpersist()
