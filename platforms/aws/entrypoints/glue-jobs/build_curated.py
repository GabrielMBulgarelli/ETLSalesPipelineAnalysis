#!/usr/bin/env python3
"""Build and atomically publish one validated curated batch."""

from __future__ import annotations

import sys
from uuid import uuid4

from aws_etl.aggregations import build_aggregations
from aws_etl.dimensions import build_dimensions
from aws_etl.facts import build_facts, representative_payments
from aws_etl.integrity import assert_curated_content, assert_unique_grain, conform_curated
from aws_etl.job_context import build_context, manifest_fingerprints, parse_args
from aws_etl.schemas import DATASETS, assert_curated_schema, assert_processed_schema, load_contract
from aws_etl.writers import (
    copy_prefix, delete_prefix, existing_terminal_summary, publish_terminal_summary,
    verify_marker_outputs, verify_staged, write_staged,
)


DIMENSIONS = ("dim_customer", "dim_product", "dim_seller", "dim_geography", "dim_date", "dim_order_status")
FACTS = ("fact_sales", "fact_reviews")
AGGREGATIONS = (
    "sales_by_state", "sales_by_category", "monthly_sales", "order_status",
    "cross_state_analysis", "seller_performance", "size_analysis", "payment_methods",
)


def _layer(dataset: str) -> str:
    return "dimensions" if dataset in DIMENSIONS else "facts" if dataset in FACTS else "aggregations"


def _verify_reconciliation(fact_sales, aggregates) -> None:
    from pyspark.sql import functions as F

    for dataset in AGGREGATIONS:
        expected = (
            fact_sales.filter(F.col("PaymentType").isNotNull())
            if dataset == "payment_methods"
            else fact_sales
        ).agg(F.sum("Price").alias("value")).first()["value"]
        actual = aggregates[dataset].agg(F.sum("TotalSales").alias("value")).first()["value"]
        if actual != expected:
            raise RuntimeError(
                f"aggregate reconciliation failed for {dataset}: {actual} != {expected}"
            )
def _verify_representative_payments(fact_sales, payments) -> None:
    from pyspark.sql import functions as F

    expected = representative_payments(payments).select(
        F.col("order_id").alias("ExpectedOrderID"),
        F.col("PaymentType").alias("ExpectedPaymentType"),
    )
    actual = fact_sales.select("OrderID", "PaymentType").distinct()
    mismatches = actual.join(
        expected, F.col("OrderID") == F.col("ExpectedOrderID"), "left"
    ).filter(~F.col("PaymentType").eqNullSafe(F.col("ExpectedPaymentType")))
    if mismatches.limit(1).count():
        raise RuntimeError("fact_sales representative payment does not use the first (payment_sequential, payment_type)")


def _verify_published(context, summary, contract) -> None:
    if summary.get("terminal_outcome") not in {"PASSED", "PASSED_WITH_REJECTIONS"}:
        raise RuntimeError(f"invalid curation terminal outcome: {summary.get('terminal_outcome')}")
    verify_marker_outputs(
        context.client, context.config, summary, manifest_fingerprints(context.manifests),
        context.batch_id, contract["contract_version"],
    )
    expected = set(DIMENSIONS + FACTS + AGGREGATIONS)
    actual = set(summary["produced_datasets"])
    if actual != expected:
        raise RuntimeError(f"curation marker dataset mismatch: {sorted(actual)} != {sorted(expected)}")
    marker_counts = {
        dataset: metadata["row_count"]
        for dataset, metadata in summary["produced_datasets"].items()
    }
    if summary.get("row_counts") != marker_counts:
        raise RuntimeError("curation marker row-count summary disagrees with produced datasets")
    published = {}
    for dataset, metadata in sorted(summary["produced_datasets"].items()):
        frame = context.spark.read.parquet(f"s3a://{context.config.bucket}/{metadata['prefix']}")
        assert_curated_schema(dataset, frame, contract)
        assert_curated_content(frame, dataset, contract)
        if frame.count() != metadata["row_count"]:
            raise RuntimeError(f"curation marker/output row-count mismatch for {dataset}")
        grain = contract["datasets"][dataset].get("grain") or contract["datasets"][dataset].get("business_key")
        assert_unique_grain(frame, dataset, grain)
        published[dataset] = frame
    _verify_reconciliation(
        published["fact_sales"], {dataset: published[dataset] for dataset in AGGREGATIONS}
    )


def main() -> int:
    args = parse_args()
    context = build_context(args)
    try:
        if args.curated_contract is None:
            raise ValueError("curated contract path is required")
        curated_contract = load_contract(args.curated_contract, "curated")
        identity = manifest_fingerprints(context.manifests)
        completed = existing_terminal_summary(context.client, context.config, context.batch_id, "curation")
        if completed is not None:
            _verify_published(context, completed, curated_contract)
            print(f"Curation for {context.batch_id} already completed consistently; no-op")
            return 0

        validation = existing_terminal_summary(context.client, context.config, context.batch_id, "validation")
        if validation is None:
            raise RuntimeError("validation completion marker is missing")
        verify_marker_outputs(
            context.client, context.config, validation, identity, context.batch_id,
            context.processed_contract["contract_version"],
        )
        if validation.get("terminal_outcome") not in {"PASSED", "PASSED_WITH_REJECTIONS"}:
            raise RuntimeError(f"curation blocked by validation outcome {validation.get('terminal_outcome')}")

        frames = {}
        for dataset in DATASETS:
            prefix = validation["produced_datasets"][f"valid:{dataset}"]["prefix"]
            frame = context.spark.read.parquet(f"s3a://{context.config.bucket}/{prefix}")
            assert_processed_schema(dataset, frame, context.processed_contract)
            frames[dataset] = frame

        batch_timestamp = next(iter(context.manifests.values()))["batch_timestamp"]
        dimensions = build_dimensions(frames, batch_timestamp)
        facts = build_facts(frames)
        aggregates = build_aggregations(facts["fact_sales"], dimensions, batch_timestamp)
        _verify_representative_payments(facts["fact_sales"], frames["order_payments"])
        _verify_reconciliation(facts["fact_sales"], aggregates)
        built = {**dimensions, **facts, **aggregates}
        expected = DIMENSIONS + FACTS + AGGREGATIONS
        if tuple(built) != expected:
            raise RuntimeError(f"required curated datasets mismatch: {tuple(built)}")

        outputs, counts = {}, {}
        staging_root = f"{context.config.staging_prefix}curated/batch_id={context.batch_id}/"
        delete_prefix(context.client, context.config.bucket, staging_root)
        staged = []
        for dataset in expected:
            output = conform_curated(built[dataset], dataset, curated_contract).cache()
            assert_curated_schema(dataset, output, curated_contract)
            assert_curated_content(output, dataset, curated_contract)
            grain = curated_contract["datasets"][dataset].get("grain") or curated_contract["datasets"][dataset].get("business_key")
            assert_unique_grain(output, dataset, grain)
            counts[dataset] = output.count()
            stage_prefix = f"{staging_root}{_layer(dataset)}/{dataset}/"
            write_staged(output, context.config.bucket, stage_prefix)
            staged.append(stage_prefix)
            output.unpersist()
        verify_staged(context.client, context.config.bucket, staged)

        for dataset in expected:
            final_prefix = f"{context.config.curated_prefix}{_layer(dataset)}/{dataset}/batch_id={context.batch_id}/"
            delete_prefix(context.client, context.config.bucket, final_prefix)
            copy_prefix(context.client, context.config.bucket, f"{staging_root}{_layer(dataset)}/{dataset}/", final_prefix)
            outputs[dataset] = {"prefix": final_prefix, "row_count": counts[dataset]}

        summary = {
            "batch_id": context.batch_id,
            "attempt_id": str(uuid4()),
            "source_content_identity": identity,
            "contract_version": curated_contract["contract_version"],
            "pipeline_version": context.config.pipeline_version,
            "terminal_outcome": "PASSED_WITH_REJECTIONS" if validation["terminal_outcome"] == "PASSED_WITH_REJECTIONS" else "PASSED",
            "produced_datasets": outputs,
            "row_counts": counts,
            "validation_attempt_id": validation["attempt_id"],
        }
        publish_terminal_summary(context.client, context.config, context.batch_id, "curation", summary)
        delete_prefix(context.client, context.config.bucket, staging_root)
        print(f"Published {len(outputs)} curated datasets for batch {context.batch_id}")
        return 0
    finally:
        context.spark.stop()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
