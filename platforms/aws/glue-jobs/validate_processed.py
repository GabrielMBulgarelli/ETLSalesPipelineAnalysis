#!/usr/bin/env python3
"""Validate one processed batch and publish a terminal quality marker."""

from __future__ import annotations

import sys
from uuid import uuid4

import yaml

from aws_etl.job_context import build_context, completed_replay_matches, manifest_fingerprints, parse_args
from aws_etl.quality import validate_processed
from aws_etl.schemas import DATASETS, assert_processed_schema
from aws_etl.writers import (
    copy_prefix, delete_prefix, existing_summary, existing_terminal_summary,
    publish_terminal_summary, verify_marker_outputs, verify_staged, write_staged,
)


def _rule_contract(path, name: str):
    if path is None:
        raise ValueError(f"{name} contract path is required")
    with path.open(encoding="utf-8") as handle:
        document = yaml.safe_load(handle)
    if not isinstance(document, dict) or document.get("contract_version") != 1:
        raise ValueError(f"invalid_contract_version: {name}")
    return document


def _verify_counts(spark, bucket: str, outputs: dict, row_counts: dict, processed_contract) -> None:
    expected = {
        f"{kind}:{dataset}"
        for dataset in DATASETS
        for kind in ("valid", "rejected")
    }
    if set(outputs) != expected:
        raise RuntimeError(
            f"validation marker dataset mismatch: {sorted(outputs)} != {sorted(expected)}"
        )
    for dataset, metadata in sorted(outputs.items()):
        frame = spark.read.parquet(f"s3a://{bucket}/{metadata['prefix']}")
        kind, dataset_name = dataset.split(":", 1)
        if kind == "valid":
            assert_processed_schema(dataset_name, frame, processed_contract)
        marker_count = row_counts.get(dataset_name, {}).get(f"{kind}_rows")
        if marker_count != metadata["row_count"]:
            raise RuntimeError(
                f"validation marker row-count summaries disagree for {dataset}: "
                f"{marker_count} != {metadata['row_count']}"
            )
        actual = frame.count()
        if actual != metadata["row_count"]:
            raise RuntimeError(f"completion marker/output row-count mismatch for {dataset}: {actual} != {metadata['row_count']}")


def main() -> int:
    args = parse_args()
    context = build_context(args)
    try:
        identity = manifest_fingerprints(context.manifests)
        completed = existing_terminal_summary(context.client, context.config, context.batch_id, "validation")
        if completed is not None:
            verify_marker_outputs(
                context.client, context.config, completed, identity, context.batch_id,
                context.processed_contract["contract_version"],
            )
            _verify_counts(
                context.spark, context.config.bucket, completed["produced_datasets"],
                completed.get("row_counts", {}), context.processed_contract,
            )
            if completed.get("terminal_outcome") not in {"PASSED", "PASSED_WITH_REJECTIONS", "FAILED"}:
                raise RuntimeError(f"invalid validation terminal outcome: {completed.get('terminal_outcome')}")
            print(f"Validation for {context.batch_id} reused terminal {completed['terminal_outcome']}")
            return 2 if completed["terminal_outcome"] == "FAILED" else 0

        processed_summary = existing_summary(context.client, context.config, context.batch_id)
        if processed_summary is None:
            raise RuntimeError("processed completion marker is missing")
        completed_replay_matches(context, processed_summary)
        quality_contract = _rule_contract(args.quality_contract, "quality")
        reference_contract = _rule_contract(args.reference_contract, "referential integrity")

        frames = {}
        for dataset in DATASETS:
            prefix = f"{context.config.processed_prefix}{dataset}/batch_id={context.batch_id}/"
            frame = context.spark.read.parquet(f"s3a://{context.config.bucket}/{prefix}")
            assert_processed_schema(dataset, frame, context.processed_contract)
            frames[dataset] = frame
        warnings = sum(
            int(item.get("exact_duplicates_removed", 0))
            for item in processed_summary.get("datasets", {}).values()
        )
        result = validate_processed(
            frames, context.processed_contract, quality_contract, reference_contract, warnings
        )

        staging_root = f"{context.config.staging_prefix}validation/batch_id={context.batch_id}/"
        validated_root = f"{context.config.staging_prefix}validated/batch_id={context.batch_id}/"
        delete_prefix(context.client, context.config.bucket, staging_root)
        delete_prefix(context.client, context.config.bucket, validated_root)
        staged, outputs = [], {}
        for dataset in DATASETS:
            valid_prefix = f"{validated_root}{dataset}/"
            rejected_stage = f"{staging_root}rejected/{dataset}/"
            rejected_prefix = f"{context.config.rejected_prefix}{dataset}/batch_id={context.batch_id}/validation/"
            delete_prefix(context.client, context.config.bucket, rejected_prefix)
            write_staged(result.valid[dataset], context.config.bucket, valid_prefix)
            write_staged(result.rejected[dataset], context.config.bucket, rejected_stage)
            staged.extend((valid_prefix, rejected_stage))
            outputs[f"valid:{dataset}"] = {
                "prefix": valid_prefix, "row_count": result.summary["datasets"][dataset]["valid_rows"]
            }
            outputs[f"rejected:{dataset}"] = {
                "prefix": rejected_prefix, "row_count": result.summary["datasets"][dataset]["rejected_rows"]
            }
        verify_staged(context.client, context.config.bucket, staged)
        for dataset in DATASETS:
            rejected_stage = f"{staging_root}rejected/{dataset}/"
            rejected_prefix = f"{context.config.rejected_prefix}{dataset}/batch_id={context.batch_id}/validation/"
            copy_prefix(context.client, context.config.bucket, rejected_stage, rejected_prefix)

        summary = {
            "batch_id": context.batch_id,
            "attempt_id": str(uuid4()),
            "source_content_identity": identity,
            "contract_version": context.processed_contract["contract_version"],
            "pipeline_version": context.config.pipeline_version,
            "terminal_outcome": result.summary["outcome"],
            "produced_datasets": outputs,
            "row_counts": result.summary["datasets"],
            "quality": {key: value for key, value in result.summary.items() if key != "datasets"},
        }
        publish_terminal_summary(context.client, context.config, context.batch_id, "validation", summary)
        delete_prefix(context.client, context.config.bucket, staging_root)
        print(f"Validated batch {context.batch_id}: {summary['terminal_outcome']}")
        return 2 if summary["terminal_outcome"] == "FAILED" else 0
    finally:
        context.spark.stop()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
