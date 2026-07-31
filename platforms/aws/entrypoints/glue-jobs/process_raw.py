#!/usr/bin/env python3
"""AWS Glue 5 raw-to-processed job for the nine contracted Olist datasets."""

from __future__ import annotations

import sys

from aws_etl.cleansing import classify_rows
from aws_etl.deduplication import deduplicate
from aws_etl.job_context import build_context, completed_replay_matches, manifest_fingerprints, parse_args
from aws_etl.normalization import append_runtime_metadata, derive_orders, enrich_products, normalize_raw, select_processed_columns
from aws_etl.readers import read_raw_csv
from aws_etl.schemas import DATASETS, assert_processed_schema
from aws_etl.writers import copy_prefix, delete_prefix, existing_summary, publish_summary, verify_staged, write_staged


def main() -> int:
    context = build_context(parse_args())
    try:
        completed = existing_summary(context.client, context.config, context.batch_id)
        if completed is not None:
            completed_replay_matches(context, completed)
            print(f"Batch {context.batch_id} already has a completed matching publication; no-op")
            return 0

        staging_root = f"{context.config.staging_prefix}processed/batch_id={context.batch_id}/"
        delete_prefix(context.client, context.config.bucket, staging_root)
        for dataset in DATASETS:
            delete_prefix(context.client, context.config.bucket, f"{context.config.processed_prefix}{dataset}/batch_id={context.batch_id}/")
            delete_prefix(context.client, context.config.bucket, f"{context.config.rejected_prefix}{dataset}/batch_id={context.batch_id}/")

        accepted, rejected = {}, {}
        source_counts, rejected_counts, exact_duplicates = {}, {}, {}
        for dataset in DATASETS:
            raw = read_raw_csv(context.spark, context.config, dataset, context.manifests[dataset], context.raw_contract)
            source_counts[dataset] = raw.count()
            clean, bad = classify_rows(normalize_raw(raw, dataset, context.raw_contract), dataset, context.raw_contract)
            rejected_counts[dataset] = bad.count()
            accepted[dataset], exact_duplicates[dataset] = deduplicate(clean, dataset, context.processed_contract)
            rejected[dataset] = bad

        accepted["orders"] = derive_orders(accepted["orders"], context.processed_contract)
        accepted["products"] = enrich_products(accepted["products"], accepted["category_translation"],
                                                context.processed_contract["datasets"]["products"]["category_fallback"])

        summaries, staging_prefixes = {}, []
        for dataset in DATASETS:
            output = select_processed_columns(
                append_runtime_metadata(accepted[dataset], context.manifests[dataset], context.processing_timestamp,
                                        int(context.processed_contract["contract_version"])),
                dataset, context.processed_contract).cache()
            assert_processed_schema(dataset, output, context.processed_contract)
            processed_count = output.count()
            if source_counts[dataset] != rejected_counts[dataset] + processed_count + exact_duplicates[dataset]:
                raise RuntimeError(f"row-count reconciliation failed for {dataset}")
            rejected_output = append_runtime_metadata(rejected[dataset], context.manifests[dataset], context.processing_timestamp,
                                                      int(context.processed_contract["contract_version"]))
            raw_names = [field["name"] for field in context.raw_contract["datasets"][dataset]["fields"]]
            rejected_output = rejected_output.select(*raw_names, "rejection_codes", "rejection_descriptions",
                                                      "batch_id", "source_file_id", "ingestion_timestamp",
                                                      "processing_timestamp", "contract_version")
            processed_stage = f"{staging_root}processed/{dataset}/"
            rejected_stage = f"{staging_root}rejected/{dataset}/"
            write_staged(output, context.config.bucket, processed_stage)
            write_staged(rejected_output, context.config.bucket, rejected_stage)
            staging_prefixes.extend((processed_stage, rejected_stage))
            summaries[dataset] = {"source_rows": source_counts[dataset], "processed_rows": processed_count,
                                  "rejected_rows": rejected_counts[dataset], "exact_duplicates_removed": exact_duplicates[dataset],
                                  "processed_schema": output.schema.simpleString()}
            output.unpersist()

        verify_staged(context.client, context.config.bucket, staging_prefixes)
        for dataset in DATASETS:
            copy_prefix(context.client, context.config.bucket, f"{staging_root}processed/{dataset}/",
                        f"{context.config.processed_prefix}{dataset}/batch_id={context.batch_id}/")
            copy_prefix(context.client, context.config.bucket, f"{staging_root}rejected/{dataset}/",
                        f"{context.config.rejected_prefix}{dataset}/batch_id={context.batch_id}/")
        summary = {
            "batch_id": context.batch_id,
            "batch_timestamp": next(iter(context.manifests.values()))["batch_timestamp"],
            "processing_timestamp": context.processing_timestamp,
            "raw_contract_version": context.raw_contract["contract_version"],
            "processed_contract_version": context.processed_contract["contract_version"],
            "manifest_content_sha256": manifest_fingerprints(context.manifests),
            "datasets": summaries,
            "totals": {"source_rows": sum(source_counts.values()),
                       "processed_rows": sum(item["processed_rows"] for item in summaries.values()),
                       "rejected_rows": sum(rejected_counts.values()),
                       "exact_duplicates_removed": sum(exact_duplicates.values())},
            "publication_status": "COMPLETED",
        }
        publish_summary(context.client, context.config, context.batch_id, summary)
        delete_prefix(context.client, context.config.bucket, staging_root)
        print(f"Published batch {context.batch_id}: {summary['totals']}")
        return 0
    finally:
        context.spark.stop()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
