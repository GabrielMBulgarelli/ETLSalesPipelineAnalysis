"""Glue processing job arguments, timestamps, Spark, and replay context."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import AwsEtlConfig, load_config
from .readers import load_batch_manifests
from .schemas import load_contract
from .storage import s3_client
from .writers import verify_marker_outputs


@dataclass(frozen=True)
class JobContext:
    batch_id: str
    processing_timestamp: str
    config: AwsEtlConfig
    client: Any
    spark: Any
    raw_contract: dict[str, Any]
    processed_contract: dict[str, Any]
    manifests: dict[str, dict[str, Any]]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process one manifested raw Olist batch")
    parser.add_argument("--batch-id", "--BATCH_ID", dest="batch_id", required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--raw-contract", type=Path, required=True)
    parser.add_argument("--processed-contract", type=Path, required=True)
    parser.add_argument("--curated-contract", type=Path)
    parser.add_argument("--quality-contract", type=Path)
    parser.add_argument("--reference-contract", type=Path)
    parser.add_argument("--SUBMISSIONS_JSON")
    parser.add_argument("--BUCKET")
    parser.add_argument("--RAW_PREFIX")
    parser.add_argument("--PROCESSED_PREFIX")
    parser.add_argument("--CURATED_PREFIX")
    parser.add_argument("--REJECTED_PREFIX")
    parser.add_argument("--QUALITY_PREFIX")
    parser.add_argument("--STAGING_PREFIX")
    parser.add_argument("--MANIFEST_PREFIX")
    parser.add_argument("--AUDIT_PREFIX")
    parser.add_argument("--CONTRACT_VERSION")
    parser.add_argument("--PIPELINE_VERSION")
    return parser.parse_args()


def create_spark(config: AwsEtlConfig):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("ecommerce-sales-process-raw").config("spark.sql.session.timeZone", "UTC")
    if config.endpoint_url:
        builder = (builder.config("spark.hadoop.fs.s3a.endpoint", config.endpoint_url)
                   .config("spark.hadoop.fs.s3a.path.style.access", "true")
                   .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                   .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
                   .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                   .config("spark.hadoop.fs.s3a.access.key", config.aws_access_key_id)
                   .config("spark.hadoop.fs.s3a.secret.key", config.aws_secret_access_key))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_context(args: argparse.Namespace) -> JobContext:
    config = load_config(args.config)
    client = s3_client(config)
    raw_contract = load_contract(args.raw_contract, "raw")
    processed_contract = load_contract(args.processed_contract, "processed")
    manifests = load_batch_manifests(client, config, args.batch_id)
    expected_runtime = {
        "BUCKET": config.bucket,
        "RAW_PREFIX": config.raw_prefix,
        "PROCESSED_PREFIX": config.processed_prefix,
        "CURATED_PREFIX": config.curated_prefix,
        "REJECTED_PREFIX": config.rejected_prefix,
        "QUALITY_PREFIX": config.quality_prefix,
        "STAGING_PREFIX": config.staging_prefix,
        "MANIFEST_PREFIX": config.manifest_prefix,
        "AUDIT_PREFIX": config.audit_prefix,
        "CONTRACT_VERSION": str(processed_contract["contract_version"]),
        "PIPELINE_VERSION": config.pipeline_version,
    }
    for argument, expected in expected_runtime.items():
        supplied = getattr(args, argument)
        if supplied is not None and supplied != expected:
            raise ValueError(f"orchestration argument {argument} does not match the active runtime configuration")
    if args.SUBMISSIONS_JSON is not None:
        submissions = json.loads(args.SUBMISSIONS_JSON)
        if not isinstance(submissions, list) or len(submissions) != len(manifests):
            raise ValueError("orchestration submissions do not match the manifested batch")
        identity = {
            str(item.get("dataset")): str(item.get("manifest", {}).get("content_sha256"))
            for item in submissions
            if isinstance(item, dict)
        }
        if identity != manifest_fingerprints(manifests):
            raise ValueError("orchestration submission identity does not match the manifested batch")
    return JobContext(args.batch_id, datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"), config, client,
                      create_spark(config), raw_contract, processed_contract, manifests)


def manifest_fingerprints(manifests: dict[str, dict[str, Any]]) -> dict[str, str]:
    return {dataset: str(manifest["content_sha256"]) for dataset, manifest in sorted(manifests.items())}


def completed_replay_matches(context: JobContext, summary: dict[str, Any]) -> bool:
    identity = manifest_fingerprints(context.manifests)
    verify_marker_outputs(
        context.client,
        context.config,
        summary,
        identity,
        context.batch_id,
        int(context.processed_contract["contract_version"]),
    )
    expected = {f"processed:{dataset}" for dataset in context.manifests} | {
        f"rejected:{dataset}" for dataset in context.manifests
    }
    if set(summary["produced_datasets"]) != expected:
        raise RuntimeError("processed completion marker does not declare the expected outputs")
    return True
