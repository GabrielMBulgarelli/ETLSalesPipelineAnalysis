"""Glue processing job arguments, timestamps, Spark, and replay context."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import AwsEtlConfig, load_config
from .readers import load_batch_manifests
from .schemas import load_contract
from .storage import s3_client
from .writers import parquet_keys


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
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--raw-contract", type=Path, required=True)
    parser.add_argument("--processed-contract", type=Path, required=True)
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
    return JobContext(args.batch_id, datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"), config, client,
                      create_spark(config), load_contract(args.raw_contract, "raw"), load_contract(args.processed_contract, "processed"),
                      load_batch_manifests(client, config, args.batch_id))


def manifest_fingerprints(manifests: dict[str, dict[str, Any]]) -> dict[str, str]:
    return {dataset: str(manifest["content_sha256"]) for dataset, manifest in sorted(manifests.items())}


def completed_replay_matches(context: JobContext, summary: dict[str, Any]) -> bool:
    if summary.get("manifest_content_sha256") != manifest_fingerprints(context.manifests):
        raise RuntimeError("completed publication exists with different manifest identities")
    for dataset in context.manifests:
        for layer_prefix in (context.config.processed_prefix, context.config.rejected_prefix):
            prefix = f"{layer_prefix}{dataset}/batch_id={context.batch_id}/"
            if not parquet_keys(context.client, context.config.bucket, prefix):
                layer = layer_prefix.rstrip("/").rsplit("/", 1)[-1]
                raise RuntimeError(f"completed publication marker exists but {layer} output is missing for {dataset}")
    return True
