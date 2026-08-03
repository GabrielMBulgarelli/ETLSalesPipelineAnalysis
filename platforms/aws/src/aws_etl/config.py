"""Configuration loading and validation for local and managed AWS runtimes."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

import yaml


DEFAULTS: dict[str, str] = {
    "environment": "local",
    "endpoint_url": "http://localhost:4566",
    "region": "us-east-1",
    "bucket": "ecommerce-sales-local",
    "raw_prefix": "raw/",
    "manifest_prefix": "manifests/",
    "audit_prefix": "audit/",
    "processed_prefix": "processed/",
    "curated_prefix": "curated/",
    "rejected_prefix": "rejected/",
    "quality_prefix": "quality/",
    "staging_prefix": "staging/",
    "pipeline_version": "1.0.0",
    "contract_version": "1",
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
}

ENVIRONMENT_KEYS: dict[str, tuple[str, ...]] = {
    "environment": ("AWS_ETL_ENVIRONMENT",),
    "endpoint_url": ("AWS_ENDPOINT_URL", "AWS_ETL_ENDPOINT_URL"),
    "region": ("AWS_DEFAULT_REGION", "AWS_REGION", "AWS_ETL_REGION"),
    "bucket": ("CUSTOMER_AWS_ETL_BUCKET", "AWS_ETL_BUCKET"),
    "raw_prefix": ("AWS_ETL_RAW_PREFIX",),
    "manifest_prefix": ("AWS_ETL_MANIFEST_PREFIX",),
    "audit_prefix": ("AWS_ETL_AUDIT_PREFIX",),
    "processed_prefix": ("AWS_ETL_PROCESSED_PREFIX",),
    "curated_prefix": ("AWS_ETL_CURATED_PREFIX",),
    "rejected_prefix": ("AWS_ETL_REJECTED_PREFIX",),
    "quality_prefix": ("AWS_ETL_QUALITY_PREFIX",),
    "staging_prefix": ("AWS_ETL_STAGING_PREFIX",),
    "pipeline_version": ("AWS_ETL_PIPELINE_VERSION",),
    "contract_version": ("AWS_ETL_CONTRACT_VERSION",),
    "aws_access_key_id": ("AWS_ACCESS_KEY_ID",),
    "aws_secret_access_key": ("AWS_SECRET_ACCESS_KEY",),
    "aws_session_token": ("AWS_SESSION_TOKEN",),
}

DUMMY_CREDENTIALS = {"test", "dummy", "local", "localstack"}
LOCAL_BUCKET = "ecommerce-sales-local"


def _prefix(value: str, name: str) -> str:
    cleaned = value.strip().strip("/")
    if not cleaned:
        raise ValueError(f"{name} must not be empty")
    return f"{cleaned}/"


@dataclass(frozen=True)
class AwsEtlConfig:
    environment: str
    endpoint_url: str | None
    region: str
    bucket: str
    raw_prefix: str
    manifest_prefix: str
    audit_prefix: str
    processed_prefix: str
    curated_prefix: str
    rejected_prefix: str
    quality_prefix: str
    staging_prefix: str
    pipeline_version: str
    contract_version: str
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None = None

    def validate(self) -> None:
        if not self.environment:
            raise ValueError("environment must not be empty")
        if not self.region:
            raise ValueError("region must not be empty")
        if not self.bucket:
            raise ValueError("bucket must not be empty")
        if not self.pipeline_version:
            raise ValueError("pipeline_version must not be empty")
        if not self.contract_version:
            raise ValueError("contract_version must not be empty")
        if bool(self.aws_access_key_id) != bool(self.aws_secret_access_key):
            raise ValueError("AWS access key ID and secret access key must be supplied together")

        credential_values = {
            value.lower()
            for value in (self.aws_access_key_id, self.aws_secret_access_key)
            if value
        }
        if self.environment == "local":
            if self.bucket != LOCAL_BUCKET:
                raise ValueError(f"local environment bucket must be {LOCAL_BUCKET!r}")
            if not self.endpoint_url:
                raise ValueError("local environment requires endpoint_url")
            parsed = urlparse(self.endpoint_url)
            if parsed.scheme not in {"http", "https"} or parsed.hostname not in {
                "localhost",
                "127.0.0.1",
                "localstack",
            }:
                raise ValueError("local endpoint_url must address localhost, 127.0.0.1, or localstack")
            if not self.aws_access_key_id:
                raise ValueError("local environment requires explicit LocalStack credentials")
        elif credential_values & DUMMY_CREDENTIALS:
            raise ValueError("dummy AWS credentials are permitted only when environment=local")


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"configuration file must contain a mapping: {path}")
    unknown = sorted(set(loaded) - set(DEFAULTS) - {"aws_session_token"})
    if unknown:
        raise ValueError(f"unknown configuration keys in {path}: {', '.join(unknown)}")
    return loaded


def load_config(
    config_path: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> AwsEtlConfig:
    """Load package defaults, then YAML, then environment variables."""
    environment = os.environ if environ is None else environ
    if config_path is None:
        configured_path = environment.get("AWS_ETL_CONFIG_FILE")
        config_path = configured_path or Path(__file__).resolve().parents[2] / "local-runtime/config.yaml"
    values: dict[str, Any] = {**DEFAULTS, **_read_yaml(Path(config_path))}
    for key, variable_names in ENVIRONMENT_KEYS.items():
        for variable_name in variable_names:
            value = environment.get(variable_name)
            if value is not None and value != "":
                values[key] = value
                break

    config = AwsEtlConfig(
        environment=str(values["environment"]).strip().lower(),
        endpoint_url=str(values["endpoint_url"]).strip() if values.get("endpoint_url") else None,
        region=str(values["region"]).strip(),
        bucket=str(values["bucket"]).strip(),
        raw_prefix=_prefix(str(values["raw_prefix"]), "raw_prefix"),
        manifest_prefix=_prefix(str(values["manifest_prefix"]), "manifest_prefix"),
        audit_prefix=_prefix(str(values["audit_prefix"]), "audit_prefix"),
        processed_prefix=_prefix(str(values["processed_prefix"]), "processed_prefix"),
        curated_prefix=_prefix(str(values["curated_prefix"]), "curated_prefix"),
        rejected_prefix=_prefix(str(values["rejected_prefix"]), "rejected_prefix"),
        quality_prefix=_prefix(str(values["quality_prefix"]), "quality_prefix"),
        staging_prefix=_prefix(str(values["staging_prefix"]), "staging_prefix"),
        pipeline_version=str(values["pipeline_version"]).strip(),
        contract_version=str(values["contract_version"]).strip(),
        aws_access_key_id=str(values["aws_access_key_id"]).strip() if values.get("aws_access_key_id") else None,
        aws_secret_access_key=str(values["aws_secret_access_key"]).strip() if values.get("aws_secret_access_key") else None,
        aws_session_token=str(values["aws_session_token"]).strip() if values.get("aws_session_token") else None,
    )
    config.validate()
    return config
