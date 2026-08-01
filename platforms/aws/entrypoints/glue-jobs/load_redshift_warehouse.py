#!/usr/bin/env python3
"""Deployment-ready managed Redshift loader; never used by the local PostgreSQL substitute."""

from __future__ import annotations

import json
import sys

import yaml

from aws_etl.job_context import build_context, parse_args
from aws_etl.redshift_warehouse import run_redshift_warehouse
from aws_etl.schemas import load_contract


def main() -> int:
    args = parse_args()
    if args.curated_contract is None:
        raise ValueError("curated contract path is required")
    if args.redshift_policy is None:
        raise ValueError("Redshift warehouse policy path is required")
    context = build_context(args)
    try:
        policy = yaml.safe_load(args.redshift_policy.read_text(encoding="utf-8"))
        if not isinstance(policy, dict):
            raise ValueError("invalid Redshift warehouse policy")
        if policy.get("policy_id") != "redshift-scd2-v1":
            raise ValueError("unsupported Redshift warehouse policy")
        result = run_redshift_warehouse(
            context,
            load_contract(args.curated_contract, "curated"),
            load_attempt_id=args.LOAD_ATTEMPT_ID,
            retry_count=args.WAREHOUSE_RETRY_COUNT,
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    finally:
        context.spark.stop()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
