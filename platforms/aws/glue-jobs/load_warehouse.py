#!/usr/bin/env python3
"""Load or validate the local PostgreSQL warehouse from a completed curated batch."""

from __future__ import annotations

import sys

from aws_etl.job_context import build_context, parse_args
from aws_etl.schemas import load_contract
from aws_etl.warehouse import run_warehouse


def main() -> int:
    args = parse_args()
    if args.curated_contract is None:
        raise ValueError("curated contract path is required")
    context = build_context(args)
    try:
        contract = load_contract(args.curated_contract, "curated")
        import os
        run_warehouse(context, contract, mode=os.environ.get("AWS_ETL_WAREHOUSE_MODE", "load"))
        return 0
    finally:
        context.spark.stop()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
