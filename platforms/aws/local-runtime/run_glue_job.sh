#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
# shellcheck source=glue.env
source "${SCRIPT_DIR}/glue.env"

: "${BATCH_ID:?BATCH_ID is required}"
: "${GLUE_IMAGE:?GLUE_IMAGE must be configured in glue.env}"
: "${GLUE_JOB:=process_raw}"

case "${GLUE_JOB}" in
  process_raw|validate_processed|build_curated|load_warehouse) ;;
  *) echo "Unsupported GLUE_JOB: ${GLUE_JOB}" >&2; exit 2 ;;
esac

docker run --rm \
  --platform linux/amd64 \
  --network ecommerce-sales-aws-local_default \
  --env AWS_ACCESS_KEY_ID=test \
  --env AWS_SECRET_ACCESS_KEY=test \
  --env AWS_DEFAULT_REGION=us-east-1 \
  --env AWS_ENDPOINT_URL=http://localstack:4566 \
  --env POSTGRES_HOST=postgres \
  --env POSTGRES_PORT=5432 \
  --env POSTGRES_DB="${POSTGRES_DB:-ecommerce_sales}" \
  --env POSTGRES_ETL_USER="${POSTGRES_ETL_USER:-ecommerce_etl}" \
  --env POSTGRES_ETL_PASSWORD="${POSTGRES_ETL_PASSWORD:-}" \
  --env AWS_ETL_WAREHOUSE_MODE="${AWS_ETL_WAREHOUSE_MODE:-load}" \
  --env AWS_ETL_WAREHOUSE_FAIL_AFTER="${AWS_ETL_WAREHOUSE_FAIL_AFTER:-}" \
  --env AWS_ETL_WAREHOUSE_ALLOW_INJECTION="${AWS_ETL_WAREHOUSE_ALLOW_INJECTION:-}" \
  --env AWS_ETL_WAREHOUSE_FINGERPRINT_OVERRIDE="${AWS_ETL_WAREHOUSE_FINGERPRINT_OVERRIDE:-}" \
  --env PYTHONPATH=/home/hadoop/workspace/platforms/aws/src \
  --volume "${REPOSITORY_ROOT}:/home/hadoop/workspace:ro,Z" \
  "${GLUE_IMAGE}" \
  -lc 'cp -a /home/hadoop/workspace/platforms/aws /tmp/aws-etl-package && python3 -m pip install --user --quiet /tmp/aws-etl-package && exec spark-submit "$@"' \
  glue-spark-submit \
    "/home/hadoop/workspace/platforms/aws/glue-jobs/${GLUE_JOB}.py" \
    --batch-id "${BATCH_ID}" \
    --config /home/hadoop/workspace/platforms/aws/local-runtime/config.yaml \
    --raw-contract /home/hadoop/workspace/contracts/schemas/raw/datasets.yaml \
    --processed-contract /home/hadoop/workspace/contracts/schemas/processed/datasets.yaml \
    --curated-contract /home/hadoop/workspace/contracts/schemas/curated/datasets.yaml \
    --quality-contract /home/hadoop/workspace/contracts/rules/quality-thresholds.yaml \
    --reference-contract /home/hadoop/workspace/contracts/rules/referential-integrity.yaml
