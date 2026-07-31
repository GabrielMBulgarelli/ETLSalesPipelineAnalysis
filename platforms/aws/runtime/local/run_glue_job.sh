#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
# shellcheck source=glue.env
source "${SCRIPT_DIR}/glue.env"

: "${BATCH_ID:?BATCH_ID is required}"
: "${GLUE_IMAGE:?GLUE_IMAGE must be configured in glue.env}"
: "${GLUE_JOB:=process_raw}"

case "${GLUE_JOB}" in
  process_raw|validate_processed|build_curated) ;;
  *) echo "Unsupported GLUE_JOB: ${GLUE_JOB}" >&2; exit 2 ;;
esac

docker run --rm \
  --network ecommerce-sales-aws-local_default \
  --env AWS_ACCESS_KEY_ID=test \
  --env AWS_SECRET_ACCESS_KEY=test \
  --env AWS_DEFAULT_REGION=us-east-1 \
  --env AWS_ENDPOINT_URL=http://localstack:4566 \
  --env PYTHONPATH=/home/hadoop/workspace/platforms/aws/src \
  --volume "${REPOSITORY_ROOT}:/home/hadoop/workspace:ro,Z" \
  "${GLUE_IMAGE}" \
  -lc 'cp -a /home/hadoop/workspace/platforms/aws /tmp/aws-etl-package && python3 -m pip install --user --quiet /tmp/aws-etl-package && exec spark-submit "$@"' \
  glue-spark-submit \
    "/home/hadoop/workspace/platforms/aws/entrypoints/glue-jobs/${GLUE_JOB}.py" \
    --batch-id "${BATCH_ID}" \
    --config /home/hadoop/workspace/platforms/aws/runtime/local/config.yaml \
    --raw-contract /home/hadoop/workspace/contracts/schemas/raw/datasets.yaml \
    --processed-contract /home/hadoop/workspace/contracts/schemas/processed/datasets.yaml \
    --curated-contract /home/hadoop/workspace/contracts/schemas/curated/datasets.yaml \
    --quality-contract /home/hadoop/workspace/contracts/rules/quality-thresholds.yaml \
    --reference-contract /home/hadoop/workspace/contracts/rules/referential-integrity.yaml
