#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
aws_root="$(cd -- "${script_dir}/../.." && pwd)"
compose_file="${script_dir}/docker-compose.yml"

docker compose -f "${compose_file}" up -d localstack

for attempt in $(seq 1 45); do
    if curl -fsS http://localhost:4566/_localstack/health >/dev/null 2>&1; then
        break
    fi
    if [ "${attempt}" -eq 45 ]; then
        echo "LocalStack did not become healthy within 45 seconds." >&2
        docker compose -f "${compose_file}" ps >&2
        exit 1
    fi
    sleep 1
done

PYTHONPATH="${aws_root}/src${PYTHONPATH:+:${PYTHONPATH}}" \
    python3 "${script_dir}/seed_s3.py" --config "${script_dir}/config.yaml" --bootstrap-only
