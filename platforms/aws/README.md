# AWS-local raw-to-curated pipeline

Phases 3 through 6 provide a local S3-compatible ingestion and AWS Glue 5 path from the nine Olist source files through validated processed data, curated dimensions and facts, eight aggregates, and explicit curated catalog metadata. LocalStack and the Glue container do not validate managed AWS IAM, networking, scaling, durability, or orchestration behavior.

## Prerequisites

- Linux x86_64 (the supported local host)
- Docker Engine with Docker Compose v2 (Podman compatibility is best-effort)
- Python 3.11 or newer
- `curl`

Install the package once in a virtual environment:

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -e platforms/aws
```

The complete Olist dataset stays outside Git. Do not copy it into this repository.

## Run locally

```bash
make aws-local-up
make aws-local-seed DATASET_DIR=/absolute/path/to/olist BATCH_ID=my-batch
make aws-local-process BATCH_ID=my-batch
make aws-local-validate BATCH_ID=my-batch
make aws-local-curate BATCH_ID=my-batch
make aws-local-status
make aws-local-down
```

The Glue image is configured once in [`runtime/local/glue.env`](runtime/local/glue.env). It is the official x86-64 Glue 5.0.9 image pinned by digest. The job uses Python 3.11, Java 17, Spark 3.5.4, and UTC. Monetary fields remain `decimal(10,2)`; unqualified logical decimals use `decimal(38,18)`. CSV schema inference is never used.

`aws-local-up` and `aws-local-status` are safe to rerun. `aws-local-down` stops the service without deleting the named LocalStack volume. Seeding creates a new batch ID unless `BATCH_ID` is supplied. For a controlled replay:

```bash
make aws-local-seed DATASET_DIR=/absolute/path/to/olist BATCH_ID=my-batch
```

When reusing a batch ID, the runtime reuses its original batch timestamp unless `BATCH_TIMESTAMP` is explicitly supplied. Supplying changed immutable content under that batch ID fails the batch.

## Accepted source files

`DATASET_DIR` is searched recursively and must contain exactly one file with each case-sensitive canonical name:

- `olist_customers_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `product_category_name_translation.csv`

Missing, duplicate, case-ambiguous, and unexpected CSV files are reported before any raw, manifest, or audit object is uploaded. The filenames map respectively to the provider-neutral datasets `customers`, `orders`, `order_items`, `order_payments`, `order_reviews`, `products`, `sellers`, `geolocation`, and `category_translation`.

## Configuration and local credentials

Configuration precedence is:

1. environment variables;
2. [`runtime/local/config.yaml`](runtime/local/config.yaml);
3. package defaults.

The useful environment variables are documented in the root [`.env.example`](../../.env.example). The committed local configuration uses `test`/`test` credentials and the LocalStack endpoint. These dummy credentials are allowed only when `environment=local`; configuration validation rejects them for any non-local profile. The local profile also requires the fixed `ecommerce-sales-local` bucket and a loopback or LocalStack endpoint.

## S3 layout

```text
s3://ecommerce-sales-local/
├── raw/<dataset>/
├── processed/<dataset>/
├── curated/dimensions/
├── curated/facts/
├── curated/aggregations/
├── rejected/<dataset>/
├── quality/
├── manifests/
├── audit/
└── staging/
```

Processed Parquet is published at `processed/<dataset>/batch_id=<batch-id>/`. Each processed row contains contracted business columns followed by `batch_id`, `source_file_id`, `ingestion_timestamp`, `processing_timestamp`, and `contract_version`. Malformed rows are written once at `rejected/<dataset>/batch_id=<batch-id>/` with sorted, aligned reason-code and description arrays.

The job first writes and verifies every dataset under a batch-specific staging prefix, copies complete output into final prefixes, and publishes `quality/batch_id=<batch-id>/processed-summary.json` last. This immutable summary is the completion marker. A completed matching replay is a no-op; only an incomplete matching publication is replaced.

Phase 5 publishes `validation-summary.json` last after deterministic validation and `curation-summary.json` last after every curated output has been staged, schema/grain/count checked, and published. Validation permits curation only for `PASSED` and `PASSED_WITH_REJECTIONS`; deterministic `FAILED` results are immutable and reusable, while transient failures publish no terminal marker. A replay is a no-op only when its marker identity and every declared output agree; mismatches fail clearly.

Curated dimensions are snapshots and retain their business identifiers. Facts retain business identifiers at `(OrderID, OrderItemID)` and `(OrderID, ReviewID)` grains; no warehouse surrogate keys are assigned. Each dimension and fact has a lowercase SHA-256 `RecordHash` over canonical JSON in contract field order, preserving nulls and normalizing decimals and timestamps as specified by the curated contract. This is a full-record fingerprint, not an SCD Type 2 tracked-attribute hash.

The `payment_methods` aggregate groups item `Price` by the representative payment type, selected as the first payment ordered by `(payment_sequential, payment_type)`. It is descriptive item-price attribution, not tendered-payment totals; `payment_value` is neither allocated nor aggregated into `fact_sales` or this aggregate.

## Curated catalog metadata

The authoritative Glue table templates are under [`catalog/tables`](catalog/tables), with database metadata in [`catalog/database.json`](catalog/database.json). Each template uses the Glue `CreateTable` request envelope but contains the required deployment-time token `${AWS_ETL_BUCKET}`. The committed cloud JSON is therefore a template, not a directly invokable request; Phase 8 CDK must resolve the token before deployment.

Structurally matching local manifests are under [`runtime/local/catalog-manifests`](runtime/local/catalog-manifests). They point to `s3://ecommerce-sales-local/curated/<layer>/<dataset>/` and describe the same 16 unpartitioned Parquet datasets. These versioned JSON files validate metadata equivalence only: they are not a running Glue Data Catalog and do not establish that Glue Data Catalog was deployed or invoked. Crawlers are not authoritative for curated schemas.

Regenerate and validate the deterministic metadata from the committed curated contract with:

```bash
make catalog-generate
make catalog-validate
```

Raw objects are content-addressed as `raw/<dataset>/content_sha256=<sha256>/<canonical-filename>`. Each manifest records the batch ID and timestamp, provider-neutral dataset, canonical source-file identity, size, source modification timestamp, content SHA-256, raw object path, and pipeline version. Manifests are stored at `manifests/dataset=<dataset>/batch_id=<batch-id>/manifest.json`. Submission evidence is append-only under `audit/dataset=<dataset>/batch_id=<batch-id>/attempt=<number>/`.

## Replay behavior

- An identical successful manifest is a no-op.
- Identical successful content under a new batch ID is a no-op with new manifest and audit evidence, without republishing raw data.
- A latest failed attempt is retried only when `Retryable=true`.
- A deterministic failure, or failure without explicit retryability, produces reused-failure audit evidence without reprocessing.
- Changed immutable content under an existing batch ID fails the batch.
- The latest attempt is the maximum `AttemptNumber` for each `(Dataset, BatchID)`.
- Every submission creates immutable audit evidence; no-op and reused-failure submissions do not republish raw data.

## Inspect LocalStack

With the AWS CLI installed, use the local endpoint and dummy credentials:

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://localhost:4566 s3 ls s3://ecommerce-sales-local/ --recursive

AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url http://localhost:4566 s3 cp \
  s3://ecommerce-sales-local/manifests/ - --recursive --exclude '*' --include '*.json'
```

The repository status command reports foundation completeness and object counts without requiring the AWS CLI:

```bash
make aws-local-status
```
