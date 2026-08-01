# AWS-local raw-to-curated pipeline and PostgreSQL warehouse

Phases 3 through 7 provide a local S3-compatible ingestion and AWS Glue 5 path from the nine Olist source files through validated processed data, curated dimensions and facts, eight aggregates, explicit curated catalog metadata, and a Step Functions orchestration definition. LocalStack and the Glue container do not validate managed AWS IAM, networking, scaling, durability, or managed Step Functions execution behavior.

## Prerequisites

- Linux x86_64 (the supported local host)
- Docker Engine with Docker Compose v2 (Podman compatibility is best-effort)
- Python 3.11 or newer
- `curl`
- Node.js 22 and npm (for Phase 8 CDK synthesis)

Install the package once in a virtual environment:

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -e platforms/aws
```

The complete Olist dataset stays outside Git. Do not copy it into this repository.

Copy the explicitly local dummy credentials from the root `.env.example` into an ignored `.env`, or export equivalent values. PostgreSQL passwords are never embedded in SQL or committed runtime configuration.

## Synthesize the managed AWS infrastructure

Phase 8 defines five CDK stacks under [`infrastructure/cdk`](infrastructure/cdk): Storage, Catalog, Processing, Orchestration, and Observability. Synthesis creates CloudFormation and native CDK file assets locally; it does not deploy resources. The lockfile pins CDK CLI 2.1133.0, CDK libraries 2.262.1, and the TypeScript toolchain, so installation uses `npm ci`:

```bash
make aws-cdk-install
make aws-cdk-build
make aws-cdk-synth
```

Defaults are `environment=dev`, `awsRegion=us-east-1`, Glue 5.0 `G.1X` with two workers, a 60-minute timeout, and one concurrent run. Only CDK context changes cloud settings; for example:

```bash
cd platforms/aws/infrastructure/cdk
npm exec cdk synth --quiet \
  -c environment=staging \
  -c awsRegion=us-west-2 \
  -c glueWorkerType=G.2X \
  -c glueWorkerCount=4 \
  -c glueTimeoutMinutes=90 \
  -c glueMaxConcurrency=1
```

Allowed environments are `dev`, `staging`, and `prod`. An organization-managed boundary can be applied to every Glue and Step Functions execution role with `-c permissionsBoundaryArn=arn:aws:iam::<account>:policy/<name>`. The S3 bucket is retained, encrypted with SSE-S3, versioned, and blocks all public access. Staging objects and incomplete multipart uploads expire after seven days; noncurrent versions expire after 90 days; authoritative current data is retained indefinitely. Logs are retained for 30 days. Failure alarms intentionally have no actions or notification service.

The Processing stack packages the committed Python package, each existing Glue entrypoint, a generated credential-free cloud config, and the committed contracts as CDK S3 assets. The Catalog stack renders the committed 16 unpartitioned table templates against the deployed bucket. The Orchestration stack resolves the three Glue job-name tokens in the committed 29-state Standard Workflow without changing its replay or state envelope semantics.

Generate the deterministic Step Functions execution name before calling `StartExecution`; orchestration attempt defaults to 1:

```bash
make aws-execution-name ENVIRONMENT=dev BATCH_ID=my-batch ATTEMPT=1
```

The name hashes the canonical text `<batch-id>\n<attempt>`, adds a sanitized batch prefix, and stays within the 80-character Step Functions limit. Phase 8 provides the helper and infrastructure only: it adds no scheduler, event trigger, invocation service, or deployment action.

## Run locally

```bash
make aws-local-up
make aws-local-run DATASET_DIR=/absolute/path/to/olist BATCH_ID=my-batch
# The stage-specific commands below remain useful for diagnosis.
make aws-local-seed DATASET_DIR=/absolute/path/to/olist BATCH_ID=my-batch
make aws-local-process BATCH_ID=my-batch
make aws-local-validate BATCH_ID=my-batch
make aws-local-curate BATCH_ID=my-batch
make aws-local-status
make aws-local-down
```

`aws-local-run` is the authoritative local orchestration path. It initializes the nested batch/storage/submissions/orchestration envelope, classifies each of the nine dataset submissions, acquires an immutable execution claim, and runs processing, validation, and curation synchronously in that order. A matching completed replay records a no-op without starting Glue. A partial run resumes at the first incomplete stage only when every earlier immutable marker matches the batch, all manifested content hashes, contract and pipeline versions, and every expected Parquet output. A marker/output mismatch fails deterministically and is never overwritten.

## Load the local PostgreSQL warehouse

Phase 9 adds PostgreSQL 16.14 on Debian Bookworm, pinned by immutable image-list digest and constrained to `linux/amd64`. The local database is `ecommerce_sales`; the `ecommerce_admin` bootstrap owner applies migrations, while Spark connects only as the non-superuser `ecommerce_etl` role. Schemas are `staging`, `warehouse`, `analytics`, and `audit`. Host access is limited to `127.0.0.1:${POSTGRES_PORT:-54329}`. Containers use `postgres:5432` on the Compose network.

```bash
make aws-postgres-up
make aws-postgres-load BATCH_ID=my-batch
make aws-postgres-validate BATCH_ID=my-batch
make aws-postgres-status
make aws-postgres-down
```

`aws-local-warehouse` is a compatibility alias for `aws-postgres-load`. The existing raw-to-curated pipeline remains PostgreSQL-independent. To opt in after a successful local curation, run `make aws-local-run ... WAREHOUSE=1`; warehouse evidence remains separate from source replay, Glue validation, curation, and terminal pipeline evidence.

The pinned Glue container writes all 16 curated datasets through Spark JDBC into attempt-isolated, `UNLOGGED` staging tables. These separate JDBC writes are not represented as one transaction. After complete staged schema, nullability, count, grain, relationship, `RecordHash`, representative-payment, and aggregate validation, one PostgreSQL connection begins the publication transaction and takes `pg_advisory_xact_lock`. It synchronizes six dimensions by business identifier without reallocating existing identity keys, replaces both fact snapshots and eight physical aggregate snapshots, removes dimensions absent from the incoming snapshot only after facts are rebuilt, and commits the completed registry and event with the data once. A publication error rolls the entire transaction back; its immutable failure event is committed separately before any attempt-scoped cleanup.

Publication equality is a canonical lowercase SHA-256 over BatchID, contract and pipeline versions, the exact curation-marker digest, and the ordered names, row counts, and curated object hashes of all 16 datasets. The load-attempt identity is deliberately excluded. A completed matching replay records an immutable no-op; a different fingerprint for an already completed BatchID records a conflict and fails. Failed attempts never create completed-publication entries, and rejected rows are not loaded.

The warehouse is a single current snapshot, not SCD Type 2. Dimension surrogate keys use PostgreSQL identity columns and remain stable for business identifiers already encountered; contracted business IDs and `RecordHash` remain present. Reviews retain their order/review grain without product attribution. `payment_methods` remains representative-payment item-price attribution and contains no `payment_value` allocation.

`make aws-postgres-down` retains PostgreSQL development evidence. `make aws-postgres-clean` is the explicit destructive cleanup command for the PostgreSQL container and named volume. Phase 10 will translate the logical model to Redshift-native DDL and load behavior; PostgreSQL identity columns, advisory locks, triggers, and transaction SQL are not intended to be copied verbatim.

The authoritative AWS state machine template is [`orchestration/pipeline.asl.json`](orchestration/pipeline.asl.json). It contains the deployment tokens `${ProcessRawGlueJobName}`, `${ValidateProcessedGlueJobName}`, and `${BuildCuratedGlueJobName}`; Phase 8 must resolve all three before deployment. The Glue tasks retry only `ConcurrentRunsExceededException`, `InternalServiceException`, and `OperationTimeoutException`, with a 5-second initial interval, 2.0 backoff, and two retry attempts. Deterministic validation, missing-job, invalid-input, access, `reject-dataset`, and `fail-batch` outcomes are not retried.

Execution claims live under `staging/orchestration/claims/` and are create-only, keyed by batch and orchestration attempt, with the execution owner recorded in both the payload and metadata. Completion and failure records also use conditional `PutObject` with `IfNoneMatch: "*"`; an existing object is accepted only when its submission identity and canonical evidence hash match. AWS details remain isolated under `ProviderEvidence` in provider-specific audit payloads. Validate the ASL graph, exact job tokens and retry boundary, conditional writes, and local retry classification with:

```bash
make aws-state-machine-validate
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

Curated dimensions are snapshots and retain their business identifiers. Facts retain business identifiers at `(OrderID, OrderItemID)` and `(OrderID, ReviewID)` grains. The curated datasets themselves contain no surrogate keys; the local PostgreSQL warehouse assigns stable identity keys to dimension business identifiers and resolves fact references during publication. Each dimension and fact has a lowercase SHA-256 `RecordHash` over canonical JSON in contract field order, preserving nulls and normalizing decimals and timestamps as specified by the curated contract. This is a full-record fingerprint, not an SCD Type 2 tracked-attribute hash.

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
