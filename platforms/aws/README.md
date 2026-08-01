# AWS implementation and local reproduction guide

This implementation provides the data and infrastructure layers for the **AWS-First E-commerce Data Engineering Platform**: an Amazon S3 lake, four AWS Glue 5 PySpark jobs, 16 Glue Data Catalog tables, a 32-state Step Functions workflow, Redshift Serverless with SCD Type 2, six AWS CDK stacks, least-privilege IAM, private networking, CloudWatch, and a PostgreSQL local warehouse.

The same `aws_etl` transformation modules run in the official Glue 5 Docker image locally and are packaged for managed Glue. LocalStack provides S3-compatible storage, the Python runner mirrors workflow decisions, and PostgreSQL validates warehouse publication. AWS infrastructure synthesis is credential-free and does not deploy resources.

## Supported environment

- Linux x86_64
- Python 3.11 or newer
- Docker Engine with Docker Compose v2
- Node.js 22 and npm
- `curl`
- A complete Olist dataset directory outside Git for end-to-end execution

Install the local package and pinned CDK dependencies:

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
python3 -m pip install -e platforms/aws
make aws-cdk-install
```

Copy [`.env.example`](../../.env.example) to an ignored `.env` if you need to override local defaults. The committed `test` credentials are LocalStack-only dummy values; configuration rejects them outside the `local` profile. PostgreSQL passwords are passed through the environment and are not embedded in SQL.

## Complete local reproduction

Start LocalStack and PostgreSQL, execute all pipeline stages, publish the warehouse, validate it, and inspect status:

```bash
make aws-local-up
make aws-postgres-up
make aws-local-run \
  DATASET_DIR=/absolute/path/to/olist \
  BATCH_ID=project-run \
  WAREHOUSE=1
make aws-postgres-validate BATCH_ID=project-run
make aws-local-status
make aws-postgres-status
```

`aws-local-run` is the authoritative local orchestration path. It discovers the nine canonical files, seeds immutable raw objects and manifests, classifies replay, acquires an execution claim, processes raw data, validates quality and relationships, builds curated datasets, and optionally publishes PostgreSQL. The warehouse step remains separate from the terminal source-pipeline evidence so warehouse failure cannot rewrite prior curation evidence.

Use stage-level commands for diagnosis or controlled reruns:

```bash
make aws-local-seed DATASET_DIR=/absolute/path/to/olist BATCH_ID=project-run
make aws-local-process BATCH_ID=project-run
make aws-local-validate BATCH_ID=project-run
make aws-local-curate BATCH_ID=project-run
make aws-postgres-load BATCH_ID=project-run
make aws-postgres-validate BATCH_ID=project-run
```

Stop services without deleting retained volumes:

```bash
make aws-postgres-down
make aws-local-down
```

`make aws-postgres-clean` is the explicit destructive command for the PostgreSQL container and named volume. No implicit cleanup command removes evidence.

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

Missing, duplicate, case-ambiguous, and unexpected CSV files fail before raw, manifest, or audit publication. The complete dataset remains outside Git; the repository contains only a deterministic contract fixture.

## Data-lake and curation behavior

```text
s3://ecommerce-sales-local/
├── raw/<dataset>/content_sha256=<sha256>/
├── processed/<dataset>/batch_id=<batch-id>/
├── curated/{dimensions,facts,aggregations}/
├── rejected/<dataset>/batch_id=<batch-id>/
├── quality/
├── manifests/
├── audit/
└── staging/
```

CSV inference is never used. Explicit contract schemas preserve monetary values as `decimal(10,2)` and normalize timestamps in UTC. Processing stages complete output under batch staging, verify it, publish stable Parquet prefixes, and write the terminal marker last. Rejected rows carry stable aligned reason codes and descriptions.

The 16 curated datasets are six dimensions (`customer`, `product`, `seller`, `geography`, `date`, `order_status`), two facts (`sales`, `reviews`), and eight aggregates (`sales_by_state`, `sales_by_category`, `monthly_sales`, `order_status`, `cross_state_analysis`, `seller_performance`, `size_analysis`, `payment_methods`). Sales grain is `(OrderID, OrderItemID)`; review grain is `(OrderID, ReviewID)` without invented product attribution.

## Replay and orchestration

The authoritative AWS definition is [`orchestration/pipeline.asl.json`](orchestration/pipeline.asl.json). Its 32 states carry batch, attempt, submission, storage prefix, pipeline-version, and provider-evidence context through replay classification, resume, four synchronous Glue tasks, quality routing, conditional terminal writes, and success/failure states.

- A matching completed replay records an immutable no-op.
- A partial run resumes only if every earlier marker and expected Parquet output matches immutable evidence.
- Changed content under an existing batch ID fails.
- Explicit transient Glue failures use bounded retry; deterministic validation, access, invalid-input, and quality failures do not retry.
- Completion/failure objects are create-only and accept an existing object only when submission identity and canonical evidence hash agree.
- A deterministic failure can be reused; every submission still records new audit evidence.

Validate the graph, four deployment tokens, retry boundary, conditional writes, and local classification logic with `make aws-state-machine-validate`. Generate a deterministic, Step Functions-safe execution name with:

```bash
make aws-execution-name ENVIRONMENT=dev BATCH_ID=project-run ATTEMPT=1
```

## Local PostgreSQL warehouse

PostgreSQL 16 runs on `127.0.0.1:${POSTGRES_PORT:-54329}` with separate bootstrap-owner and non-superuser ETL roles. Its `staging`, `warehouse`, `analytics`, and `audit` schemas validate:

- attempt-isolated Spark JDBC staging for all 16 curated datasets;
- schema, count, grain, relationship, hash, payment, and aggregate checks;
- stable surrogate keys for existing business identifiers;
- one advisory-lock-protected publication transaction;
- atomic facts, aggregate snapshots, completed registry, and audit evidence;
- rollback on publication errors, no-op replay, and completed-batch conflict handling.

PostgreSQL publishes a current snapshot and does not claim SCD2 engine parity. Redshift history behavior has separate SQL and deterministic simulation.

## Redshift Serverless design

[`sql/redshift`](sql/redshift/) contains six ordered, Redshift-native files for schemas, all 16 staging tables, six dimensions, two facts, eight analytics tables, audit structures, validation, and the `audit.publish_warehouse` procedure. The cloud loader verifies curation evidence, emits Parquet COPY manifests, performs 16 attempt-isolated COPY operations, binds staged rows to a publication fingerprint, and issues one parameterized Data API `CALL`.

Customer, product, seller, and geography are SCD2 dimensions with tracked-attribute hashes and half-open effective intervals. Date and order status are static. Sales use purchase time to resolve dimension versions; reviews use review-creation time and preserve the deliberate absence of product/seller/geography attribution. Duplicate keys, interval conflicts, and zero or multiple fact-time matches fail deterministically.

The private Redshift design uses a dedicated `/24` VPC, three isolated `/27` subnets, no public subnet, internet gateway, or NAT gateway, an S3 gateway endpoint, and enhanced VPC routing. Capacity is fixed at 8 base RPU and 16 maximum RPU, with a 40 RPU-hour monthly usage-limit deactivation action. Distribution and sort choices are provisional, not empirically optimized.

See [Redshift Serverless warehouse design](../../platforms/aws/warehouse-design.md) for publication, recovery, IAM, and validation details.

## CDK infrastructure

Six stacks synthesize in dependency order:

1. `StorageStack` — encrypted, versioned, private S3 bucket and lifecycle controls.
2. `CatalogStack` — curated database and 16 explicit table definitions.
3. `WarehouseStack` — private Redshift Serverless, networking, COPY role, logs, and capacity controls.
4. `ProcessingStack` — four Glue 5 jobs, assets, runtime configuration, and per-job roles.
5. `OrchestrationStack` — 32-state Standard Workflow and execution role.
6. `ObservabilityStack` — CloudWatch log retention and actionless failure alarms.

Build and synthesize without AWS credentials:

```bash
make aws-cdk-build
make aws-cdk-synth
```

Defaults are `environment=dev`, `awsRegion=us-east-1`, two `G.1X` Glue workers, a 60-minute timeout, and concurrency one. Allowed environments are `dev`, `staging`, and `prod`; an optional `permissionsBoundaryArn` applies an organization boundary to Glue and Step Functions roles. CDK context may change supported settings, but this repository intentionally provides no deploy target or credentialed CI job.

## Catalog metadata

Authoritative cloud templates are under [`catalog/tables`](catalog/tables) and use `${AWS_ETL_BUCKET}` for deployment-time rendering. Matching local manifests under [`runtime/local/catalog-manifests`](runtime/local/catalog-manifests) point to local S3-compatible locations. They prove schema equivalence only; they are not a running Glue Data Catalog.

```bash
make catalog-generate
make catalog-validate
```

## Credential-free validation

Run the complete final gate from the repository root:

```bash
make project-validate
```

The gate compiles Python, validates baseline/contracts and all 16 catalog tables, validates the 32-state workflow and replay classifier, inspects Redshift SQL, runs the SCD2/replay simulation, compiles TypeScript, synthesizes six CDK stacks, and checks repository hygiene. CI runs the same practical checks and never requests cloud credentials.

The pinned CDK dependency tree currently reports one high-severity transitive `brace-expansion` advisory (`GHSA-mh99-v99m-4gvg`). This disclosure is preserved; dependency remediation is intentionally outside documentation finalization.

## Evidence boundary

| Level | What it establishes |
|---|---|
| Executed locally | S3-compatible ingestion, Glue-container transformations, replay decisions, curated Parquet, and PostgreSQL publication behavior |
| Deterministically validated | Contracts, metadata, workflow graph, retry/replay rules, Redshift SQL, SCD2 interval behavior, publication equality, and recovery simulation |
| Synthesized, not deployed | S3, Glue, Catalog, Step Functions, Redshift Serverless, IAM, VPC, Secrets Manager, and CloudWatch resource definitions |

No managed AWS execution, measured AWS cost, production deployment, or empirical Redshift performance is claimed.
