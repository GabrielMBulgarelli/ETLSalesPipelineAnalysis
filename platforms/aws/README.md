# <span style="font-size: 28px;">E-Commerce Sales Analysis and Optimization using AWS Analytics Services</span>

[Project overview](../../README.md) · [Azure implementation](../azure/README.md)

## Overview

This implementation provides a contract-driven batch pipeline for the Olist e-commerce dataset using Amazon S3, AWS Glue 5, Step Functions, Glue Data Catalog, and a Redshift Serverless warehouse design. The same transformation package runs in the official Glue 5 container for local validation, while LocalStack and PostgreSQL provide credential-free substitutes for storage and warehouse publication.

Local execution used LocalStack, Glue Docker, and PostgreSQL; replay, SCD2, publication, and recovery were deterministically validated; six AWS CDK stacks synthesized; no AWS resources were deployed.

## Dataset

The [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) contains marketplace orders, products, customers, sellers, locations, payments, and reviews from approximately 100,000 Brazilian orders placed between 2016 and 2018. The pipeline accepts exactly nine canonical CSV files and rejects missing, duplicate, case-ambiguous, or unexpected inputs before publishing raw objects, manifests, or audit evidence.

```text
customers ─┐
orders ────┼── order items ── products ── category translation
payments ──┤         │
reviews ───┤         └──────── sellers
geolocation┘
```

The complete dataset stays outside Git. Versioned [schemas](../../contracts/schemas/README.md), [quality and replay rules](../../contracts/rules/README.md), a deterministic fixture, and expected evidence define repository behavior without bundling source data.

## Prerequisites

The supported local environment is Linux x86_64 with Python 3.11 or newer, Docker Engine with Docker Compose v2, Node.js 22, npm, `curl`, and a complete Olist dataset directory outside the repository.

Install the Python package and pinned CDK dependencies from the repository root:

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
python3 -m pip install -e platforms/aws
make aws-cdk-install
```

Copy [`.env.example`](../../.env.example) to an ignored `.env` only when local defaults need overriding. LocalStack test credentials are rejected outside the local profile, PostgreSQL passwords come from the environment, and cloud designs use IAM and managed secrets rather than embedded credentials.

For a complete local run:

```bash
make aws-local-up
make aws-postgres-up
make aws-local-run DATASET_DIR=/absolute/path/to/olist BATCH_ID=project-run WAREHOUSE=1
make aws-postgres-validate BATCH_ID=project-run
make aws-local-status
make aws-postgres-status
```

Stage-level seed, process, validate, curate, and warehouse commands are available in the [Makefile](../../Makefile). `make aws-postgres-clean` is the explicit destructive cleanup; ordinary down commands retain evidence volumes.

## Data Architecture

The implementation uses a governed lake and warehouse publication boundary:

- Amazon S3 and LocalStack organize immutable raw objects, processed and rejected batches, curated Parquet datasets, quality results, manifests, audit records, and warehouse staging.
- Four Glue 5 PySpark jobs share the `aws_etl` modules between local and cloud entrypoints.
- Sixteen explicit Glue Data Catalog definitions describe the curated outputs.
- Step Functions defines a 32-state Standard Workflow for claims, replay, processing, quality routing, warehouse loading, and terminal evidence.
- PostgreSQL validates current-snapshot publication locally; Redshift Serverless has separate native SQL and deterministic SCD2 validation.
- Six CDK stacks define private storage, catalog, processing, orchestration, warehouse, IAM, managed secrets, CloudWatch observability, and bounded-capacity controls.

```text
Olist CSV files
  -> S3 raw objects + immutable manifests
  -> Glue processing + rejected rows
  -> quality and referential-integrity decision
  -> curated Parquet + Catalog metadata
  -> attempt-isolated warehouse staging
  -> atomic warehouse publication + audit evidence
```

## Data Flow

The pipeline discovers the nine source files, hashes their content, writes versioned raw objects, and records an immutable manifest. `ProcessRaw` applies explicit schemas, UTC timestamp normalization, deterministic cleansing and deduplication, and stable rejected-row reason codes. `ValidateProcessed` checks quality thresholds and cross-dataset relationships before `BuildCurated` produces dimensions, facts, and aggregates. `LoadWarehouse` verifies curation evidence before staging and publication.

```text
ProcessRaw
    │
    v
ValidateProcessed ── quality failure ──> immutable failure evidence
    │ accepted
    v
BuildCurated
    │
    v
LoadWarehouse ──> completed registry + immutable completion evidence
```

A matching replay is an immutable no-op. A partial run resumes only when earlier markers and Parquet outputs match recorded evidence; changed content under an existing batch ID is a conflict. Transient Glue failures use bounded retry, while validation, access, input, and quality failures do not. Terminal objects are create-only, and recovery rejects disagreement between warehouse and object-store evidence.

The authoritative workflow is [`orchestration/pipeline.asl.json`](orchestration/pipeline.asl.json). Validate its graph, job tokens, retry boundary, conditional writes, and replay classifier with `make aws-state-machine-validate`.

## Dimensional Model

The curated layer contains six dimensions and two facts. Customer, product, seller, and geography use SCD2 tracked-attribute hashes and half-open effective intervals in the Redshift design; date and order status are static. PostgreSQL intentionally publishes a current snapshot and validates atomicity, stable surrogate keys, replay equality, conflict handling, and rollback rather than claiming Redshift engine parity.

```text
dim_customer ─┐
dim_product ──┤
dim_seller ───┼── fact_sales
dim_geography ┤
dim_date ─────┤
dim_status ───┘

dim_customer ─┬── fact_reviews
dim_date ─────┘
```

The sales fact has one row per order item, identified by `(OrderID, OrderItemID)`. The review fact has one row per order and review pair, identified by `(OrderID, ReviewID)`; reviews do not invent product, seller, or geography attribution. Redshift resolves dimension versions using purchase time for sales and review-creation time for reviews.

Attempt-isolated PostgreSQL and Redshift staging validates schema, nullability, counts, grains, relationships, hashes, payment semantics, and aggregates before one publication transaction. Redshift rejects duplicate keys, conflicting intervals, and zero or multiple event-time dimension matches.

## Aggregation Tables

Eight curated aggregates support common analytical questions while retaining deterministic definitions from the shared contracts:

```text
sales_by_state        sales_by_category
monthly_sales         order_status
cross_state_analysis  seller_performance
size_analysis         payment_methods
```

The aggregates cover geography, product-category performance, monthly trends, current order status, interstate commerce, seller operations, product size, and representative payment method. Payment-method attribution selects one payment type deterministically at item grain and does not treat payment value as allocated item revenue.

## Glue Job Pipeline

The four Glue 5 jobs are deployed as separate entrypoints while reusing the same tested transformation modules:

```text
ProcessRaw
  -> ValidateProcessed
  -> BuildCurated
  -> LoadWarehouse
```

`ProcessRaw` publishes processed and rejected Parquet only after staging verification. `ValidateProcessed` gates downstream work on quality and referential integrity. `BuildCurated` publishes all 16 analytical datasets and its terminal marker last. `LoadWarehouse` creates 16 COPY manifests, loads attempt-isolated staging, binds the input to a publication fingerprint, and calls the warehouse publication procedure.

The local Python runner mirrors the workflow decisions. Build an execution-safe name with `make aws-execution-name ENVIRONMENT=dev BATCH_ID=project-run ATTEMPT=1`, and inspect individual stages with `make aws-local-seed`, `make aws-local-process`, `make aws-local-validate`, `make aws-local-curate`, and `make aws-postgres-load`.

## <span style="font-size: 24px;">Analysis</span>

The eight curated aggregates expose the following analytical views. Detailed sample results are recorded in [AWS implementation and validation evidence](validation-evidence.md).

### <span style="font-size: 20px;">Analysis #1: Geographic Sales Distribution</span>

`sales_by_state` provides order count, unique-customer count, item revenue, average item price, and average delivery time by customer state.

### <span style="font-size: 20px;">Analysis #2: Product Category Performance</span>

`sales_by_category` provides order count, unique-customer count, item revenue, average item price, average delivery time, and delayed-order count by product category.

### <span style="font-size: 20px;">Analysis #3: Monthly Sales Trends</span>

`monthly_sales` summarizes monthly revenue, order volume, and average ticket value to support seasonality and growth analysis.

### <span style="font-size: 20px;">Analysis #4: Order Status Analysis</span>

`order_status` provides order count, unique-customer count, and item revenue by category and current order status; it does not model status-transition history.

### <span style="font-size: 20px;">Analysis #5: Cross-State Commerce Analysis</span>

`cross_state_analysis` measures interstate order volume, delivery time, and regional relationships for logistics analysis.

### <span style="font-size: 20px;">Analysis #6: Seller Performance Metrics</span>

`seller_performance` reports order count, item revenue, average freight cost, delivery time, delayed-order count, and delay rate without attributing review scores to sellers.

### <span style="font-size: 20px;">Analysis #7: Product Size Impact Analysis</span>

`size_analysis` relates product dimensions to order volume and revenue by category. The additional `payment_methods` aggregate reports item-grain value against a deterministic representative payment type.

## <span style="font-size: 24px;">Dimensional Tables</span>

The warehouse contains six dimensions:

<table>
  <tr>
    <th align="left">Dimension</th>
    <th align="left">Description</th>
  </tr>
  <tr>
    <td>Customer</td>
    <td>Customer identifiers, location, and tracked attributes</td>
  </tr>
  <tr>
    <td>Product</td>
    <td>Product category, measurements, and tracked attributes</td>
  </tr>
  <tr>
    <td>Seller</td>
    <td>Seller identifiers, location, and tracked attributes</td>
  </tr>
  <tr>
    <td>Geography</td>
    <td>Postal-code, city, and state relationships</td>
  </tr>
  <tr>
    <td>Date</td>
    <td>Static calendar attributes for time-based analysis</td>
  </tr>
  <tr>
    <td>Order Status</td>
    <td>Static order-status descriptions</td>
  </tr>
</table>

Four business dimensions use Redshift SCD2 history. The SQL, publication procedure, IAM boundary, recovery rules, and deterministic evidence are documented in the [Redshift Serverless warehouse design](warehouse-design.md).

## <span style="font-size: 24px;">Fact Tables</span>

The warehouse publishes two facts after dimension keys are resolved:

<table>
  <tr>
    <th align="left">Fact Table</th>
    <th align="left">Description</th>
  </tr>
  <tr>
    <td>Sales Fact Table</td>
    <td>One row per order item with item price, freight, delivery, status, and dimensional keys</td>
  </tr>
  <tr>
    <td>Reviews Fact Table</td>
    <td>One row per order and review pair linked to order, customer, and review-creation date</td>
  </tr>
</table>

Run the credential-free final gate with:

```bash
make project-validate
```

It runs Python compilation; baseline and contract validation; catalog validation; Step Functions and replay checks; Redshift SQL and warehouse simulations; the TypeScript build; and CDK synthesis. The evidence boundary distinguishes locally executed behavior, deterministic checks, and synthesized infrastructure; it does not claim managed execution, measured cost, or empirical Redshift performance.

## Future Enhancements

- Deploy a budget-capped AWS sandbox and compare managed outputs with the Azure implementation against identical inputs.
- Capture managed Glue, Step Functions, Redshift, IAM, networking, Secrets Manager, and CloudWatch execution evidence.
- Measure duration and cost at fixture and full-data scale before adjusting Glue workers or Redshift capacity.
- Validate Redshift query plans, skew, distribution, sort behavior, backup, restore, and disaster recovery.
- Expand operational alert routing and production runbooks after a managed exercise.
