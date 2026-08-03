# AWS Implementation and Validation Evidence

## Project summary

This AWS implementation is a batch analytics project that transforms nine Olist CSV sources into governed Parquet data, a dimensional warehouse, and eight analytical datasets. It combines an Amazon S3 data-lake design, four AWS Glue 5 PySpark jobs, 16 explicit Glue Data Catalog tables, a 32-state Step Functions Standard Workflow, Redshift Serverless with SCD Type 2 dimensions, six AWS CDK stacks, least-privilege IAM, private networking, CloudWatch observability, and a PostgreSQL local warehouse.

The pipeline and PostgreSQL publication path were executed locally. Contracts, catalog metadata, orchestration, Redshift SQL, SCD2, and replay behavior are deterministically validated. CDK synthesized all six AWS stacks, but no managed AWS resources were deployed or executed.

## Architecture at a glance

The [AWS architecture](README.md#data-architecture) shows the managed design, and the [data flow](README.md#data-flow) describes the credential-free substitute path. Both consume the same `aws_etl` transformations; only runtime integrations differ.

```text
Olist CSV
  -> raw S3 object + immutable manifest + submission audit
  -> Glue ProcessRaw: schema, cleanse, normalize, deduplicate, reject, Parquet
  -> Glue ValidateProcessed: quality + cross-dataset integrity decision
  -> Glue BuildCurated: 6 dimensions + 2 facts + 8 aggregates
  -> Glue Data Catalog: 16 explicit, unpartitioned Parquet tables
  -> Glue LoadWarehouse: attempt-isolated Redshift staging + publication call
  -> Redshift: SCD2 dimensions + event-time facts + analytics + audit registry
```

## AWS services and repository evidence

| AWS capability | Implemented design | Primary repository evidence | Status |
|---|---|---|---|
| Amazon S3 | Raw, processed, curated, rejected, quality, manifest, audit, and staging prefixes; encryption, versioning, access blocking, and lifecycle rules | [`storage-stack.ts`](cdk-infrastructure/lib/storage-stack.ts), [`storage.py`](src/aws_etl/storage.py) | Local substitute executed; CDK synthesized |
| AWS Glue 5 | Four jobs: `ProcessRaw`, `ValidateProcessed`, `BuildCurated`, `LoadWarehouse`; explicit schemas and Glue 5 runtime settings | [`processing-stack.ts`](cdk-infrastructure/lib/processing-stack.ts), [`glue-jobs`](glue-jobs/) | Transformations executed locally; definitions synthesized |
| Glue Data Catalog | Database plus 16 explicit Parquet table definitions generated from contracts; crawlers are non-authoritative | [`catalog`](catalog/), [`catalog-stack.ts`](cdk-infrastructure/lib/catalog-stack.ts) | Metadata validated; resources synthesized |
| Step Functions | 32 states covering claims, replay/resume, four Glue tasks, quality routing, immutable terminal evidence, and failure classification | [`pipeline.asl.json`](orchestration/pipeline.asl.json), [`orchestration-stack.ts`](cdk-infrastructure/lib/orchestration-stack.ts) | Graph and behavior validated; resource synthesized |
| Redshift Serverless | Private namespace/workgroup; 16 staging tables; 6 dimensions; 2 facts; 8 aggregates; audit and atomic publication procedure | [`warehouse-stack.ts`](cdk-infrastructure/lib/warehouse-stack.ts), [`sql/redshift`](sql/redshift/), [`redshift_warehouse.py`](src/aws_etl/redshift_warehouse.py) | SQL and behavior simulated; resources synthesized |
| AWS IAM | Per-job roles, prefix-scoped S3 access, four-job Step Functions invocation boundary, optional permissions boundary | [`processing-stack.ts`](cdk-infrastructure/lib/processing-stack.ts), [`orchestration-stack.ts`](cdk-infrastructure/lib/orchestration-stack.ts) | Policies synthesized and reviewable |
| Amazon VPC | Dedicated `/24`, three isolated `/27` subnets across AZs, no public subnets/NAT/IGW, S3 gateway endpoint, enhanced VPC routing | [`warehouse-stack.ts`](cdk-infrastructure/lib/warehouse-stack.ts) | Synthesized; not connectivity-tested in AWS |
| CloudWatch | Glue and workflow logs, 30-day retention, Glue/workflow/warehouse failure alarms | [`observability-stack.ts`](cdk-infrastructure/lib/observability-stack.ts), [`warehouse-stack.ts`](cdk-infrastructure/lib/warehouse-stack.ts) | Synthesized; no production telemetry |
| AWS CDK | Storage, Catalog, Warehouse, Processing, Orchestration, and Observability stacks | [`cdk-infrastructure`](cdk-infrastructure/) | TypeScript compiled; six stacks synthesized |
| Secrets Manager | Redshift-managed administrator secret; runtime jobs use IAM/Data API rather than embedded passwords | [`warehouse-stack.ts`](cdk-infrastructure/lib/warehouse-stack.ts) | Synthesized |

## Datasets and warehouse flow

The 16 curated datasets are:

- Dimensions: `dim_customer`, `dim_product`, `dim_seller`, `dim_geography`, `dim_date`, `dim_order_status`.
- Facts: `fact_sales` at `(OrderID, OrderItemID)` and `fact_reviews` at `(OrderID, ReviewID)`.
- Aggregates: `sales_by_state`, `sales_by_category`, `monthly_sales`, `order_status`, `cross_state_analysis`, `seller_performance`, `size_analysis`, `payment_methods`.

Curated Parquet retains business identifiers. Warehouse loaders first write attempt-isolated staging data, validate schema, nullability, row counts, grain, relationships, record hashes, payment semantics, and aggregates, and only then publish. Facts resolve surrogate keys after dimension publication. Reviews intentionally have no product attribution. `payment_methods` attributes item price to one deterministically selected representative payment type; it does not allocate `payment_value`.

## SCD2 and replay behavior

The Redshift design applies SCD Type 2 to customer, product, seller, and geography. Each tracked dimension uses a canonical tracked-attribute hash, half-open `[effective, expiration)` intervals, one current row, and strict event-time fact resolution. Date and order-status dimensions are static. Same-time conflicts, duplicate incoming keys, overlapping intervals, and zero/multiple fact-time matches fail deterministically.

Publication equality hashes the batch ID, contract and pipeline versions, curation-marker identity, ordered dataset names, object hashes and row counts, and the Redshift policy version. A matching completed replay is an immutable no-op. Different evidence for a completed batch is a conflict. Failed attempts cannot enter the completed registry. A committed Redshift registry can repair a missing S3 completion marker; disagreement between the two is rejected.

The local PostgreSQL warehouse intentionally implements a current-snapshot publication algorithm rather than SCD2. It validates staging, stable surrogate keys, atomic publication, registry equality, no-op replay, conflict handling, and rollback. Redshift-specific history behavior is validated by deterministic simulation, not by claiming PostgreSQL is Redshift.

## IAM, networking, observability, and cost controls

- The Redshift COPY role reads only `staging/warehouse/redshift/`; it cannot read raw or processed data.
- The warehouse Glue role reads curated data and curation evidence, writes Redshift staging/audit evidence, calls the Data API for one workgroup, reads deployment assets, and writes its own logs.
- Step Functions may run and inspect only the four declared Glue jobs and has no Redshift administration access.
- The Redshift workgroup is private and uses enhanced VPC routing. The VPC has isolated subnets and an S3 gateway endpoint instead of NAT gateways.
- The bucket blocks public access, uses SSE-S3 and versioning, expires staging and incomplete multipart uploads after seven days, and expires noncurrent versions after 90 days.
- Glue defaults to two `G.1X` workers, a 60-minute timeout, and concurrency one. Redshift is fixed at 8 base RPU, 16 maximum RPU, and a 40 RPU-hour monthly usage-limit deactivation action.
- Logs retain 30 days. Alarms have no notification actions, avoiding an unconfigured notification dependency.

These controls are configuration evidence, not a measured AWS bill. The design intentionally avoids NAT gateway cost and uncontrolled warehouse capacity, but no actual AWS cost is claimed.

## Deterministic sample evidence

| Metric | Expected result |
|---|---:|
| Input datasets | 9 |
| Processed rows by dataset | customers 3; orders 3; order items 4; payments 4; reviews 3; products 3; sellers 2; geolocation 4; category translation 2 |
| Duplicate rows removed | 2 |
| Rejected rows | 2 |
| Curated dimensions / facts / aggregates | 6 / 2 / 8 |
| Dimension rows | customer 3; product 3; seller 2; geography 3; date 60; order status 6 |
| Accepted sales rows / orders | 3 / 2 |
| Accepted review rows | 2 |
| Same-state / cross-state / delayed sales items | 1 / 2 / 1 |
| Gross input item value | $355.00 |
| Accepted curated item-price total | $320.00 |
| Accepted freight total | $35.00 |
| Accepted total item value including freight | $355.00 |

The identical `$355.00` gross-input and accepted-price-plus-freight figures have different scopes. The authoritative machine-readable evidence is [`baseline_snapshot.json`](../../contracts/expected/baseline_snapshot.json).

## Architecture decisions

1. **Contracts before provider code.** Versioned schemas, grains, quality rules, replay policy, and expected evidence prevent service definitions from becoming the business specification.
2. **One transformation package, separate integrations.** Local and cloud jobs share `aws_etl` modules while LocalStack/Python/PostgreSQL and S3/Step Functions/Redshift integrations remain isolated.
3. **Explicit catalog metadata.** Curated schemas are contract-derived and committed; crawlers cannot silently redefine analytical tables.
4. **Immutable completion evidence.** Data files publish before conditional completion markers, allowing deterministic resume and preventing partial output from looking complete.
5. **Attempt-isolated warehouse staging.** COPY/JDBC operations can finish independently without exposing partial warehouse data; publication occurs only after complete validation.
6. **Separate PostgreSQL and Redshift SQL.** PostgreSQL proves local publication behavior, while Redshift uses native DDL, Data API semantics, and a Redshift-specific SCD2 procedure.
7. **Private, cost-bounded defaults.** Isolated networking, no NAT, fixed Glue concurrency, fixed RPU bounds, and a usage-limit action reduce accidental exposure and spend.

## Validation evidence

The single credential-free gate is:

```bash
make project-validate
```

It runs Python compilation; baseline and contract validation; catalog validation; Step Functions and replay checks; Redshift SQL and warehouse simulations; the TypeScript build; and CDK synthesis. The CI workflow runs the same gate without AWS credentials.

The CDK dependency tree currently has one disclosed high-severity `npm audit` advisory in the transitive `brace-expansion` package (`GHSA-mh99-v99m-4gvg`). The advisory remains present in the current dependency lockfile and is disclosed as a known limitation.

## Limitations and optional future work

- No managed AWS service was deployed or executed; IAM, networking, CloudWatch, and service integration remain synthesis evidence.
- No measured AWS cost, service duration, production SLA, scale test, disaster-recovery exercise, or operational runbook is claimed.
- Redshift distribution and sort choices are provisional; no empirical query-plan, skew, distribution, or scan optimization is claimed.
- The deterministic fixture is deliberately small, and the complete Olist dataset remains outside Git.
- PostgreSQL is a logical local warehouse substitute, not an engine-parity test for Redshift.
- Optional future work: deploy a budget-capped sandbox, run fixture then full data, capture managed logs/counts/replay/failure evidence, measure cost and duration, validate Redshift physical design, and tear down chargeable resources.
