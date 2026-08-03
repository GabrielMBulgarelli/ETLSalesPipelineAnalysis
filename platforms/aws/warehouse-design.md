# Redshift Serverless warehouse design

The Redshift Serverless layer implements staging, four historical dimensions, two static dimensions, two facts, eight analytical tables, publication audit, replay recovery, private networking, and cost-bounded capacity as AWS CDK and Redshift-native SQL.

> **Evidence boundary:** Redshift SQL and SCD2/replay behavior were validated deterministically and the infrastructure was synthesized, but Redshift Serverless was not deployed or executed. Distribution and sort choices were not empirically optimized.

## Architecture and SQL

Redshift SQL is isolated in `platforms/aws/sql/redshift/`; PostgreSQL remains the local substitute and never executes this SQL. The six ordered files are: `001_schema.sql` (schemas), `002_staging.sql` (attempt-isolated tables for all 16 curated datasets), `003_dimensions.sql` (six dimensions), `004_facts.sql` (two facts), `005_aggregates.sql` (eight physical analytics tables), and `006_audit.sql` (append-only evidence, completed registry, publication lock, validation, and the default-mode atomic `audit.publish_warehouse` procedure). Bootstrap execution of these files is optional future managed-deployment work.

The loader validates the immutable passing curation marker and every curated dataset, emits attempt-isolated Parquet and COPY manifests, runs 16 COPY operations into unpublished staging, binds staged rows to the expected fingerprint, rechecks the registry, then makes one parameterized Data API `CALL`. COPY commits do not share a transaction with publication. The procedure locks `audit.warehouse_publication_lock`, validates staging, performs history/static/fact/aggregate changes, and commits completed registry and audit evidence atomically. It contains no `COMMIT`, `ROLLBACK`, or `TRUNCATE`; an error rolls back publication. Data API requests use deterministic ClientTokens derived from fingerprint, LoadAttemptID, operation, and dataset, remain below the 40-statement batch limit, and inspect known statement IDs before replacement. Loader attempts are serialized at Glue concurrency one in addition to the Redshift publication lock.

## History and fact policy

`WarehouseEffectiveAt` is the immutable manifest `batch_timestamp`: warehouse-observed effective time, not a source business-effective timestamp. Initial versions start at `1900-01-01T00:00:00Z`; later loads can split intervals. Validity is `[WarehouseEffectiveAt, WarehouseExpirationAt)`, with `NULL` expiration as infinity. Same-time identical changes are no-ops; same-time conflicts and duplicate incoming keys fail. Absent keys remain unchanged.

Historical dimensions and tracked attributes are:

- customer / `CustomerID`: `CustomerUniqueID`, zip, city, state;
- product / `ProductID`: category names, physical measures, volume, size category;
- seller / `SellerID`: zip, city, state;
- geography / `ZipCodePrefix`: city, state, latitude, longitude, region.

Each has a dedicated canonical `SCD2TrackedHash`; curated `RecordHash` remains traceability and is not a history change detector. Operational, attempt, publication, surrogate, and interval fields are excluded. Date and order status are static dimensions.

Important customer limitation: Olist `CustomerID` is an order-scoped customer record. This warehouse tracks corrections to that committed record between pipeline batches; it does not claim person-level history across orders sharing `CustomerUniqueID`. That would require a separate grain, ordering, and duplicate-resolution design.

Sales use purchase timestamp to resolve customer, product, seller, `CustomerGeographyKey`, and `SellerGeographyKey` under strict half-open matching, plus exact date/status lookups. Reviews use review creation timestamp for customer and exact date; they have no product, seller, geography, payment, or invented status relationship. Zero or multiple matches fail deterministically. Business identifiers and contracted fact grains remain present.

## Identity, recovery, and audit

Publication equality contains BatchID, contract and pipeline versions, immutable curation-marker hash, the ordered identities/hashes/counts of all 16 datasets, and `redshift-scd2-v1`. LoadAttemptID, Glue job-run ID, Data API statement IDs, timestamp, and retry count are separate attempt evidence and never affect equality. Failed attempts cannot create completed registry rows.

The committed Redshift registry is authoritative. A matching registry with a missing S3 completion marker reconstructs the marker conditionally. An S3 marker without a registry, or disagreement between them, is a deterministic conflict. Audit tables are append-only through loader-role grants and procedure ownership; administrators can still alter rows, so physical immutability against administrators is not claimed.

## Infrastructure, IAM, and cost controls

`WarehouseStack` defines a dedicated `/24` VPC with three `/27` isolated subnets in distinct AZs, DNS enabled, no public subnets, internet gateway, or NAT gateway, and an S3 gateway endpoint scoped to curated, quality/manifest evidence, Redshift staging, and Redshift audit objects. The workgroup is private with enhanced VPC routing. Glue remains outside the VPC and calls regional S3/Data API endpoints.

The namespace uses database `ecommerce_sales`, nondefault administrator `warehouse_admin`, Redshift-managed password/Secrets Manager secret, AWS-managed encryption, and user/connection/user-activity logs. Capacity is 8 base RPU and 16 maximum RPU. The monthly 40 RPU-hour deactivation limit is a hard operational guard, not a cost estimate; alarms are actionless. Production retains the namespace; nonproduction uses snapshot removal behavior.

The COPY role reads only `staging/warehouse/redshift/`. The Glue loader role reads curated and curation evidence, writes only Redshift staging/audit evidence, operates Data API/GetCredentials for its workgroup, reads deployment assets, and writes its logs. Step Functions can run/inspect only four declared jobs and has no Redshift administration access. Unavoidable wildcards are the existing Step Functions CloudWatch Logs delivery APIs (AWS does not support resource scoping) and the S3 endpoint `Principal` (constrained by endpoint resources/actions/prefixes); no raw or processed access is granted to Redshift roles.

Physical distribution/sort keys are provisional deployment-ready defaults. Informational constraints are never used for correctness. Optional future managed validation must examine query plans, skew, distribution, and scan behavior before retaining them.

## Validation boundary and commands

Run `make project-validate` for Python compilation; baseline and contract validation; catalog validation; Step Functions and replay checks; Redshift SQL and warehouse simulations; the TypeScript build; and CDK synthesis. The simulation covers initial/unchanged/tracked/ignored/late/same-time/absent history, duplicate and temporal failures, separate geography roles, review limitations, replay/conflict/nonpublication, and cross-service marker recovery. PostgreSQL execution validation additionally requires a locally curated batch and `make aws-postgres-validate BATCH_ID=<batch-id>`. These checks are not evidence of managed-service execution, real runtime, RPU consumption, or empirical physical-design performance.
