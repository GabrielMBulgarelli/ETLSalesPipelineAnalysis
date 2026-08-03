# Azure Baseline

The Azure implementation uses the provider-neutral contracts in [`contracts/contracts.yaml`](../../contracts/contracts.yaml). The local baseline reproduces the contracted snapshot with a deterministic fixture and requires Python 3.10 or newer; it does not require Azure credentials.

## Reproduction commands

Run the complete local baseline gate from the repository root:

```bash
make baseline-test
```

The command runs the shared fixture and contract tests, the Azure baseline tests, and a comparison of a freshly built logical snapshot with [`contracts/expected/baseline_snapshot.json`](../../contracts/expected/baseline_snapshot.json). Inspect the generated result without changing files with:

```bash
python3 scripts/baseline_fixture.py --print
```

The deterministic fixture uses `2018-04-01T00:00:00Z` as its batch timestamp. A Synapse pipeline run must set Spark configuration `etl.batchTimestamp` to its chosen stable batch value before executing the curated notebook.

## Fixture expectations

The fixture contains all nine raw datasets, an exact duplicate customer, an exact duplicate order item, null optional review/product/order fields, orphan item and review records, an invalid review score, multiple items and payments, a delayed order, and both same-state and cross-state items.

Expected results are three sales fact rows across two orders, two accepted reviews, one same-state item, two cross-state items, one delayed item, gross item value `355.00`, and three rejected raw rows or rules. Per-dataset and per-target row counts are defined by the contract; complete expected values are versioned in the baseline snapshot.

## Contracts and grains

The current Azure implementation is a full-refresh snapshot:

- `fact_sales` has one row per `(OrderID, OrderItemID)`; order counts are distinct.
- `fact_reviews` has one row per `(OrderID, ReviewID)` and relates to orders and customers without product attribution.
- Cross-state analysis joins both customer and seller state before deriving `IsCrossState`.
- Representative payment is selected deterministically by lowest payment sequence and then payment type.
- Curated audit timestamps come from the injected stable batch timestamp.
- Fact loads stage business identifiers and resolve them through current dimensions before publication.
- Dimension rows set `RowEffectiveDate` to the batch timestamp, `RowExpirationDate` to null, and `CurrentFlag` to true.

The authoritative schemas, grains, quality rules, replay behavior, and evidence expectations are documented under [`contracts/`](../../contracts/).

## Supported analyses

The Azure notebooks and SQL load paths support:

- sales by product category;
- sales by customer state;
- seller performance;
- monthly sales;
- order status;
- cross-state commerce;
- product-size analysis;
- representative payment-method analysis.

Each analysis has a transformation output and a corresponding SQL target and load path. Checked-in CSVs under [`historical-artifacts/csvs`](historical-artifacts/csvs/) are reference outputs from a full-dataset Azure run; they are not the authoritative contract or evidence of a current managed execution.

## Azure loading order

Execute the dedicated-pool SQL in this order:

1. [`Schema.sql`](synapse-sql/Schema.sql)
2. [`ingest_dimensions.sql`](synapse-sql/load/ingest_dimensions.sql)
3. [`ingest_fact_tables.sql`](synapse-sql/load/ingest_fact_tables.sql)
4. [`ingest_analysis_agg.sql`](synapse-sql/load/ingest_analysis_agg.sql)

Dimensions must load before facts so staged business identifiers can resolve to current surrogate keys. The scripts use the configured managed-identity ADLS locations and perform a full refresh.

## Implementation limitations

- The local gate validates contracts and deterministic transformations; it does not provision Azure, access ADLS, or execute Synapse Spark or dedicated-pool workloads.
- Dimensions do not compare tracked attributes, expire prior rows, or insert historical versions. Operational SCD Type 2 behavior is not implemented in the Azure path.
- The checked-in Azure outputs do not include payment-method results and remain non-authoritative reference artifacts.
- Managed-service execution, operational monitoring, cost, scale, and end-to-end cross-cloud output comparison are outside the current validation evidence.
