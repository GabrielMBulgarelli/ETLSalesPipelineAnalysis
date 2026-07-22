# Azure Synapse implementation

The Azure implementation is the locally verified baseline. Its deterministic behavior and repository contracts pass locally, but the moved assets have not been revalidated in a live Synapse workspace or dedicated SQL pool.

## Architecture and flow

![Azure ETL architecture](../../docs/architecture/azure/architecture-flow.svg)

Synapse orchestrates three notebooks in order:

1. [`01_EcomSales_Raw_Ingest.ipynb`](entrypoints/synapse-notebooks/01_EcomSales_Raw_Ingest.ipynb) ingests the nine source datasets into the raw layer.
2. [`02_EcomSales_Processed_Transform.ipynb`](entrypoints/synapse-notebooks/02_EcomSales_Processed_Transform.ipynb) cleans, translates, and deduplicates the processed datasets.
3. [`03_EcomSales_Curated_Analytics.ipynb`](entrypoints/synapse-notebooks/03_EcomSales_Curated_Analytics.ipynb) builds dimensions, facts, and supported aggregates.

Additional diagrams describe the [source dataset](../../docs/architecture/azure/dataset_schema_overview.png), [dimensional model](../../docs/architecture/azure/dimensions_facts_tables.svg), [aggregates](../../docs/architecture/azure/aggregation_tables.svg), and [notebook pipeline](../../docs/architecture/azure/simplified_notebook_pipeline.png). The editable source is [`ELTDiagram.drawio`](../../docs/architecture/azure/ELTDiagram.drawio).

## Moved Azure assets

| Asset | Location |
|---|---|
| Synapse notebooks | [`entrypoints/synapse-notebooks/`](entrypoints/synapse-notebooks/) |
| Warehouse DDL and loaders | [`sql/synapse/`](sql/synapse/) |
| Synapse pipelines | [`runtime/cloud/synapse/pipelines/`](runtime/cloud/synapse/pipelines/) |
| Data Factory templates | [`runtime/cloud/data-factory/pipelines/`](runtime/cloud/data-factory/pipelines/) |
| Resource commands | [`runtime/cloud/ResourceCommands.txt`](runtime/cloud/ResourceCommands.txt) |
| Azure contract tests | [`tests/`](tests/) |

## Warehouse load order

After the notebooks publish curated Parquet, execute:

1. [`Schema.sql`](sql/synapse/Schema.sql)
2. [`ingest_dimensions.sql`](sql/synapse/load/ingest_dimensions.sql)
3. [`ingest_fact_tables.sql`](sql/synapse/load/ingest_fact_tables.sql)
4. [`ingest_analysis_agg.sql`](sql/synapse/load/ingest_analysis_agg.sql)

Dimensions must load before facts so business identifiers resolve to current surrogate keys. The scripts retain the existing managed-identity ADLS URLs and full-refresh behavior.

## Contracts, artifacts, and limitations

Azure must conform to the shared [`baseline contract`](../../contracts/contracts.yaml), fixtures, and expected snapshot. The [baseline audit](../../platforms/azure/baseline.md) records the corrected behavior and the [execution guide](../../platforms/azure/baseline.md) explains local reproduction.

Local verification does not provision Azure, access ADLS, or execute Synapse Spark or dedicated-pool workloads. Operational SCD Type 2, production-scale schemas, quality severities, and cross-platform equivalence remain future decisions.
