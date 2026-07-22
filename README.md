# Dual-Platform Sales ETL Pipeline

## Product vision

This repository presents a provider-neutral sales ETL pipeline with Azure and AWS implementations. Shared schemas, transformation rules, fixtures, expected outputs, and validation govern the behavior each platform implementation must preserve.

## Platform roadmap

| Platform | Status | Scope |
|---|---|---|
| [Azure](platforms/azure/README.md) | Locally verified; live-cloud validation pending | Synapse notebooks, orchestration templates, and SQL warehouse loaders |
| [AWS](platforms/aws/README.md) | Planned; no runtime or infrastructure implementation yet | Reserved for the AWS implementation when available |

The AWS implementation will occupy this reserved space when available, with its architecture, orchestration, warehouse, and validation evidence added as it is implemented.

## Shared data contract

The provider-neutral contract is [`contracts/contracts.yaml`](contracts/contracts.yaml). Deterministic raw fixtures in [`contracts/fixtures/`](contracts/fixtures/) are compared with [`contracts/expected/baseline_snapshot.json`](contracts/expected/baseline_snapshot.json) by [`scripts/baseline_fixture.py`](scripts/baseline_fixture.py).

The sales fact has one row per order item. Reviews are linked to orders and customers, not products. These grains and transformation rules are shared requirements, not Azure-specific behavior. Phase 2 schema and rule namespaces are reserved in [`contracts/schemas/`](contracts/schemas/) and [`contracts/rules/`](contracts/rules/).

## Azure implementation

The Azure implementation is the locally verified baseline; its platform guide documents the Synapse notebooks, orchestration templates, and warehouse loaders.

### Architecture

This diagram communicates the Azure ETL flow from source ingestion through processed and curated layers.

![Azure ETL architecture](docs/architecture/azure/architecture-flow.svg)

### Dimensional model

This diagram communicates the dimensions and facts produced for sales analytics.

![Azure dimensional model](docs/architecture/azure/dimensions_facts_tables.svg)

### Aggregate datasets

This diagram communicates the aggregate datasets built from the curated model.

![Azure aggregate datasets](docs/architecture/azure/aggregation_tables.svg)

### Notebook pipeline

This diagram communicates the sequence of Azure notebooks in the pipeline.

![Azure notebook pipeline](docs/architecture/azure/simplified_notebook_pipeline.png)

## AWS implementation

AWS is planned. No AWS runtime, infrastructure deployment, or architecture diagram exists yet; the platform space is reserved for its future implementation.

## Local verification

Python 3.10 or newer is sufficient; Azure credentials are not required.

```bash
make baseline-test
```

This runs the provider-neutral behavioral tests, Azure-specific contract tests, repository-layout and link checks, and a deterministic snapshot comparison. See the [baseline execution guide](platforms/azure/baseline.md) and [Azure baseline audit](platforms/azure/baseline.md) for current evidence and limitations.

## Repository layout

```text
contracts/              Provider-neutral schemas, rules, fixtures, and expected outputs
docs/                   Architecture and baseline evidence
platforms/azure/        Locally verified Azure implementation
platforms/aws/          Reserved AWS platform space
scripts/                Provider-neutral deterministic fixture runner
tests/         Shared behavioral and repository-layout tests
```
