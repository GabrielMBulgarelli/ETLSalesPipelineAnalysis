# Dual-Platform Sales ETL Pipeline

This repository defines a provider-neutral sales ETL baseline and platform-specific implementations for Azure and, in a future phase, AWS. Shared contracts, deterministic fixtures, expected snapshots, and the local fixture runner establish the behavior every platform must preserve.

## Platform status

| Platform | Status | Scope |
|---|---|---|
| [Azure](platforms/azure/README.md) | Baseline verified locally; cloud not revalidated | Synapse notebooks, orchestration templates, SQL warehouse loaders, and historical review artifacts |
| [AWS](platforms/aws/README.md) | Not implemented | Planned only; no AWS runtime or infrastructure code exists |

## Shared baseline

The provider-neutral contract is [`contracts/contracts.yaml`](contracts/contracts.yaml). It is exercised by the deterministic raw fixtures in [`contracts/fixtures/`](contracts/fixtures/), compared with [`contracts/expected/baseline_snapshot.json`](contracts/expected/baseline_snapshot.json), and executed by [`scripts/baseline_fixture.py`](scripts/baseline_fixture.py).

The sales fact has one row per order item. Reviews are linked to orders and customers, not products. These grains and the transformation rules are shared requirements, not Azure-specific behavior.

Phase 2 schema and rule namespaces are reserved in [`contracts/schemas/`](contracts/schemas/) and [`contracts/rules/`](contracts/rules/); they are documentation placeholders only.

## Local verification

Python 3.10 or newer is sufficient; Azure credentials are not required.

```bash
make baseline-test
```

This runs the provider-neutral behavioral tests, Azure-specific contract tests, repository-layout/link checks, and a deterministic snapshot comparison. See [baseline execution](platforms/azure/baseline.md) and the [Azure baseline audit](platforms/azure/baseline.md) for the evidence and limitations.

## Repository layout

```text
contracts/              Provider-neutral contracts, fixtures, and snapshots
docs/                   Architecture and baseline evidence
platforms/azure/        Verified-local Azure implementation and historical artifacts
platforms/aws/          Planned platform documentation only
scripts/                Provider-neutral deterministic fixture runner
tests/         Shared behavioral and repository-layout tests
```

The checked-in CSV outputs under Azure are pre-audit historical review artifacts. They are non-authoritative and do not demonstrate a current managed-cloud execution.
