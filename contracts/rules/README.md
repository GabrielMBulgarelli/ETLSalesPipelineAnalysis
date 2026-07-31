# Shared contract rules

These version 1 YAML files define provider-neutral business keys, fact grains, referential-integrity rules, quality severities, incremental replay behavior, and the current snapshot-only dimension policy.

`scripts/validate_contracts.py` is the reference local validator for contract structure, cross-references, replay semantics, and fixture conformance. It does not represent a provider runtime or durable audit implementation.
