# deterministic baseline Unresolved Decisions

These items are explicit boundaries, not hidden baseline behavior:

1. Historical SCD Type 2 behavior is absent. A later contract phase must define change detection, effective/expiration boundaries, late-arriving dimensions, and replay semantics before implementation.
2. The representative-payment rule is intentionally lossy. A separate payment fact/bridge may replace it if split-payment analysis is required; until then the lowest `payment_sequential` rule is authoritative.
3. The deterministic baseline runner verifies logical behavior locally, but the corrected notebook and T-SQL were not executed against a live Synapse workspace or dedicated SQL pool because no cloud environment/credentials are part of the repository gate.
4. Explicit production-scale Spark raw schemas, quality thresholds, and cross-platform equivalence belong to later plan phases. The deterministic baseline contract records the observable baseline types and fixture behavior only.
5. Existing `notebooks/output/csvs` files predate this audit. They are retained as historical Azure review artifacts and should be regenerated only during a verified cloud execution phase.
