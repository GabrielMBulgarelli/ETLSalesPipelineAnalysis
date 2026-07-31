# Provider-Neutral Contract Decisions and Boundaries

These decisions define provider-neutral behavior. They do not claim that a provider runtime or durable audit store has been implemented.

1. Contract version 1 dimensions are snapshot-only. Historical effective and expiration boundaries, late-arriving dimension changes, and fact-time dimension resolution are deferred to the historical warehouse runtime. Redshift Serverless is the AWS-specific implementation of that runtime.
2. Representative payment selects the first row ordered by `(payment_sequential, payment_type)`. It provides descriptive item-price attribution only. Curated monetary measures do not aggregate or allocate `payment_value`. Complete tendered-value and split-payment reporting is deferred to a future payment fact at `(OrderID, PaymentSequential)` grain.
3. Validator reports retain the four-level taxonomy: `warning`, `reject-row`, `reject-dataset`, and `fail-batch`. Warnings map to `PASSED`; row rejection maps to `PASSED_WITH_REJECTIONS`; every dataset rejection and batch failure maps to `FAILED`. Only `reject-dataset` and `fail-batch` produce a nonzero validator exit. Optional datasets are not defined by contract version 1.
4. Replay is manifest-driven. An identical successful manifest or identical successful content under a new batch ID is a no-op. A failed latest attempt is retried only when it is explicitly marked retryable. Replaying an identical deterministic `reject-dataset` or `fail-batch` result does not reprocess data; it creates new audit evidence reusing the prior failure. Changing immutable content under an existing batch ID fails the batch.
5. Every submission creates immutable audit evidence, including no-op, reused-failure, retry, rejection, and failure submissions. This contract defines only the provider-neutral audit schema and expected evidence. Durable persistence is deferred to the corresponding provider runtime.
6. `fact_sales` has grain `(OrderID, OrderItemID)`.
7. `fact_reviews` has grain `(OrderID, ReviewID)` and has no product attribution in contract version 1.
8. `StatusID` is a string wherever it appears.
9. The validator and fixture runner provide local logical evidence. They do not establish that Synapse, AWS, or another managed provider service was executed.
10. Existing Azure CSV outputs remain historical review artifacts and are not authoritative contract evidence.
