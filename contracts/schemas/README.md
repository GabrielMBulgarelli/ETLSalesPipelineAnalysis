# Shared schema contracts

The version 1 YAML catalogs define provider-neutral logical schemas.

- Raw contracts preserve original CSV headers in `source_name`.
- Processed contracts expose canonical `snake_case` field names.
- Curated contracts preserve the verified baseline output names.
- Audit contracts define provider-neutral submission evidence without claiming durable persistence.

Storage paths and provider-specific physical types are deliberately excluded. An empty `partition_columns` list means the verified baseline is unpartitioned. Provider implementations may use different physical types only when they preserve the logical type, nullability, and invalid-value behavior declared by the contract.
