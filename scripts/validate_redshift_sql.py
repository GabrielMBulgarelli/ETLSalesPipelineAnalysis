#!/usr/bin/env python3
"""Deterministically inspect the committed Redshift SQL without executing Redshift."""

from __future__ import annotations

import re
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
SQL_ROOT = ROOT / "platforms/aws/sql/redshift"
CONTRACT = ROOT / "contracts/schemas/curated/datasets.yaml"
EXPECTED_FILES = tuple(f"00{number}_{name}.sql" for number, name in enumerate(
    ("schema", "staging", "dimensions", "facts", "aggregates", "audit"), start=1
))
DIMENSIONS = {
    "dim_customer", "dim_product", "dim_seller", "dim_geography", "dim_date", "dim_order_status",
}
FACTS = {"fact_sales", "fact_reviews"}
AGGREGATES = {
    "sales_by_state", "sales_by_category", "monthly_sales", "order_status",
    "cross_state_analysis", "seller_performance", "size_analysis", "payment_methods",
}
FORBIDDEN = {
    r"\bUNLOGGED\b": "PostgreSQL UNLOGGED tables",
    r"\bCREATE\s+(?:UNIQUE\s+)?INDEX\b": "PostgreSQL indexes",
    r"\bCREATE\s+TRIGGER\b": "PostgreSQL triggers",
    r"\bpg_advisory": "PostgreSQL advisory locks",
    r"\bON\s+CONFLICT\b": "PostgreSQL ON CONFLICT",
    r"\bjsonb\b": "PostgreSQL jsonb",
    r"\\gexec\b": "psql meta-commands",
    r"\bGENERATED\s+(?:ALWAYS|BY\s+DEFAULT)\s+AS\s+IDENTITY\b": "unverified PostgreSQL identity syntax",
}


def _created_tables(sql: str, schema: str) -> set[str]:
    return set(re.findall(
        rf"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?{schema}\.([a-z_]+)",
        sql,
        flags=re.IGNORECASE,
    ))


def main() -> int:
    errors: list[str] = []
    actual_files = tuple(sorted(path.name for path in SQL_ROOT.glob("*.sql")))
    if actual_files != EXPECTED_FILES:
        errors.append(f"expected Redshift SQL files {EXPECTED_FILES}, found {actual_files}")

    contents: dict[str, str] = {}
    for filename in EXPECTED_FILES:
        path = SQL_ROOT / filename
        if path.exists():
            contents[filename] = path.read_text(encoding="utf-8")

    combined = "\n".join(contents.values())
    for pattern, description in FORBIDDEN.items():
        if re.search(pattern, combined, flags=re.IGNORECASE):
            errors.append(f"Redshift SQL contains forbidden {description}")

    schema_sql = contents.get("001_schema.sql", "")
    for schema in ("staging", "warehouse", "analytics", "audit"):
        if not re.search(rf"CREATE\s+SCHEMA\s+IF\s+NOT\s+EXISTS\s+{schema}\b", schema_sql, re.IGNORECASE):
            errors.append(f"001_schema.sql does not create {schema}")

    contract = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
    contracted = set(contract["datasets"])
    expected = DIMENSIONS | FACTS | AGGREGATES
    if contracted != expected:
        errors.append(f"validator dataset inventory differs from curated contract: {sorted(contracted ^ expected)}")

    staged = _created_tables(contents.get("002_staging.sql", ""), "staging")
    if staged != expected | {"publication_datasets"}:
        errors.append(f"staging coverage mismatch: missing={sorted(expected - staged)}, extra={sorted(staged - expected - {'publication_datasets'})}")
    published_dimensions = _created_tables(contents.get("003_dimensions.sql", ""), "warehouse")
    if published_dimensions != DIMENSIONS:
        errors.append(
            "dimension coverage mismatch: "
            f"missing={sorted(DIMENSIONS - published_dimensions)}, extra={sorted(published_dimensions - DIMENSIONS)}"
        )
    published_facts = _created_tables(contents.get("004_facts.sql", ""), "warehouse")
    if published_facts != FACTS:
        errors.append(f"fact coverage mismatch: missing={sorted(FACTS - published_facts)}, extra={sorted(published_facts - FACTS)}")
    published_aggregates = _created_tables(contents.get("005_aggregates.sql", ""), "analytics")
    if published_aggregates != AGGREGATES:
        errors.append(
            "aggregate coverage mismatch: "
            f"missing={sorted(AGGREGATES - published_aggregates)}, extra={sorted(published_aggregates - AGGREGATES)}"
        )

    if re.search(r"warehouse\.fact_reviews[\s\S]*?Product(?:ID|Key)", contents.get("004_facts.sql", ""), re.IGNORECASE):
        errors.append("fact_reviews must not gain product attribution")
    payment_sql = "\n".join(
        line for line in combined.splitlines() if "payment_methods" in line.lower() or "payment_value" in line.lower()
    )
    if re.search(r"payment_value", payment_sql, re.IGNORECASE):
        errors.append("payment_methods SQL must never reference payment_value")
    if combined and not re.search(r"DECIMAL\s*\(\s*38\s*,\s*18\s*\)", combined, re.IGNORECASE):
        errors.append("Redshift SQL does not preserve decimal(38,18)")
    audit_sql = contents.get("006_audit.sql", "")
    required_atomic = ("LOCK audit.warehouse_publication_lock", "CREATE OR REPLACE PROCEDURE audit.publish_warehouse")
    for required_sql in required_atomic:
        if required_sql.lower() not in audit_sql.lower():
            errors.append(f"atomic publication SQL is missing {required_sql}")
    procedure = audit_sql.split("CREATE OR REPLACE PROCEDURE", 1)[-1]
    for forbidden in ("TRUNCATE", "COMMIT", "ROLLBACK"):
        if re.search(rf"\b{forbidden}\b", procedure, re.IGNORECASE):
            errors.append(f"publication procedure contains forbidden {forbidden}")
    if audit_sql.upper().count("CALL AUDIT.PUBLISH_WAREHOUSE") > 0:
        errors.append("publication CALL belongs in the loader, not the DDL")

    postgres_sql = ROOT / "platforms/aws/sql/local-postgres"
    overlap = {path.resolve() for path in SQL_ROOT.glob("*.sql")} & {path.resolve() for path in postgres_sql.glob("*.sql")}
    if overlap:
        errors.append("Redshift and PostgreSQL SQL paths overlap")

    if errors:
        raise SystemExit("Redshift SQL validation failed:\n- " + "\n- ".join(errors))
    print("Validated six Redshift SQL files, all 16 staged datasets, and all 16 published datasets.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
