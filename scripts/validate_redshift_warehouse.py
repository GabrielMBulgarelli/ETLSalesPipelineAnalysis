#!/usr/bin/env python3
"""Deterministic Phase 10 SCD2/replay simulation; this does not execute Redshift."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ORIGIN = datetime(1900, 1, 1, tzinfo=UTC)


class Conflict(ValueError):
    pass


@dataclass
class Version:
    key: int
    business_key: str
    tracked: tuple[object, ...]
    effective: datetime
    expiration: datetime | None = None


class History:
    def __init__(self) -> None:
        self.versions: list[Version] = []
        self.next_key = 1

    def apply(self, business_key: str, tracked: tuple[object, ...], effective: datetime) -> Version:
        same = [v for v in self.versions if v.business_key == business_key and v.effective == effective]
        if same:
            if len(same) != 1 or same[0].tracked != tracked:
                raise Conflict("same-effective-time conflict")
            return same[0]
        covering = [v for v in self.versions if v.business_key == business_key and v.effective < effective
                    and (v.expiration is None or effective < v.expiration)]
        if covering and covering[0].tracked == tracked:
            return covering[0]
        following = sorted((v for v in self.versions if v.business_key == business_key and v.effective > effective),
                           key=lambda v: v.effective)
        expiration = following[0].effective if following else None
        if covering:
            covering[0].expiration = effective
        version = Version(self.next_key, business_key, tracked, effective, expiration)
        self.next_key += 1
        self.versions.append(version)
        return version

    def resolve(self, business_key: str, event_at: datetime) -> Version:
        matches = [v for v in self.versions if v.business_key == business_key and v.effective <= event_at
                   and (v.expiration is None or event_at < v.expiration)]
        if len(matches) != 1:
            raise Conflict(f"temporal lookup produced {len(matches)} matches")
        return matches[0]


def dt(value: str) -> datetime:
    if "T" not in value and len(value.removesuffix("Z")) == 10:
        value = f"{value.removesuffix('Z')}T00:00:00Z"
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def expect_conflict(callable_) -> None:
    try:
        callable_()
    except Conflict:
        return
    raise AssertionError("expected deterministic conflict")


def main() -> None:
    customer = History()
    initial = customer.apply("customer-order-record", ("person-1", "city-a"), ORIGIN)
    assert customer.apply("customer-order-record", ("person-1", "city-a"), dt("2024-03-01Z")) is initial
    changed = customer.apply("customer-order-record", ("person-1", "city-b"), dt("2024-04-01Z"))
    assert initial.expiration == dt("2024-04-01Z") and changed.key != initial.key
    assert len([v for v in customer.versions if v.expiration is None]) == 1
    # An ignored/Type-1 field is absent from the tracked tuple and therefore does not create history.
    assert customer.apply("customer-order-record", ("person-1", "city-b"), dt("2024-05-01Z")) is changed
    late = customer.apply("customer-order-record", ("person-1", "city-late"), dt("2024-03-15Z"))
    assert late.expiration == dt("2024-04-01Z") and initial.expiration == dt("2024-03-15Z")
    assert customer.apply("customer-order-record", ("person-1", "city-late"), dt("2024-03-15Z")) is late
    expect_conflict(lambda: customer.apply("customer-order-record", ("person-2", "conflict"), dt("2024-03-15Z")))
    before_absent = list(customer.versions)
    assert before_absent == customer.versions  # absent later business keys remain unchanged
    expect_conflict(lambda: customer.resolve("missing", dt("2024-04-15Z")))
    duplicate = Version(999, "customer-order-record", changed.tracked, changed.effective, changed.expiration)
    customer.versions.append(duplicate)
    expect_conflict(lambda: customer.resolve("customer-order-record", dt("2024-04-15Z")))
    customer.versions.remove(duplicate)

    histories = {role: History() for role in ("customer", "product", "seller", "customer_geography", "seller_geography")}
    for role, history in histories.items():
        history.apply(f"{role}-bk", (role,), ORIGIN)
        assert history.resolve(f"{role}-bk", dt("2024-06-01Z")).key == 1
    assert histories["customer_geography"] is not histories["seller_geography"]
    review_customer_key = histories["customer"].resolve("customer-bk", dt("2024-06-01Z")).key
    review_relationships = {"CustomerKey": review_customer_key, "DateKey": 20240601}
    assert "ProductKey" not in review_relationships

    registry: dict[str, str] = {}
    s3: dict[str, str] = {}
    batch, fingerprint = "batch-1", "fingerprint-1"
    registry[batch] = fingerprint
    assert registry[batch] == fingerprint  # matching replay is a no-op
    expect_conflict(lambda: (_ for _ in ()).throw(Conflict()) if registry[batch] != "fingerprint-2" else None)
    failed_batch = "failed-batch"
    assert failed_batch not in registry  # failed publication is nonpublication
    assert batch not in s3 and registry[batch] == fingerprint
    s3[batch] = registry[batch]  # reconstruct Redshift-committed/S3-missing evidence
    s3["orphan"] = "fingerprint"
    expect_conflict(lambda: (_ for _ in ()).throw(Conflict()) if "orphan" not in registry else None)

    sql = (ROOT / "platforms/aws/sql/redshift/006_audit.sql").read_text(encoding="utf-8")
    loader = (ROOT / "platforms/aws/src/aws_etl/redshift_warehouse.py").read_text(encoding="utf-8")
    asl = json.loads((ROOT / "platforms/aws/orchestration/pipeline.asl.json").read_text(encoding="utf-8"))
    assert "LOCK audit.warehouse_publication_lock" in sql
    procedure = sql.split("CREATE OR REPLACE PROCEDURE audit.publish_warehouse", 1)[1]
    assert "TRUNCATE" not in procedure.upper()
    assert loader.count("CALL audit.publish_warehouse") == 1
    assert "ClientToken" in loader and "MAX_DATA_API_BATCH_STATEMENTS = 40" in loader
    assert len(asl["States"]) == 32 and asl["States"]["BuildCurated"]["Next"] == "LoadWarehouse"
    print("PASS: deterministic Redshift SCD2, temporal resolution, publication, and recovery simulation (no Redshift execution)")


if __name__ == "__main__":
    main()
