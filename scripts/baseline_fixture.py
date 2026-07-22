"""Deterministic executable specification for the corrected Azure baseline.

This standard-library runner is intentionally limited to the deterministic baseline fixture. It
does not replace the Synapse/PySpark implementation; it makes the baseline
contract executable on machines without Spark or cloud credentials.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path


BATCH_TIMESTAMP = "2018-04-01T00:00:00Z"
DATASETS = {
    "customers": ("olist_customers_dataset.csv", ("customer_id",)),
    "orders": ("olist_orders_dataset.csv", ("order_id",)),
    "order_items": ("olist_order_items_dataset.csv", ("order_id", "order_item_id")),
    "order_payments": ("olist_order_payments_dataset.csv", ("order_id", "payment_sequential")),
    "order_reviews": ("olist_order_reviews_dataset.csv", ("review_id", "order_id")),
    "products": ("olist_products_dataset.csv", ("product_id",)),
    "sellers": ("olist_sellers_dataset.csv", ("seller_id",)),
    "geolocation": (
        "olist_geolocation_dataset.csv",
        ("geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"),
    ),
    "category_translation": ("product_category_name_translation.csv", ("product_category_name",)),
}


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _deduplicate(rows: list[dict[str, str]], key: tuple[str, ...]) -> list[dict[str, str]]:
    unique = {}
    for row in rows:
        unique.setdefault(tuple(row[column] for column in key), row)
    return [unique[item] for item in sorted(unique)]


def _timestamp(value: str) -> datetime | None:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S") if value else None


def _money(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.01")))


def _average(values: list[Decimal | int]) -> str | None:
    if not values:
        return None
    return _money(sum((Decimal(value) for value in values), Decimal()) / len(values))


def _status(order: dict[str, str]) -> str:
    source_status = order["order_status"].upper()
    if source_status in {"CANCELLED", "UNAVAILABLE"}:
        return source_status
    if order["order_delivered_customer_date"]:
        return "DELIVERED"
    if order["order_delivered_carrier_date"]:
        return "SHIPPED"
    if order["order_approved_at"]:
        return "APPROVED"
    return "CREATED"


def _dimension_summary(rows: list[dict[str, str]], key: str, batch_timestamp: str) -> dict:
    return {
        "row_count": len(rows),
        "business_keys": sorted(row[key] for row in rows),
        "RowEffectiveDate": batch_timestamp,
        "CurrentFlag": True,
        "history_behavior": "full-refresh snapshot; operational SCD Type 2 is not implemented",
    }


def _build_aggregations(
    facts: list[dict], product_attributes: dict[str, dict], batch_timestamp: str
) -> dict[str, list[dict]]:
    definitions = {
        "sales_by_category": lambda row: (product_attributes[row["ProductID"]]["category"],),
        "sales_by_state": lambda row: (row["CustomerState"],),
        "seller_performance": lambda row: (row["SellerID"], row["SellerState"]),
        "monthly_sales": lambda row: (row["OrderPurchaseTimestamp"][:7],),
        "order_status": lambda row: (product_attributes[row["ProductID"]]["category"], row["StatusID"]),
        "cross_state_analysis": lambda row: (
            product_attributes[row["ProductID"]]["category"],
            row["IsCrossState"],
        ),
        "size_analysis": lambda row: (
            product_attributes[row["ProductID"]]["category"],
            product_attributes[row["ProductID"]]["size"],
        ),
        "payment_methods": lambda row: (row["PaymentType"],),
    }
    grouped: dict[str, dict[tuple, list[dict]]] = {}
    for name, key_function in definitions.items():
        groups = defaultdict(list)
        for row in facts:
            groups[key_function(row)].append(row)
        grouped[name] = groups

    results: dict[str, list[dict]] = {}
    for name, groups in grouped.items():
        rows = []
        for key, members in sorted(groups.items(), key=lambda item: str(item[0])):
            orders = {row["OrderID"] for row in members}
            customers = {row["CustomerID"] for row in members}
            common = {
                "OrdersCount": len(orders),
                "TotalSales": _money(sum((Decimal(row["Price"]) for row in members), Decimal())),
                "LastUpdated": batch_timestamp,
            }
            if name == "sales_by_category":
                row = {
                    "ProductCategoryNameEnglish": key[0],
                    **common,
                    "UniqueCustomers": len(customers),
                    "AvgItemPrice": _average([Decimal(item["Price"]) for item in members]),
                    "AvgDeliveryTime": _average([item["TotalDays"] for item in members if item["TotalDays"] >= 0]),
                    "DelayedOrders": len({item["OrderID"] for item in members if item["IsDelayed"]}),
                }
            elif name == "sales_by_state":
                row = {
                    "CustomerState": key[0],
                    **common,
                    "UniqueCustomers": len(customers),
                    "AvgItemPrice": _average([Decimal(item["Price"]) for item in members]),
                    "AvgDeliveryTime": _average([item["TotalDays"] for item in members if item["TotalDays"] >= 0]),
                }
            elif name == "seller_performance":
                delayed = {item["OrderID"] for item in members if item["IsDelayed"]}
                row = {
                    "SellerID": key[0],
                    "SellerState": key[1],
                    **common,
                    "AvgShippingCost": _average([Decimal(item["FreightValue"]) for item in members]),
                    "AvgDeliveryTime": _average([item["TotalDays"] for item in members if item["TotalDays"] >= 0]),
                    "DelayedOrders": len(delayed),
                    "DelayRate": _money(Decimal(len(delayed)) / len(orders)),
                }
            elif name == "monthly_sales":
                year, month = key[0].split("-")
                row = {
                    "Year": int(year),
                    "Month": int(month),
                    **common,
                    "UniqueCustomers": len(customers),
                    "AvgItemPrice": _average([Decimal(item["Price"]) for item in members]),
                }
            elif name == "order_status":
                row = {
                    "ProductCategoryNameEnglish": key[0],
                    "OrderStatus": key[1],
                    **common,
                    "UniqueCustomers": len(customers),
                }
            elif name == "cross_state_analysis":
                delayed = {item["OrderID"] for item in members if item["IsDelayed"]}
                row = {
                    "ProductCategoryNameEnglish": key[0],
                    "IsCrossState": key[1],
                    **common,
                    "AvgDeliveryTime": _average([item["TotalDays"] for item in members if item["TotalDays"] >= 0]),
                    "DelayRate": _money(Decimal(len(delayed)) / len(orders)),
                }
            elif name == "size_analysis":
                row = {
                    "ProductCategoryNameEnglish": key[0],
                    "SizeCategory": key[1],
                    **common,
                    "AvgShippingCost": _average([Decimal(item["FreightValue"]) for item in members]),
                }
            else:
                order_values = defaultdict(Decimal)
                for item in members:
                    order_values[item["OrderID"]] += Decimal(item["Price"])
                row = {
                    "PaymentType": key[0],
                    **common,
                    "AvgOrderValue": _average(list(order_values.values())),
                    "UniqueCustomers": len(customers),
                }
            rows.append(row)
        results[name] = rows
    return results


def build_baseline_snapshot(raw_directory: Path, batch_timestamp: str = BATCH_TIMESTAMP) -> dict:
    """Build the deterministic baseline logical snapshot from the deterministic raw fixture."""
    raw_directory = Path(raw_directory)
    raw = {}
    processed = {}
    for name, (filename, key) in DATASETS.items():
        source_rows = _read_csv(raw_directory / filename)
        rows = _deduplicate(source_rows, key)
        raw[name] = rows
        processed[name] = {
            "row_count": len(rows),
            "duplicates_removed": len(source_rows) - len(rows),
            "deduplication_key": list(key),
        }

    customers = {row["customer_id"]: row for row in raw["customers"]}
    orders = {row["order_id"]: row for row in raw["orders"]}
    products = {row["product_id"]: row for row in raw["products"]}
    sellers = {row["seller_id"]: row for row in raw["sellers"]}
    translations = {
        row["product_category_name"]: row["product_category_name_english"]
        for row in raw["category_translation"]
    }

    primary_payments = {}
    for payment in sorted(
        raw["order_payments"],
        key=lambda row: (row["order_id"], int(row["payment_sequential"]), row["payment_type"]),
    ):
        primary_payments.setdefault(payment["order_id"], payment["payment_type"])

    product_attributes = {}
    for product_id, product in products.items():
        dimensions = [product["product_length_cm"], product["product_height_cm"], product["product_width_cm"]]
        if not all(dimensions):
            volume = None
            size = "Unknown"
        else:
            volume = Decimal(dimensions[0]) * Decimal(dimensions[1]) * Decimal(dimensions[2])
            size = "Small" if volume < 1000 else "Medium" if volume < 8000 else "Large"
        product_attributes[product_id] = {
            "category": translations.get(product["product_category_name"], "uncategorized"),
            "volume": str(volume) if volume is not None else None,
            "size": size,
        }

    rejected_items = [row for row in raw["order_items"] if row["order_id"] not in orders]
    valid_items = [row for row in raw["order_items"] if row["order_id"] in orders]
    fact_sales = []
    for item in sorted(valid_items, key=lambda row: (row["order_id"], int(row["order_item_id"]))):
        order = orders[item["order_id"]]
        customer = customers[order["customer_id"]]
        seller = sellers[item["seller_id"]]
        purchase = _timestamp(order["order_purchase_timestamp"])
        approved = _timestamp(order["order_approved_at"])
        carrier = _timestamp(order["order_delivered_carrier_date"])
        delivered = _timestamp(order["order_delivered_customer_date"])
        estimated = _timestamp(order["order_estimated_delivery_date"])
        is_delayed = delivered > estimated if delivered and estimated else None
        price = Decimal(item["price"])
        freight = Decimal(item["freight_value"])
        fact_sales.append(
            {
                "OrderID": item["order_id"],
                "OrderItemID": int(item["order_item_id"]),
                "CustomerID": order["customer_id"],
                "ProductID": item["product_id"],
                "SellerID": item["seller_id"],
                "DateKey": int(purchase.strftime("%Y%m%d")),
                "StatusID": _status(order),
                "ZipCodePrefix": int(customer["customer_zip_code_prefix"]),
                "CustomerState": customer["customer_state"],
                "SellerState": seller["seller_state"],
                "OrderPurchaseTimestamp": purchase.isoformat(sep=" "),
                "OrderDeliveredCustomerDate": delivered.isoformat(sep=" ") if delivered else None,
                "Price": _money(price),
                "FreightValue": _money(freight),
                "TotalItemValue": _money(price + freight),
                "ShippingDays": (carrier.date() - approved.date()).days if carrier and approved else -1,
                "DeliveryDays": (delivered.date() - carrier.date()).days if delivered and carrier else -1,
                "TotalDays": (delivered.date() - purchase.date()).days if delivered else -1,
                "IsDelayed": is_delayed,
                "DelayDays": (delivered.date() - estimated.date()).days if is_delayed else 0,
                "IsCrossState": customer["customer_state"] != seller["seller_state"],
                "PaymentType": primary_payments.get(item["order_id"]),
            }
        )

    rejected_reviews = []
    fact_reviews = []
    for review in sorted(raw["order_reviews"], key=lambda row: (row["order_id"], row["review_id"])):
        reasons = []
        if review["order_id"] not in orders:
            reasons.append("orphan_review")
        if not 1 <= int(review["review_score"]) <= 5:
            reasons.append("invalid_review_score")
        if reasons:
            rejected_reviews.append({"ReviewID": review["review_id"], "reasons": reasons})
            continue
        order = orders[review["order_id"]]
        created = _timestamp(review["review_creation_date"])
        answered = _timestamp(review["review_answer_timestamp"])
        fact_reviews.append(
            {
                "OrderID": review["order_id"],
                "ReviewID": review["review_id"],
                "CustomerID": order["customer_id"],
                "DateKey": int(_timestamp(order["order_purchase_timestamp"]).strftime("%Y%m%d")),
                "ReviewScore": int(review["review_score"]),
                "ReviewCommentMessage": review["review_comment_message"] or None,
                "ReviewCreationDate": created.isoformat(sep=" "),
                "ReviewAnswerTimestamp": answered.isoformat(sep=" "),
                "ReviewResponseDays": (answered.date() - created.date()).days,
            }
        )

    customer_keys = {key: index for index, key in enumerate(sorted(customers), 1)}
    product_keys = {key: index for index, key in enumerate(sorted(products), 1)}
    seller_keys = {key: index for index, key in enumerate(sorted(sellers), 1)}
    geography_keys = {
        key: index
        for index, key in enumerate(
            sorted({int(row["geolocation_zip_code_prefix"]) for row in raw["geolocation"]}), 1
        )
    }
    status_keys = {name: index for index, name in enumerate(("CREATED", "APPROVED", "SHIPPED", "DELIVERED", "CANCELLED", "UNAVAILABLE"), 1)}
    warehouse_sales = []
    for row in fact_sales:
        warehouse_row = {
            key: value
            for key, value in row.items()
            if key not in {"CustomerID", "ProductID", "SellerID", "StatusID", "ZipCodePrefix", "CustomerState", "SellerState"}
        }
        warehouse_row.update(
            CustomerKey=customer_keys[row["CustomerID"]],
            ProductKey=product_keys[row["ProductID"]],
            SellerKey=seller_keys[row["SellerID"]],
            StatusKey=status_keys[row["StatusID"]],
            GeographyKey=geography_keys[row["ZipCodePrefix"]],
        )
        warehouse_sales.append(warehouse_row)
    warehouse_reviews = []
    for row in fact_reviews:
        warehouse_row = {key: value for key, value in row.items() if key != "CustomerID"}
        warehouse_row["CustomerKey"] = customer_keys[row["CustomerID"]]
        warehouse_reviews.append(warehouse_row)

    dimensions = {
        "dim_customer": _dimension_summary(raw["customers"], "customer_id", batch_timestamp),
        "dim_product": _dimension_summary(raw["products"], "product_id", batch_timestamp),
        "dim_seller": _dimension_summary(raw["sellers"], "seller_id", batch_timestamp),
        "dim_geography": {
            "row_count": len(geography_keys),
            "business_keys": sorted(geography_keys),
            "RowEffectiveDate": batch_timestamp,
            "CurrentFlag": True,
            "history_behavior": "full-refresh snapshot; operational SCD Type 2 is not implemented",
        },
        "dim_date": {
            "row_count": 60,
            "range": ["2018-01-01", "2018-03-01"],
            "business_key": "DateKey",
        },
        "dim_order_status": {"row_count": len(status_keys), "business_keys": list(status_keys)},
    }
    rules = {
        "orphan_order_item": len(rejected_items),
        "orphan_review": sum("orphan_review" in row["reasons"] for row in rejected_reviews),
        "invalid_review_score": sum("invalid_review_score" in row["reasons"] for row in rejected_reviews),
    }
    published_sales = [
        {key: value for key, value in row.items() if key not in {"CustomerState", "SellerState"}}
        for row in fact_sales
    ]
    return {
        "batch_timestamp": batch_timestamp,
        "processed": processed,
        "curated": {
            "dimensions": dimensions,
            "fact_sales": published_sales,
            "fact_reviews": fact_reviews,
            "aggregations": _build_aggregations(fact_sales, product_attributes, batch_timestamp),
        },
        "warehouse": {
            "staging": {"fact_sales": len(fact_sales), "fact_reviews": len(fact_reviews)},
            "fact_sales": warehouse_sales,
            "fact_reviews": warehouse_reviews,
        },
        "quality": {
            "rejected_rows": len(rejected_items) + len(rejected_reviews),
            "rules": rules,
            "rows": [
                *({"dataset": "order_items", "business_key": f'{row["order_id"]}:{row["order_item_id"]}', "reasons": ["orphan_order_item"]} for row in rejected_items),
                *({"dataset": "order_reviews", "business_key": row["ReviewID"], "reasons": row["reasons"]} for row in rejected_reviews),
            ],
        },
        "metrics": {
            "sales_fact_rows": len(fact_sales),
            "unique_sales_orders": len({row["OrderID"] for row in fact_sales}),
            "same_state_items": sum(not row["IsCrossState"] for row in fact_sales),
            "cross_state_items": sum(row["IsCrossState"] for row in fact_sales),
            "delayed_items": sum(row["IsDelayed"] is True for row in fact_sales),
            "gross_item_value": _money(sum((Decimal(row["TotalItemValue"]) for row in fact_sales), Decimal())),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, default=Path("contracts/fixtures"))
    parser.add_argument("--expected", type=Path, default=Path("contracts/expected/baseline_snapshot.json"))
    parser.add_argument("--print", action="store_true", dest="print_snapshot")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    snapshot = build_baseline_snapshot(args.fixture)
    if args.print_snapshot:
        print(json.dumps(snapshot, indent=2, sort_keys=True))
    if args.check:
        expected = json.loads(args.expected.read_text(encoding="utf-8"))
        if snapshot != expected:
            print("baseline snapshot does not match the versioned expectation")
            return 1
        print("baseline snapshot matches the versioned expectation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
