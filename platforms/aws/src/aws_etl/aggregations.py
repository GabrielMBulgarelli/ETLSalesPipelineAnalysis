"""Curated item-price sales aggregates."""

from __future__ import annotations

from typing import Any


def build_aggregations(fact_sales: Any, dimensions: dict[str, Any], batch_timestamp: str) -> dict[str, Any]:
    from pyspark.sql import functions as F

    products = dimensions["dim_product"].select("ProductID", "ProductCategoryNameEnglish", "SizeCategory")
    customers = dimensions["dim_customer"].select("CustomerID", "CustomerState")
    sellers = dimensions["dim_seller"].select("SellerID", "SellerState")
    enriched = fact_sales.join(products, "ProductID").join(customers, "CustomerID").join(sellers, "SellerID")

    def stamp(frame: Any):
        return frame.withColumn("LastUpdated", F.to_timestamp(F.lit(batch_timestamp)))

    def base(group_fields: list[str], delivery: bool = False, delayed: bool = False):
        expressions = [
            F.countDistinct("OrderID").cast("int").alias("OrdersCount"),
            F.countDistinct("CustomerID").cast("int").alias("UniqueCustomers"),
            F.sum("Price").alias("TotalSales"),
            F.avg("Price").alias("AvgItemPrice"),
        ]
        if delivery:
            expressions.append(F.avg("TotalDays").alias("AvgDeliveryTime"))
        if delayed:
            expressions.append(F.countDistinct(F.when(F.col("IsDelayed"), F.col("OrderID"))).cast("int").alias("DelayedOrders"))
        return stamp(enriched.groupBy(*group_fields).agg(*expressions))

    sales_by_category = base(["ProductCategoryNameEnglish"], delivery=True, delayed=True)
    sales_by_state = base(["CustomerState"], delivery=True)
    monthly_sales = stamp(enriched.groupBy(
        F.year("OrderPurchaseTimestamp").cast("int").alias("Year"),
        F.month("OrderPurchaseTimestamp").cast("int").alias("Month"),
    ).agg(
        F.countDistinct("OrderID").cast("int").alias("OrdersCount"),
        F.countDistinct("CustomerID").cast("int").alias("UniqueCustomers"),
        F.sum("Price").alias("TotalSales"),
        F.avg("Price").alias("AvgItemPrice"),
    ))
    order_status = stamp(enriched.groupBy(
        "ProductCategoryNameEnglish", F.col("StatusID").alias("OrderStatus")
    ).agg(
        F.countDistinct("OrderID").cast("int").alias("OrdersCount"),
        F.sum("Price").alias("TotalSales"),
        F.countDistinct("CustomerID").cast("int").alias("UniqueCustomers"),
    ))
    cross_state = stamp(enriched.groupBy("ProductCategoryNameEnglish", "IsCrossState").agg(
        F.countDistinct("OrderID").cast("int").alias("OrdersCount"),
        F.sum("Price").alias("TotalSales"),
        F.avg("TotalDays").alias("AvgDeliveryTime"),
        (F.countDistinct(F.when(F.col("IsDelayed"), F.col("OrderID"))) / F.countDistinct("OrderID")).alias("DelayRate"),
    ))
    seller_performance = stamp(enriched.groupBy("SellerID", "SellerState").agg(
        F.countDistinct("OrderID").cast("int").alias("OrdersCount"),
        F.sum("Price").alias("TotalSales"),
        F.avg("FreightValue").alias("AvgShippingCost"),
        F.avg("TotalDays").alias("AvgDeliveryTime"),
        F.countDistinct(F.when(F.col("IsDelayed"), F.col("OrderID"))).cast("int").alias("DelayedOrders"),
        (F.countDistinct(F.when(F.col("IsDelayed"), F.col("OrderID"))) / F.countDistinct("OrderID")).alias("DelayRate"),
    ))
    size_analysis = stamp(enriched.groupBy("ProductCategoryNameEnglish", "SizeCategory").agg(
        F.countDistinct("OrderID").cast("int").alias("OrdersCount"),
        F.sum("Price").alias("TotalSales"),
        F.avg("FreightValue").alias("AvgShippingCost"),
    ))
    payment_methods = stamp(enriched.filter(F.col("PaymentType").isNotNull()).groupBy("PaymentType").agg(
        F.countDistinct("OrderID").cast("int").alias("OrdersCount"),
        F.sum("Price").alias("TotalSales"),
        (F.sum("Price") / F.countDistinct("OrderID")).alias("AvgOrderValue"),
        F.countDistinct("CustomerID").cast("int").alias("UniqueCustomers"),
    ))
    return {
        "sales_by_state": sales_by_state,
        "sales_by_category": sales_by_category,
        "monthly_sales": monthly_sales,
        "order_status": order_status,
        "cross_state_analysis": cross_state,
        "seller_performance": seller_performance,
        "size_analysis": size_analysis,
        "payment_methods": payment_methods,
    }
