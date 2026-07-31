"""Curated sales and review fact builders."""

from __future__ import annotations

from typing import Any


def representative_payments(payments: Any):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    window = Window.partitionBy("order_id").orderBy(F.col("payment_sequential"), F.col("payment_type"))
    return payments.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").select(
        "order_id", F.col("payment_type").alias("PaymentType")
    )


def build_facts(frames: dict[str, Any]) -> dict[str, Any]:
    from pyspark.sql import functions as F

    orders = frames["orders"].alias("o")
    customers = frames["customers"].select(
        "customer_id", "customer_zip_code_prefix", "customer_state"
    ).alias("c")
    sellers = frames["sellers"].select("seller_id", "seller_state").alias("s")
    payments = representative_payments(frames["order_payments"]).alias("p")
    items = frames["order_items"].alias("i")

    sales_source = (
        items.join(orders, F.col("i.order_id") == F.col("o.order_id"), "inner")
        .join(customers, F.col("o.customer_id") == F.col("c.customer_id"), "inner")
        .join(sellers, F.col("i.seller_id") == F.col("s.seller_id"), "inner")
        .join(payments, F.col("i.order_id") == F.col("p.order_id"), "left")
    )
    fact_sales = sales_source.select(
        F.col("i.order_id").alias("OrderID"),
        F.col("i.order_item_id").alias("OrderItemID"),
        F.col("o.customer_id").alias("CustomerID"),
        F.col("i.product_id").alias("ProductID"),
        F.col("i.seller_id").alias("SellerID"),
        F.date_format("o.order_purchase_timestamp", "yyyyMMdd").cast("int").alias("DateKey"),
        F.col("o.order_status").alias("StatusID"),
        F.col("c.customer_zip_code_prefix").alias("ZipCodePrefix"),
        F.col("o.order_purchase_timestamp").alias("OrderPurchaseTimestamp"),
        F.col("o.order_delivered_customer_date").alias("OrderDeliveredCustomerDate"),
        F.col("i.price").alias("Price"),
        F.col("i.freight_value").alias("FreightValue"),
        (F.col("i.price") + F.col("i.freight_value")).alias("TotalItemValue"),
        F.when(F.col("o.shipping_days") >= 0, F.col("o.shipping_days")).cast("int").alias("ShippingDays"),
        F.when(F.col("o.delivery_days") >= 0, F.col("o.delivery_days")).cast("int").alias("DeliveryDays"),
        F.when(F.col("o.total_days") >= 0, F.col("o.total_days")).cast("int").alias("TotalDays"),
        F.coalesce(F.col("o.is_delayed"), F.lit(False)).alias("IsDelayed"),
        F.coalesce(F.col("o.delay_days"), F.lit(0)).cast("int").alias("DelayDays"),
        (F.col("c.customer_state") != F.col("s.seller_state")).alias("IsCrossState"),
        F.col("p.PaymentType"),
    )

    fact_reviews = frames["order_reviews"].alias("r").join(
        orders, F.col("r.order_id") == F.col("o.order_id"), "inner"
    ).select(
        F.col("r.order_id").alias("OrderID"),
        F.col("r.review_id").alias("ReviewID"),
        F.col("o.customer_id").alias("CustomerID"),
        F.date_format("o.order_purchase_timestamp", "yyyyMMdd").cast("int").alias("DateKey"),
        F.col("r.review_score").alias("ReviewScore"),
        F.col("r.review_comment_message").alias("ReviewCommentMessage"),
        F.col("r.review_creation_date").alias("ReviewCreationDate"),
        F.col("r.review_answer_timestamp").alias("ReviewAnswerTimestamp"),
        F.datediff("r.review_answer_timestamp", "r.review_creation_date").cast("int").alias("ReviewResponseDays"),
    )
    return {"fact_sales": fact_sales, "fact_reviews": fact_reviews}
