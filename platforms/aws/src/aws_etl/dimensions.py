"""Curated snapshot dimension builders."""

from __future__ import annotations

from typing import Any


def _snapshot_columns(frame: Any, batch_timestamp: str):
    from pyspark.sql import functions as F

    return (
        frame.withColumn("RowEffectiveDate", F.to_timestamp(F.lit(batch_timestamp)))
        .withColumn("RowExpirationDate", F.lit(None).cast("timestamp"))
        .withColumn("CurrentFlag", F.lit(True))
    )


def build_dimensions(frames: dict[str, Any], batch_timestamp: str) -> dict[str, Any]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    customers = _snapshot_columns(
        frames["customers"].select(
            F.col("customer_id").alias("CustomerID"),
            F.col("customer_unique_id").alias("CustomerUniqueID"),
            F.col("customer_zip_code_prefix").alias("CustomerZipCodePrefix"),
            F.col("customer_city").alias("CustomerCity"),
            F.col("customer_state").alias("CustomerState"),
        ), batch_timestamp,
    )

    volume = F.col("product_length_cm") * F.col("product_height_cm") * F.col("product_width_cm")
    products = _snapshot_columns(
        frames["products"].select(
            F.col("product_id").alias("ProductID"),
            F.col("product_category_name").alias("ProductCategoryName"),
            F.col("product_category_name_english").alias("ProductCategoryNameEnglish"),
            F.col("product_weight_g").alias("ProductWeightG"),
            F.col("product_length_cm").alias("ProductLengthCm"),
            F.col("product_height_cm").alias("ProductHeightCm"),
            F.col("product_width_cm").alias("ProductWidthCm"),
            volume.alias("ProductVolumeCm3"),
            F.when(volume.isNull(), "Unknown")
            .when(volume < 1000, "Small")
            .when(volume < 8000, "Medium")
            .otherwise("Large").alias("SizeCategory"),
        ), batch_timestamp,
    )

    sellers = _snapshot_columns(
        frames["sellers"].select(
            F.col("seller_id").alias("SellerID"),
            F.col("seller_zip_code_prefix").alias("SellerZipCodePrefix"),
            F.col("seller_city").alias("SellerCity"),
            F.col("seller_state").alias("SellerState"),
        ), batch_timestamp,
    )

    geo_window = Window.partitionBy("geolocation_zip_code_prefix").orderBy(
        "geolocation_city", "geolocation_state", "geolocation_lat", "geolocation_lng"
    )
    geography = _snapshot_columns(
        frames["geolocation"].withColumn("_row", F.row_number().over(geo_window)).filter("_row = 1").select(
            F.col("geolocation_zip_code_prefix").alias("ZipCodePrefix"),
            F.col("geolocation_city").alias("City"),
            F.col("geolocation_state").alias("State"),
            F.col("geolocation_lat").alias("Latitude"),
            F.col("geolocation_lng").alias("Longitude"),
            F.lit(None).cast("string").alias("Region"),
        ), batch_timestamp,
    )

    bounds = frames["orders"].select(
        F.trunc(F.min(F.to_date("order_purchase_timestamp")), "month").alias("start"),
        F.max(F.to_date(F.coalesce("order_estimated_delivery_date", "order_purchase_timestamp"))).alias("end"),
    )
    dates = bounds.select(F.explode(F.sequence("start", "end")).alias("Date"))
    dim_date = dates.select(
        F.date_format("Date", "yyyyMMdd").cast("int").alias("DateKey"),
        F.date_format("Date", "yyyyMMdd").cast("int").alias("DateID"),
        "Date",
        F.year("Date").alias("Year"),
        F.month("Date").alias("Month"),
        F.dayofmonth("Date").alias("Day"),
        F.quarter("Date").alias("Quarter"),
        F.weekofyear("Date").alias("WeekOfYear"),
        F.dayofweek("Date").alias("DayOfWeek"),
        F.dayofweek("Date").isin(1, 7).alias("IsWeekend"),
        F.date_format("Date", "MMMM").alias("MonthName"),
        F.date_format("Date", "EEEE").alias("DayName"),
        F.year("Date").alias("FiscalYear"),
        F.quarter("Date").alias("FiscalQuarter"),
        F.lit(None).cast("string").alias("Holiday"),
        F.lit(False).alias("IsHoliday"),
    )

    statuses = [
        ("CREATED", "Created", False, False, False, False),
        ("APPROVED", "Approved", True, False, False, False),
        ("SHIPPED", "Shipped", True, False, True, False),
        ("DELIVERED", "Delivered", True, True, True, False),
        ("CANCELLED", "Cancelled", False, False, False, True),
        ("UNAVAILABLE", "Unavailable", False, False, False, False),
    ]
    dim_order_status = frames["orders"].sparkSession.createDataFrame(
        statuses,
        "StatusID string, StatusDescription string, IsApproved boolean, IsDelivered boolean, IsShipped boolean, IsCancelled boolean",
    )

    return {
        "dim_customer": customers,
        "dim_product": products,
        "dim_seller": sellers,
        "dim_geography": geography,
        "dim_date": dim_date,
        "dim_order_status": dim_order_status,
    }
