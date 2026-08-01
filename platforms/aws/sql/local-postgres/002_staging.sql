SET timezone = 'UTC';

CREATE UNLOGGED TABLE IF NOT EXISTS staging.dim_customer (
  "LoadAttemptID" text NOT NULL,
  "CustomerID" text NOT NULL,
  "CustomerUniqueID" text NOT NULL,
  "CustomerZipCodePrefix" integer NOT NULL,
  "CustomerCity" text NOT NULL,
  "CustomerState" text NOT NULL,
  "RowEffectiveDate" timestamp with time zone NOT NULL,
  "RowExpirationDate" timestamp with time zone,
  "CurrentFlag" boolean NOT NULL,
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$')
);
CREATE INDEX IF NOT EXISTS ix_staging_dim_customer_attempt ON staging.dim_customer ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.dim_product (
  "LoadAttemptID" text NOT NULL,
  "ProductID" text NOT NULL,
  "ProductCategoryName" text NOT NULL,
  "ProductCategoryNameEnglish" text NOT NULL,
  "ProductWeightG" numeric(38,18),
  "ProductLengthCm" numeric(38,18),
  "ProductHeightCm" numeric(38,18),
  "ProductWidthCm" numeric(38,18),
  "ProductVolumeCm3" numeric(38,18),
  "SizeCategory" text NOT NULL,
  "RowEffectiveDate" timestamp with time zone NOT NULL,
  "RowExpirationDate" timestamp with time zone,
  "CurrentFlag" boolean NOT NULL,
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$')
);
CREATE INDEX IF NOT EXISTS ix_staging_dim_product_attempt ON staging.dim_product ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.dim_seller (
  "LoadAttemptID" text NOT NULL,
  "SellerID" text NOT NULL,
  "SellerZipCodePrefix" integer NOT NULL,
  "SellerCity" text NOT NULL,
  "SellerState" text NOT NULL,
  "RowEffectiveDate" timestamp with time zone NOT NULL,
  "RowExpirationDate" timestamp with time zone,
  "CurrentFlag" boolean NOT NULL,
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$')
);
CREATE INDEX IF NOT EXISTS ix_staging_dim_seller_attempt ON staging.dim_seller ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.dim_geography (
  "LoadAttemptID" text NOT NULL,
  "ZipCodePrefix" integer NOT NULL,
  "City" text NOT NULL,
  "State" text NOT NULL,
  "Latitude" numeric(38,18) NOT NULL,
  "Longitude" numeric(38,18) NOT NULL,
  "Region" text,
  "RowEffectiveDate" timestamp with time zone NOT NULL,
  "RowExpirationDate" timestamp with time zone,
  "CurrentFlag" boolean NOT NULL,
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$')
);
CREATE INDEX IF NOT EXISTS ix_staging_dim_geography_attempt ON staging.dim_geography ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.dim_date (
  "LoadAttemptID" text NOT NULL,
  "DateKey" integer NOT NULL,
  "DateID" integer NOT NULL,
  "Date" date NOT NULL,
  "Year" integer NOT NULL,
  "Month" integer NOT NULL,
  "Day" integer NOT NULL,
  "Quarter" integer NOT NULL,
  "WeekOfYear" integer NOT NULL,
  "DayOfWeek" integer NOT NULL,
  "IsWeekend" boolean NOT NULL,
  "MonthName" text NOT NULL,
  "DayName" text NOT NULL,
  "FiscalYear" integer NOT NULL,
  "FiscalQuarter" integer NOT NULL,
  "Holiday" text,
  "IsHoliday" boolean NOT NULL,
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$')
);
CREATE INDEX IF NOT EXISTS ix_staging_dim_date_attempt ON staging.dim_date ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.dim_order_status (
  "LoadAttemptID" text NOT NULL,
  "StatusID" text NOT NULL,
  "StatusDescription" text NOT NULL,
  "IsApproved" boolean NOT NULL,
  "IsDelivered" boolean NOT NULL,
  "IsShipped" boolean NOT NULL,
  "IsCancelled" boolean NOT NULL,
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$')
);
CREATE INDEX IF NOT EXISTS ix_staging_dim_order_status_attempt ON staging.dim_order_status ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.fact_sales (
  "LoadAttemptID" text NOT NULL,
  "OrderID" text NOT NULL,
  "OrderItemID" integer NOT NULL,
  "CustomerID" text NOT NULL,
  "ProductID" text NOT NULL,
  "SellerID" text NOT NULL,
  "DateKey" integer NOT NULL,
  "StatusID" text NOT NULL,
  "ZipCodePrefix" integer NOT NULL,
  "OrderPurchaseTimestamp" timestamp with time zone NOT NULL,
  "OrderDeliveredCustomerDate" timestamp with time zone,
  "Price" numeric(38,18) NOT NULL,
  "FreightValue" numeric(38,18) NOT NULL,
  "TotalItemValue" numeric(38,18) NOT NULL,
  "ShippingDays" integer,
  "DeliveryDays" integer,
  "TotalDays" integer,
  "IsDelayed" boolean NOT NULL,
  "DelayDays" integer NOT NULL,
  "IsCrossState" boolean NOT NULL,
  "PaymentType" text,
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$')
);
CREATE INDEX IF NOT EXISTS ix_staging_fact_sales_attempt ON staging.fact_sales ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.fact_reviews (
  "LoadAttemptID" text NOT NULL,
  "OrderID" text NOT NULL,
  "ReviewID" text NOT NULL,
  "CustomerID" text NOT NULL,
  "DateKey" integer NOT NULL,
  "ReviewScore" integer NOT NULL,
  "ReviewCommentMessage" text,
  "ReviewCreationDate" timestamp with time zone NOT NULL,
  "ReviewAnswerTimestamp" timestamp with time zone NOT NULL,
  "ReviewResponseDays" integer NOT NULL,
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$')
);
CREATE INDEX IF NOT EXISTS ix_staging_fact_reviews_attempt ON staging.fact_reviews ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.sales_by_state (
  "LoadAttemptID" text NOT NULL,
  "CustomerState" text NOT NULL,
  "OrdersCount" integer NOT NULL,
  "UniqueCustomers" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgItemPrice" numeric(38,18) NOT NULL,
  "AvgDeliveryTime" numeric(38,18),
  "LastUpdated" timestamp with time zone NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_staging_sales_by_state_attempt ON staging.sales_by_state ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.sales_by_category (
  "LoadAttemptID" text NOT NULL,
  "ProductCategoryNameEnglish" text NOT NULL,
  "OrdersCount" integer NOT NULL,
  "UniqueCustomers" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgItemPrice" numeric(38,18) NOT NULL,
  "AvgDeliveryTime" numeric(38,18),
  "DelayedOrders" integer NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_staging_sales_by_category_attempt ON staging.sales_by_category ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.monthly_sales (
  "LoadAttemptID" text NOT NULL,
  "Year" integer NOT NULL,
  "Month" integer NOT NULL,
  "OrdersCount" integer NOT NULL,
  "UniqueCustomers" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgItemPrice" numeric(38,18) NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_staging_monthly_sales_attempt ON staging.monthly_sales ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.order_status (
  "LoadAttemptID" text NOT NULL,
  "ProductCategoryNameEnglish" text NOT NULL,
  "OrderStatus" text NOT NULL,
  "OrdersCount" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "UniqueCustomers" integer NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_staging_order_status_attempt ON staging.order_status ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.cross_state_analysis (
  "LoadAttemptID" text NOT NULL,
  "ProductCategoryNameEnglish" text NOT NULL,
  "IsCrossState" boolean NOT NULL,
  "OrdersCount" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgDeliveryTime" numeric(38,18),
  "DelayRate" numeric(38,18) NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_staging_cross_state_analysis_attempt ON staging.cross_state_analysis ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.seller_performance (
  "LoadAttemptID" text NOT NULL,
  "SellerID" text NOT NULL,
  "SellerState" text NOT NULL,
  "OrdersCount" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgShippingCost" numeric(38,18) NOT NULL,
  "AvgDeliveryTime" numeric(38,18),
  "DelayedOrders" integer NOT NULL,
  "DelayRate" numeric(38,18) NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_staging_seller_performance_attempt ON staging.seller_performance ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.size_analysis (
  "LoadAttemptID" text NOT NULL,
  "ProductCategoryNameEnglish" text NOT NULL,
  "SizeCategory" text NOT NULL,
  "OrdersCount" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgShippingCost" numeric(38,18) NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_staging_size_analysis_attempt ON staging.size_analysis ("LoadAttemptID");

CREATE UNLOGGED TABLE IF NOT EXISTS staging.payment_methods (
  "LoadAttemptID" text NOT NULL,
  "PaymentType" text NOT NULL,
  "OrdersCount" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgOrderValue" numeric(38,18) NOT NULL,
  "UniqueCustomers" integer NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_staging_payment_methods_attempt ON staging.payment_methods ("LoadAttemptID");
