-- Attempt-isolated Redshift staging. COPY transactions are intentionally separate from publication.
CREATE TABLE IF NOT EXISTS staging.dim_customer (
  "CustomerID" VARCHAR(64), "CustomerUniqueID" VARCHAR(64), "CustomerZipCodePrefix" INTEGER,
  "CustomerCity" VARCHAR(256), "CustomerState" VARCHAR(8), "RowEffectiveDate" TIMESTAMP,
  "RowExpirationDate" TIMESTAMP, "CurrentFlag" BOOLEAN, "RecordHash" VARCHAR(64), "SCD2TrackedHash" VARCHAR(64) NOT NULL,
  "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL,
  "WarehouseEffectiveAt" TIMESTAMP NOT NULL
) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.dim_product (
  "ProductID" VARCHAR(64), "ProductCategoryName" VARCHAR(256), "ProductCategoryNameEnglish" VARCHAR(256),
  "ProductWeightG" DECIMAL(38,18), "ProductLengthCm" DECIMAL(38,18), "ProductHeightCm" DECIMAL(38,18),
  "ProductWidthCm" DECIMAL(38,18), "ProductVolumeCm3" DECIMAL(38,18), "SizeCategory" VARCHAR(32),
  "RowEffectiveDate" TIMESTAMP, "RowExpirationDate" TIMESTAMP, "CurrentFlag" BOOLEAN,
  "RecordHash" VARCHAR(64), "SCD2TrackedHash" VARCHAR(64) NOT NULL, "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL,
  "WarehouseEffectiveAt" TIMESTAMP NOT NULL
) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.dim_seller (
  "SellerID" VARCHAR(64), "SellerZipCodePrefix" INTEGER, "SellerCity" VARCHAR(256), "SellerState" VARCHAR(8),
  "RowEffectiveDate" TIMESTAMP, "RowExpirationDate" TIMESTAMP, "CurrentFlag" BOOLEAN,
  "RecordHash" VARCHAR(64), "SCD2TrackedHash" VARCHAR(64) NOT NULL, "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL,
  "WarehouseEffectiveAt" TIMESTAMP NOT NULL
) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.dim_geography (
  "ZipCodePrefix" INTEGER, "City" VARCHAR(256), "State" VARCHAR(8), "Latitude" DECIMAL(38,18),
  "Longitude" DECIMAL(38,18), "Region" VARCHAR(32), "RowEffectiveDate" TIMESTAMP,
  "RowExpirationDate" TIMESTAMP, "CurrentFlag" BOOLEAN, "RecordHash" VARCHAR(64), "SCD2TrackedHash" VARCHAR(64) NOT NULL,
  "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL,
  "WarehouseEffectiveAt" TIMESTAMP NOT NULL
) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.dim_date (
  "DateKey" INTEGER, "DateID" INTEGER, "Date" DATE, "Year" INTEGER, "Month" INTEGER, "Day" INTEGER,
  "Quarter" INTEGER, "WeekOfYear" INTEGER, "DayOfWeek" INTEGER, "IsWeekend" BOOLEAN,
  "MonthName" VARCHAR(32), "DayName" VARCHAR(32), "FiscalYear" INTEGER, "FiscalQuarter" INTEGER,
  "Holiday" VARCHAR(128), "IsHoliday" BOOLEAN, "RecordHash" VARCHAR(64),
  "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL,
  "WarehouseEffectiveAt" TIMESTAMP NOT NULL
) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.dim_order_status (
  "StatusID" VARCHAR(64), "StatusDescription" VARCHAR(256), "IsApproved" BOOLEAN, "IsDelivered" BOOLEAN,
  "IsShipped" BOOLEAN, "IsCancelled" BOOLEAN, "RecordHash" VARCHAR(64),
  "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL,
  "WarehouseEffectiveAt" TIMESTAMP NOT NULL
) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.fact_sales (
  "OrderID" VARCHAR(64), "OrderItemID" INTEGER, "CustomerID" VARCHAR(64), "ProductID" VARCHAR(64),
  "SellerID" VARCHAR(64), "DateKey" INTEGER, "StatusID" VARCHAR(64), "ZipCodePrefix" INTEGER,
  "OrderPurchaseTimestamp" TIMESTAMP, "OrderDeliveredCustomerDate" TIMESTAMP,
  "Price" DECIMAL(38,18), "FreightValue" DECIMAL(38,18), "TotalItemValue" DECIMAL(38,18),
  "ShippingDays" INTEGER, "DeliveryDays" INTEGER, "TotalDays" INTEGER, "IsDelayed" BOOLEAN,
  "DelayDays" INTEGER, "IsCrossState" BOOLEAN, "PaymentType" VARCHAR(64), "RecordHash" VARCHAR(64),
  "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL,
  "WarehouseEffectiveAt" TIMESTAMP NOT NULL
) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.fact_reviews (
  "OrderID" VARCHAR(64), "ReviewID" VARCHAR(64), "CustomerID" VARCHAR(64), "DateKey" INTEGER,
  "ReviewScore" INTEGER, "ReviewCommentMessage" VARCHAR(65535), "ReviewCreationDate" TIMESTAMP,
  "ReviewAnswerTimestamp" TIMESTAMP, "ReviewResponseDays" INTEGER, "RecordHash" VARCHAR(64),
  "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL,
  "WarehouseEffectiveAt" TIMESTAMP NOT NULL
) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.sales_by_state ("CustomerState" VARCHAR(8), "OrdersCount" INTEGER, "UniqueCustomers" INTEGER, "TotalSales" DECIMAL(38,18), "AvgItemPrice" DECIMAL(38,18), "AvgDeliveryTime" DECIMAL(38,18), "LastUpdated" TIMESTAMP, "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL, "WarehouseEffectiveAt" TIMESTAMP NOT NULL) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.sales_by_category ("ProductCategoryNameEnglish" VARCHAR(256), "OrdersCount" INTEGER, "UniqueCustomers" INTEGER, "TotalSales" DECIMAL(38,18), "AvgItemPrice" DECIMAL(38,18), "AvgDeliveryTime" DECIMAL(38,18), "DelayedOrders" INTEGER, "LastUpdated" TIMESTAMP, "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL, "WarehouseEffectiveAt" TIMESTAMP NOT NULL) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.monthly_sales ("Year" INTEGER, "Month" INTEGER, "OrdersCount" INTEGER, "UniqueCustomers" INTEGER, "TotalSales" DECIMAL(38,18), "AvgItemPrice" DECIMAL(38,18), "LastUpdated" TIMESTAMP, "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL, "WarehouseEffectiveAt" TIMESTAMP NOT NULL) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.order_status ("ProductCategoryNameEnglish" VARCHAR(256), "OrderStatus" VARCHAR(64), "OrdersCount" INTEGER, "TotalSales" DECIMAL(38,18), "UniqueCustomers" INTEGER, "LastUpdated" TIMESTAMP, "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL, "WarehouseEffectiveAt" TIMESTAMP NOT NULL) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.cross_state_analysis ("ProductCategoryNameEnglish" VARCHAR(256), "IsCrossState" BOOLEAN, "OrdersCount" INTEGER, "TotalSales" DECIMAL(38,18), "AvgDeliveryTime" DECIMAL(38,18), "DelayRate" DECIMAL(38,18), "LastUpdated" TIMESTAMP, "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL, "WarehouseEffectiveAt" TIMESTAMP NOT NULL) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.seller_performance ("SellerID" VARCHAR(64), "SellerState" VARCHAR(8), "OrdersCount" INTEGER, "TotalSales" DECIMAL(38,18), "AvgShippingCost" DECIMAL(38,18), "AvgDeliveryTime" DECIMAL(38,18), "DelayedOrders" INTEGER, "DelayRate" DECIMAL(38,18), "LastUpdated" TIMESTAMP, "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL, "WarehouseEffectiveAt" TIMESTAMP NOT NULL) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.size_analysis ("ProductCategoryNameEnglish" VARCHAR(256), "SizeCategory" VARCHAR(32), "OrdersCount" INTEGER, "TotalSales" DECIMAL(38,18), "AvgShippingCost" DECIMAL(38,18), "LastUpdated" TIMESTAMP, "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL, "WarehouseEffectiveAt" TIMESTAMP NOT NULL) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");
CREATE TABLE IF NOT EXISTS staging.payment_methods ("PaymentType" VARCHAR(64), "OrdersCount" INTEGER, "TotalSales" DECIMAL(38,18), "AvgOrderValue" DECIMAL(38,18), "UniqueCustomers" INTEGER, "LastUpdated" TIMESTAMP, "LoadAttemptID" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL, "WarehouseEffectiveAt" TIMESTAMP NOT NULL) DISTSTYLE EVEN SORTKEY ("LoadAttemptID");

-- Attempt metadata is loaded separately from the 16 curated datasets and is consumed by publication.
CREATE TABLE IF NOT EXISTS staging.publication_datasets (
  "LoadAttemptID" VARCHAR(64) NOT NULL, "BatchID" VARCHAR(256) NOT NULL,
  "PublicationFingerprint" VARCHAR(64) NOT NULL,
  "DatasetOrdinal" INTEGER NOT NULL, "DatasetName" VARCHAR(64) NOT NULL,
  "DatasetSHA256" VARCHAR(64) NOT NULL, "RowCount" BIGINT NOT NULL
) DISTSTYLE EVEN SORTKEY ("LoadAttemptID", "DatasetOrdinal");
