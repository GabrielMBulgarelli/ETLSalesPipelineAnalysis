-- Historical warehouse dimensions. Constraints are informational and follow explicit validation.
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
  "CustomerKey" BIGINT NOT NULL, "CustomerID" VARCHAR(64) NOT NULL, "CustomerUniqueID" VARCHAR(64),
  "CustomerZipCodePrefix" INTEGER, "CustomerCity" VARCHAR(256), "CustomerState" VARCHAR(8),
  "RecordHash" VARCHAR(64) NOT NULL, "SCD2TrackedHash" VARCHAR(64) NOT NULL,
  "WarehouseEffectiveAt" TIMESTAMP NOT NULL, "WarehouseExpirationAt" TIMESTAMP, "CurrentFlag" BOOLEAN NOT NULL,
  "SourceBatchID" VARCHAR(256) NOT NULL, "CreatedAt" TIMESTAMP NOT NULL, PRIMARY KEY ("CustomerKey")
) DISTSTYLE AUTO SORTKEY ("CustomerID", "WarehouseEffectiveAt");
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
  "ProductKey" BIGINT NOT NULL, "ProductID" VARCHAR(64) NOT NULL, "ProductCategoryName" VARCHAR(256),
  "ProductCategoryNameEnglish" VARCHAR(256), "ProductWeightG" DECIMAL(38,18), "ProductLengthCm" DECIMAL(38,18),
  "ProductHeightCm" DECIMAL(38,18), "ProductWidthCm" DECIMAL(38,18), "ProductVolumeCm3" DECIMAL(38,18),
  "SizeCategory" VARCHAR(32), "RecordHash" VARCHAR(64) NOT NULL, "SCD2TrackedHash" VARCHAR(64) NOT NULL,
  "WarehouseEffectiveAt" TIMESTAMP NOT NULL, "WarehouseExpirationAt" TIMESTAMP, "CurrentFlag" BOOLEAN NOT NULL,
  "SourceBatchID" VARCHAR(256) NOT NULL, "CreatedAt" TIMESTAMP NOT NULL, PRIMARY KEY ("ProductKey")
) DISTSTYLE AUTO SORTKEY ("ProductID", "WarehouseEffectiveAt");
CREATE TABLE IF NOT EXISTS warehouse.dim_seller (
  "SellerKey" BIGINT NOT NULL, "SellerID" VARCHAR(64) NOT NULL, "SellerZipCodePrefix" INTEGER,
  "SellerCity" VARCHAR(256), "SellerState" VARCHAR(8), "RecordHash" VARCHAR(64) NOT NULL,
  "SCD2TrackedHash" VARCHAR(64) NOT NULL, "WarehouseEffectiveAt" TIMESTAMP NOT NULL,
  "WarehouseExpirationAt" TIMESTAMP, "CurrentFlag" BOOLEAN NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL,
  "CreatedAt" TIMESTAMP NOT NULL, PRIMARY KEY ("SellerKey")
) DISTSTYLE AUTO SORTKEY ("SellerID", "WarehouseEffectiveAt");
CREATE TABLE IF NOT EXISTS warehouse.dim_geography (
  "GeographyKey" BIGINT NOT NULL, "ZipCodePrefix" INTEGER NOT NULL, "City" VARCHAR(256), "State" VARCHAR(8),
  "Latitude" DECIMAL(38,18), "Longitude" DECIMAL(38,18), "Region" VARCHAR(32), "RecordHash" VARCHAR(64) NOT NULL,
  "SCD2TrackedHash" VARCHAR(64) NOT NULL, "WarehouseEffectiveAt" TIMESTAMP NOT NULL,
  "WarehouseExpirationAt" TIMESTAMP, "CurrentFlag" BOOLEAN NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL,
  "CreatedAt" TIMESTAMP NOT NULL, PRIMARY KEY ("GeographyKey")
) DISTSTYLE AUTO SORTKEY ("ZipCodePrefix", "WarehouseEffectiveAt");
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
  "DateSurrogateKey" BIGINT NOT NULL, "DateKey" INTEGER NOT NULL, "DateID" INTEGER, "Date" DATE,
  "Year" INTEGER, "Month" INTEGER, "Day" INTEGER, "Quarter" INTEGER, "WeekOfYear" INTEGER,
  "DayOfWeek" INTEGER, "IsWeekend" BOOLEAN, "MonthName" VARCHAR(32), "DayName" VARCHAR(32),
  "FiscalYear" INTEGER, "FiscalQuarter" INTEGER, "Holiday" VARCHAR(128), "IsHoliday" BOOLEAN,
  "RecordHash" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL, "UpdatedAt" TIMESTAMP NOT NULL,
  PRIMARY KEY ("DateSurrogateKey")
) DISTSTYLE AUTO SORTKEY ("DateKey");
CREATE TABLE IF NOT EXISTS warehouse.dim_order_status (
  "OrderStatusKey" BIGINT NOT NULL, "StatusID" VARCHAR(64) NOT NULL, "StatusDescription" VARCHAR(256),
  "IsApproved" BOOLEAN, "IsDelivered" BOOLEAN, "IsShipped" BOOLEAN, "IsCancelled" BOOLEAN,
  "RecordHash" VARCHAR(64) NOT NULL, "SourceBatchID" VARCHAR(256) NOT NULL, "UpdatedAt" TIMESTAMP NOT NULL,
  PRIMARY KEY ("OrderStatusKey")
) DISTSTYLE AUTO SORTKEY ("StatusID");
