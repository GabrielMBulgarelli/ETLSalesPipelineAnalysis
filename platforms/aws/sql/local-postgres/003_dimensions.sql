CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
  "CustomerKey" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "CustomerID" text NOT NULL,
  "CustomerUniqueID" text NOT NULL,
  "CustomerZipCodePrefix" integer NOT NULL,
  "CustomerCity" text NOT NULL,
  "CustomerState" text NOT NULL,
  "RowEffectiveDate" timestamp with time zone NOT NULL,
  "RowExpirationDate" timestamp with time zone,
  "CurrentFlag" boolean NOT NULL,
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$'),
  "SourceBatchID" text NOT NULL,
  UNIQUE ("CustomerID")
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
  "ProductKey" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$'),
  "SourceBatchID" text NOT NULL,
  UNIQUE ("ProductID")
);

CREATE TABLE IF NOT EXISTS warehouse.dim_seller (
  "SellerKey" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "SellerID" text NOT NULL,
  "SellerZipCodePrefix" integer NOT NULL,
  "SellerCity" text NOT NULL,
  "SellerState" text NOT NULL,
  "RowEffectiveDate" timestamp with time zone NOT NULL,
  "RowExpirationDate" timestamp with time zone,
  "CurrentFlag" boolean NOT NULL,
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$'),
  "SourceBatchID" text NOT NULL,
  UNIQUE ("SellerID")
);

CREATE TABLE IF NOT EXISTS warehouse.dim_geography (
  "GeographyKey" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "ZipCodePrefix" integer NOT NULL,
  "City" text NOT NULL,
  "State" text NOT NULL,
  "Latitude" numeric(38,18) NOT NULL,
  "Longitude" numeric(38,18) NOT NULL,
  "Region" text,
  "RowEffectiveDate" timestamp with time zone NOT NULL,
  "RowExpirationDate" timestamp with time zone,
  "CurrentFlag" boolean NOT NULL,
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$'),
  "SourceBatchID" text NOT NULL,
  UNIQUE ("ZipCodePrefix")
);

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
  "DateSurrogateKey" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$'),
  "SourceBatchID" text NOT NULL,
  UNIQUE ("DateKey")
);

CREATE TABLE IF NOT EXISTS warehouse.dim_order_status (
  "OrderStatusKey" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "StatusID" text NOT NULL,
  "StatusDescription" text NOT NULL,
  "IsApproved" boolean NOT NULL,
  "IsDelivered" boolean NOT NULL,
  "IsShipped" boolean NOT NULL,
  "IsCancelled" boolean NOT NULL,
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$'),
  "SourceBatchID" text NOT NULL,
  UNIQUE ("StatusID")
);
