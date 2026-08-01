CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
  "CustomerKey" bigint NOT NULL REFERENCES warehouse.dim_customer ("CustomerKey"),
  "ProductKey" bigint NOT NULL REFERENCES warehouse.dim_product ("ProductKey"),
  "SellerKey" bigint NOT NULL REFERENCES warehouse.dim_seller ("SellerKey"),
  "DateSurrogateKey" bigint NOT NULL REFERENCES warehouse.dim_date ("DateSurrogateKey"),
  "OrderStatusKey" bigint NOT NULL REFERENCES warehouse.dim_order_status ("OrderStatusKey"),
  "GeographyKey" bigint NOT NULL REFERENCES warehouse.dim_geography ("GeographyKey"),
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
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$'),
  "SourceBatchID" text NOT NULL,
  PRIMARY KEY ("OrderID", "OrderItemID")
);
CREATE INDEX IF NOT EXISTS ix_fact_sales_customerkey ON warehouse.fact_sales ("CustomerKey");
CREATE INDEX IF NOT EXISTS ix_fact_sales_productkey ON warehouse.fact_sales ("ProductKey");
CREATE INDEX IF NOT EXISTS ix_fact_sales_sellerkey ON warehouse.fact_sales ("SellerKey");
CREATE INDEX IF NOT EXISTS ix_fact_sales_datesurrogatekey ON warehouse.fact_sales ("DateSurrogateKey");
CREATE INDEX IF NOT EXISTS ix_fact_sales_orderstatuskey ON warehouse.fact_sales ("OrderStatusKey");
CREATE INDEX IF NOT EXISTS ix_fact_sales_geographykey ON warehouse.fact_sales ("GeographyKey");

CREATE TABLE IF NOT EXISTS warehouse.fact_reviews (
  "CustomerKey" bigint NOT NULL REFERENCES warehouse.dim_customer ("CustomerKey"),
  "DateSurrogateKey" bigint NOT NULL REFERENCES warehouse.dim_date ("DateSurrogateKey"),
  "OrderID" text NOT NULL,
  "ReviewID" text NOT NULL,
  "CustomerID" text NOT NULL,
  "DateKey" integer NOT NULL,
  "ReviewScore" integer NOT NULL,
  "ReviewCommentMessage" text,
  "ReviewCreationDate" timestamp with time zone NOT NULL,
  "ReviewAnswerTimestamp" timestamp with time zone NOT NULL,
  "ReviewResponseDays" integer NOT NULL,
  "RecordHash" text NOT NULL CHECK ("RecordHash" ~ '^[0-9a-f]{64}$'),
  "SourceBatchID" text NOT NULL,
  PRIMARY KEY ("OrderID", "ReviewID")
);
CREATE INDEX IF NOT EXISTS ix_fact_reviews_customerkey ON warehouse.fact_reviews ("CustomerKey");
CREATE INDEX IF NOT EXISTS ix_fact_reviews_datesurrogatekey ON warehouse.fact_reviews ("DateSurrogateKey");
