CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
  "OrderID" VARCHAR(64) NOT NULL, "OrderItemID" INTEGER NOT NULL, "CustomerID" VARCHAR(64) NOT NULL,
  "ProductID" VARCHAR(64) NOT NULL, "SellerID" VARCHAR(64) NOT NULL, "DateKey" INTEGER NOT NULL,
  "StatusID" VARCHAR(64) NOT NULL, "ZipCodePrefix" INTEGER NOT NULL, "CustomerKey" BIGINT NOT NULL,
  "ProductKey" BIGINT NOT NULL, "SellerKey" BIGINT NOT NULL, "CustomerGeographyKey" BIGINT NOT NULL,
  "SellerGeographyKey" BIGINT NOT NULL, "DateSurrogateKey" BIGINT NOT NULL, "OrderStatusKey" BIGINT NOT NULL,
  "OrderPurchaseTimestamp" TIMESTAMP NOT NULL, "OrderDeliveredCustomerDate" TIMESTAMP,
  "Price" DECIMAL(38,18), "FreightValue" DECIMAL(38,18), "TotalItemValue" DECIMAL(38,18),
  "ShippingDays" INTEGER, "DeliveryDays" INTEGER, "TotalDays" INTEGER, "IsDelayed" BOOLEAN,
  "DelayDays" INTEGER, "IsCrossState" BOOLEAN, "PaymentType" VARCHAR(64), "RecordHash" VARCHAR(64) NOT NULL,
  "SourceBatchID" VARCHAR(256) NOT NULL, "PublishedAt" TIMESTAMP NOT NULL,
  PRIMARY KEY ("OrderID", "OrderItemID"), FOREIGN KEY ("CustomerKey") REFERENCES warehouse.dim_customer("CustomerKey")
) DISTSTYLE KEY DISTKEY ("CustomerKey") COMPOUND SORTKEY ("OrderPurchaseTimestamp", "OrderID", "OrderItemID");
CREATE TABLE IF NOT EXISTS warehouse.fact_reviews (
  "OrderID" VARCHAR(64) NOT NULL, "ReviewID" VARCHAR(64) NOT NULL, "CustomerID" VARCHAR(64) NOT NULL,
  "DateKey" INTEGER NOT NULL, "CustomerKey" BIGINT NOT NULL, "DateSurrogateKey" BIGINT NOT NULL,
  "ReviewScore" INTEGER, "ReviewCommentMessage" VARCHAR(65535), "ReviewCreationDate" TIMESTAMP NOT NULL,
  "ReviewAnswerTimestamp" TIMESTAMP, "ReviewResponseDays" INTEGER, "RecordHash" VARCHAR(64) NOT NULL,
  "SourceBatchID" VARCHAR(256) NOT NULL, "PublishedAt" TIMESTAMP NOT NULL,
  PRIMARY KEY ("OrderID", "ReviewID"), FOREIGN KEY ("CustomerKey") REFERENCES warehouse.dim_customer("CustomerKey")
) DISTSTYLE KEY DISTKEY ("CustomerKey") COMPOUND SORTKEY ("ReviewCreationDate", "OrderID", "ReviewID");
