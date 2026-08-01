CREATE TABLE IF NOT EXISTS analytics.sales_by_state (
  "CustomerState" text NOT NULL,
  "OrdersCount" integer NOT NULL,
  "UniqueCustomers" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgItemPrice" numeric(38,18) NOT NULL,
  "AvgDeliveryTime" numeric(38,18),
  "LastUpdated" timestamp with time zone NOT NULL,
  "SourceBatchID" text NOT NULL,
  PRIMARY KEY ("CustomerState")
);

CREATE TABLE IF NOT EXISTS analytics.sales_by_category (
  "ProductCategoryNameEnglish" text NOT NULL,
  "OrdersCount" integer NOT NULL,
  "UniqueCustomers" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgItemPrice" numeric(38,18) NOT NULL,
  "AvgDeliveryTime" numeric(38,18),
  "DelayedOrders" integer NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL,
  "SourceBatchID" text NOT NULL,
  PRIMARY KEY ("ProductCategoryNameEnglish")
);

CREATE TABLE IF NOT EXISTS analytics.monthly_sales (
  "Year" integer NOT NULL,
  "Month" integer NOT NULL,
  "OrdersCount" integer NOT NULL,
  "UniqueCustomers" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgItemPrice" numeric(38,18) NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL,
  "SourceBatchID" text NOT NULL,
  PRIMARY KEY ("Year", "Month")
);

CREATE TABLE IF NOT EXISTS analytics.order_status (
  "ProductCategoryNameEnglish" text NOT NULL,
  "OrderStatus" text NOT NULL,
  "OrdersCount" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "UniqueCustomers" integer NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL,
  "SourceBatchID" text NOT NULL,
  PRIMARY KEY ("ProductCategoryNameEnglish", "OrderStatus")
);

CREATE TABLE IF NOT EXISTS analytics.cross_state_analysis (
  "ProductCategoryNameEnglish" text NOT NULL,
  "IsCrossState" boolean NOT NULL,
  "OrdersCount" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgDeliveryTime" numeric(38,18),
  "DelayRate" numeric(38,18) NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL,
  "SourceBatchID" text NOT NULL,
  PRIMARY KEY ("ProductCategoryNameEnglish", "IsCrossState")
);

CREATE TABLE IF NOT EXISTS analytics.seller_performance (
  "SellerID" text NOT NULL,
  "SellerState" text NOT NULL,
  "OrdersCount" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgShippingCost" numeric(38,18) NOT NULL,
  "AvgDeliveryTime" numeric(38,18),
  "DelayedOrders" integer NOT NULL,
  "DelayRate" numeric(38,18) NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL,
  "SourceBatchID" text NOT NULL,
  PRIMARY KEY ("SellerID")
);

CREATE TABLE IF NOT EXISTS analytics.size_analysis (
  "ProductCategoryNameEnglish" text NOT NULL,
  "SizeCategory" text NOT NULL,
  "OrdersCount" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgShippingCost" numeric(38,18) NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL,
  "SourceBatchID" text NOT NULL,
  PRIMARY KEY ("ProductCategoryNameEnglish", "SizeCategory")
);

CREATE TABLE IF NOT EXISTS analytics.payment_methods (
  "PaymentType" text NOT NULL,
  "OrdersCount" integer NOT NULL,
  "TotalSales" numeric(38,18) NOT NULL,
  "AvgOrderValue" numeric(38,18) NOT NULL,
  "UniqueCustomers" integer NOT NULL,
  "LastUpdated" timestamp with time zone NOT NULL,
  "SourceBatchID" text NOT NULL,
  PRIMARY KEY ("PaymentType")
);
