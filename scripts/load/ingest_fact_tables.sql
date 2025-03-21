-- Fact: Sales
COPY INTO ecom.FactSales (
    OrderID,
    OrderItemID,
    CustomerID,
    ProductID,
    SellerID,
    ShippingLimitDate,
    OrderPurchaseTimestamp,
    OrderApprovedAt,
    OrderDeliveredCarrierDate,
    OrderDeliveredCustomerDate,
    OrderEstimatedDeliveryDate,
    ShippingCost,
    Price,
    PaymentTypeKey,
    PaymentValue,
    PaymentInstallments,
    ProductVolume,
    OrderStatus,
    OrderPurchaseDateKey,
    OrderDeliveredDateKey,
    DaysToPrepareDelivery,
    DeliveryTimeInDays,
    DaysDelayed,
    IsDelayed
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/facts/fact_sales/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);

-- Fact: Reviews
COPY INTO ecom.FactReviews (
    OrderID,
    ReviewID,
    CustomerID,
    DateKey,
    ReviewScore,
    ReviewCommentMessage,
    ReviewCreationDate,
    ReviewAnswerTimestamp,
    ReviewResponseDays
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/facts/fact_reviews/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);