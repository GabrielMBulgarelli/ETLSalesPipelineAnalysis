COPY INTO ecom.FactSellerPerformance (
    SellerID,
    SellerState,
    OrdersCount,
    TotalSales,
    AvgShippingCost,
    AvgDeliveryTime,
    DelayedOrders,
    DelayRate,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/seller_performance/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
