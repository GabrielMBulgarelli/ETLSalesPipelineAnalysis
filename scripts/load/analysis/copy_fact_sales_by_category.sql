COPY INTO ecom.FactSalesByCategory (
    CategoryKey,
    OrdersCount,
    UniqueCustomers,
    TotalSales,
    AvgItemPrice,
    AvgDeliveryTime,
    DelayedOrders,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/sales_by_category/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
