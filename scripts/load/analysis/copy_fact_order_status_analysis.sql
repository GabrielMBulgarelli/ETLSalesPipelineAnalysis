COPY INTO ecom.FactOrderStatusAnalysis (
    ProductCategoryNameEnglish,
    OrderStatus,
    OrdersCount,
    TotalSales,
    UniqueCustomers,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/order_status/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
