COPY INTO ecom.FactPaymentAnalysis (
    PaymentTypeKey,
    OrdersCount,
    TotalSales,
    AvgOrderValue,
    UniqueCustomers,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/payment_methods/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
