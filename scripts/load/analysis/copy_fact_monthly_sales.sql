COPY INTO ecom.FactMonthlySales (
    Year,
    Month,
    OrdersCount,
    UniqueCustomers,
    TotalSales,
    AvgItemPrice,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/monthly_sales/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
