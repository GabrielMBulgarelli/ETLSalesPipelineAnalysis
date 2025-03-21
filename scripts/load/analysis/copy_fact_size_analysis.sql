COPY INTO ecom.FactSizeAnalysis (
    CategoryKey,
    SizeCategory,
    OrdersCount,
    TotalSales,
    AvgShippingCost,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/size_analysis/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
