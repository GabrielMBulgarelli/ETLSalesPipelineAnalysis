COPY INTO ecom.FactCrossStateAnalysis (
    CategoryKey,
    IsCrossState,
    OrdersCount,
    TotalSales,
    AvgDeliveryTime,
    DelayRate,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/cross_state_analysis/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
