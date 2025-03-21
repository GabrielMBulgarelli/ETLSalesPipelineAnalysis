COPY INTO ecom.FactSalesByState (
    CustomerState,
    OrdersCount,
    UniqueCustomers,
    TotalSales,
    AvgItemPrice,
    AvgDeliveryTime,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/sales_by_state/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
