COPY INTO ecom.DimCustomer (
    CustomerID,
    CustomerUniqueID,
    CustomerZipCodePrefix,
    CustomerCity,
    CustomerState,
    RowEffectiveDate,
    RowExpirationDate,
    CurrentFlag
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/dimensions/dim_customer/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
