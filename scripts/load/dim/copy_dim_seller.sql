COPY INTO ecom.DimSeller (
    SellerID,
    SellerZipCodePrefix,
    SellerCity,
    SellerState,
    RowEffectiveDate,
    RowExpirationDate,
    CurrentFlag
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/dimensions/dim_seller/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
