COPY INTO ecom.DimGeography (
    ZipCodePrefix,
    City,
    State,
    Latitude,
    Longitude,
    Region,
    RowEffectiveDate,
    RowExpirationDate,
    CurrentFlag
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/dimensions/dim_geography/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
