COPY INTO ecom.DimProduct (
    ProductID,
    ProductCategoryName,
    ProductCategoryNameEnglish,
    ProductWeightG,
    ProductLengthCm,
    ProductHeightCm,
    ProductWidthCm,
    ProductVolumeCm3,
    SizeCategory,
    RowEffectiveDate,
    RowExpirationDate,
    CurrentFlag
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/dimensions/dim_product/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
