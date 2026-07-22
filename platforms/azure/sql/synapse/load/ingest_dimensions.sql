-- Copy DimCustomer
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

-- Copy DimDate
COPY INTO ecom.DimDate (
    DateKey,
    DateID,
    Date,
    Year,
    Month,
    Day,
    Quarter,
    WeekOfYear,
    DayOfWeek,
    IsWeekend,
    MonthName,
    DayName,
    FiscalYear,
    FiscalQuarter,
    Holiday,
    IsHoliday
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/dimensions/dim_date/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);

-- Copy DimGeography
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

-- Copy DimProduct
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

-- Copy DimSeller
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