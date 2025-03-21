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
