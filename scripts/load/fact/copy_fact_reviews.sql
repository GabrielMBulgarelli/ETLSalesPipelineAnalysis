COPY INTO ecom.FactReviews (
    OrderID,
    ReviewID,
    CustomerID,
    DateKey,
    ReviewScore,
    ReviewCommentMessage,
    ReviewCreationDate,
    ReviewAnswerTimestamp,
    ReviewResponseDays
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/facts/fact_reviews/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
