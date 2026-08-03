TRUNCATE TABLE ecom.StageFactReviews;

COPY INTO ecom.StageFactReviews (
    OrderID, ReviewID, CustomerID, DateKey, ReviewScore, ReviewCommentMessage,
    ReviewCreationDate, ReviewAnswerTimestamp, ReviewResponseDays
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/facts/fact_reviews/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);

TRUNCATE TABLE ecom.FactReviews;

INSERT INTO ecom.FactReviews (
    OrderID, ReviewID, CustomerKey, DateKey, ReviewScore, ReviewCommentMessage,
    ReviewCreationDate, ReviewAnswerTimestamp, ReviewResponseDays
)
SELECT
    stage.OrderID, stage.ReviewID, customer.CustomerKey, stage.DateKey,
    stage.ReviewScore, stage.ReviewCommentMessage, stage.ReviewCreationDate,
    stage.ReviewAnswerTimestamp, stage.ReviewResponseDays
FROM ecom.StageFactReviews AS stage
JOIN ecom.DimCustomer AS customer
  ON stage.CustomerID = customer.CustomerID AND customer.CurrentFlag = 1;
