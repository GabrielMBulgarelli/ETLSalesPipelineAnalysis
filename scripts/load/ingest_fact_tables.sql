-- Run after dimensions. This combined script mirrors the two scripts in load/fact/.
TRUNCATE TABLE ecom.StageFactSales;
COPY INTO ecom.StageFactSales (
    OrderID, OrderItemID, CustomerID, ProductID, SellerID, DateKey, StatusID,
    ZipCodePrefix, OrderPurchaseTimestamp, OrderDeliveredCustomerDate, Price,
    FreightValue, TotalItemValue, ShippingDays, DeliveryDays, TotalDays,
    IsDelayed, DelayDays, IsCrossState, PaymentType
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/facts/fact_sales/'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY='Managed Identity'), MAXERRORS = 10);

TRUNCATE TABLE ecom.FactSales;
INSERT INTO ecom.FactSales (
    OrderID, OrderItemID, CustomerKey, ProductKey, SellerKey, DateKey, StatusKey,
    GeographyKey, OrderPurchaseTimestamp, OrderDeliveredCustomerDate, Price,
    FreightValue, TotalItemValue, ShippingDays, DeliveryDays, TotalDays,
    IsDelayed, DelayDays, IsCrossState, PaymentType
)
SELECT stage.OrderID, stage.OrderItemID, customer.CustomerKey, product.ProductKey,
    seller.SellerKey, stage.DateKey, status.StatusKey, geography.GeographyKey,
    stage.OrderPurchaseTimestamp, stage.OrderDeliveredCustomerDate, stage.Price,
    stage.FreightValue, stage.TotalItemValue, stage.ShippingDays, stage.DeliveryDays,
    stage.TotalDays, stage.IsDelayed, stage.DelayDays, stage.IsCrossState, stage.PaymentType
FROM ecom.StageFactSales AS stage
JOIN ecom.DimCustomer AS customer ON stage.CustomerID = customer.CustomerID AND customer.CurrentFlag = 1
JOIN ecom.DimProduct AS product ON stage.ProductID = product.ProductID AND product.CurrentFlag = 1
JOIN ecom.DimSeller AS seller ON stage.SellerID = seller.SellerID AND seller.CurrentFlag = 1
JOIN ecom.DimOrderStatus AS status ON stage.StatusID = status.StatusID
LEFT JOIN ecom.DimGeography AS geography ON stage.ZipCodePrefix = geography.ZipCodePrefix AND geography.CurrentFlag = 1;

TRUNCATE TABLE ecom.StageFactReviews;
COPY INTO ecom.StageFactReviews (
    OrderID, ReviewID, CustomerID, DateKey, ReviewScore, ReviewCommentMessage,
    ReviewCreationDate, ReviewAnswerTimestamp, ReviewResponseDays
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/facts/fact_reviews/'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY='Managed Identity'), MAXERRORS = 10);

TRUNCATE TABLE ecom.FactReviews;
INSERT INTO ecom.FactReviews (
    OrderID, ReviewID, CustomerKey, DateKey, ReviewScore, ReviewCommentMessage,
    ReviewCreationDate, ReviewAnswerTimestamp, ReviewResponseDays
)
SELECT stage.OrderID, stage.ReviewID, customer.CustomerKey, stage.DateKey,
    stage.ReviewScore, stage.ReviewCommentMessage, stage.ReviewCreationDate,
    stage.ReviewAnswerTimestamp, stage.ReviewResponseDays
FROM ecom.StageFactReviews AS stage
JOIN ecom.DimCustomer AS customer ON stage.CustomerID = customer.CustomerID AND customer.CurrentFlag = 1;
