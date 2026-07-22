TRUNCATE TABLE ecom.StageFactSales;

COPY INTO ecom.StageFactSales (
    OrderID, OrderItemID, CustomerID, ProductID, SellerID, DateKey, StatusID,
    ZipCodePrefix, OrderPurchaseTimestamp, OrderDeliveredCustomerDate, Price,
    FreightValue, TotalItemValue, ShippingDays, DeliveryDays, TotalDays,
    IsDelayed, DelayDays, IsCrossState, PaymentType
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/facts/fact_sales/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);

TRUNCATE TABLE ecom.FactSales;

INSERT INTO ecom.FactSales (
    OrderID, OrderItemID, CustomerKey, ProductKey, SellerKey, DateKey, StatusKey,
    GeographyKey, OrderPurchaseTimestamp, OrderDeliveredCustomerDate, Price,
    FreightValue, TotalItemValue, ShippingDays, DeliveryDays, TotalDays,
    IsDelayed, DelayDays, IsCrossState, PaymentType
)
SELECT
    stage.OrderID, stage.OrderItemID, customer.CustomerKey, product.ProductKey,
    seller.SellerKey, stage.DateKey, status.StatusKey, geography.GeographyKey,
    stage.OrderPurchaseTimestamp, stage.OrderDeliveredCustomerDate, stage.Price,
    stage.FreightValue, stage.TotalItemValue, stage.ShippingDays, stage.DeliveryDays,
    stage.TotalDays, stage.IsDelayed, stage.DelayDays, stage.IsCrossState, stage.PaymentType
FROM ecom.StageFactSales AS stage
JOIN ecom.DimCustomer AS customer
  ON stage.CustomerID = customer.CustomerID AND customer.CurrentFlag = 1
JOIN ecom.DimProduct AS product
  ON stage.ProductID = product.ProductID AND product.CurrentFlag = 1
JOIN ecom.DimSeller AS seller
  ON stage.SellerID = seller.SellerID AND seller.CurrentFlag = 1
JOIN ecom.DimOrderStatus AS status
  ON stage.StatusID = status.StatusID
LEFT JOIN ecom.DimGeography AS geography
  ON stage.ZipCodePrefix = geography.ZipCodePrefix AND geography.CurrentFlag = 1;
