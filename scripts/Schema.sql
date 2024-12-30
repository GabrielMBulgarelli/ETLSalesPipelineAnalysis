-- Check if schema exists, then drop it
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'ecom')
BEGIN
    -- Drop views first (using Synapse SQL Pool syntax)
    IF OBJECT_ID('ecom.VW_OrdersByDateState', 'V') IS NOT NULL
        DROP VIEW ecom.VW_OrdersByDateState;
    
    IF OBJECT_ID('ecom.VW_ProductCategorySales', 'V') IS NOT NULL
        DROP VIEW ecom.VW_ProductCategorySales;
    
    -- Drop tables individually
    IF OBJECT_ID('ecom.FactSalesByCategory', 'U') IS NOT NULL
        DROP TABLE ecom.FactSalesByCategory;
        
    IF OBJECT_ID('ecom.FactSalesByState', 'U') IS NOT NULL
        DROP TABLE ecom.FactSalesByState;
        
    IF OBJECT_ID('ecom.FactSellerPerformance', 'U') IS NOT NULL
        DROP TABLE ecom.FactSellerPerformance;
        
    IF OBJECT_ID('ecom.FactMonthlySales', 'U') IS NOT NULL
        DROP TABLE ecom.FactMonthlySales;
        
    IF OBJECT_ID('ecom.FactCrossStateAnalysis', 'U') IS NOT NULL
        DROP TABLE ecom.FactCrossStateAnalysis;
        
    IF OBJECT_ID('ecom.FactSizeAnalysis', 'U') IS NOT NULL
        DROP TABLE ecom.FactSizeAnalysis;
        
    IF OBJECT_ID('ecom.FactPaymentAnalysis', 'U') IS NOT NULL
        DROP TABLE ecom.FactPaymentAnalysis;
        
    IF OBJECT_ID('ecom.FactSales', 'U') IS NOT NULL
        DROP TABLE ecom.FactSales;
        
    IF OBJECT_ID('ecom.FactReviews', 'U') IS NOT NULL
        DROP TABLE ecom.FactReviews;
    
    -- Drop dimension tables
    IF OBJECT_ID('ecom.DimCustomer', 'U') IS NOT NULL
        DROP TABLE ecom.DimCustomer;
        
    IF OBJECT_ID('ecom.DimProduct', 'U') IS NOT NULL
        DROP TABLE ecom.DimProduct;
        
    IF OBJECT_ID('ecom.DimSeller', 'U') IS NOT NULL
        DROP TABLE ecom.DimSeller;
        
    IF OBJECT_ID('ecom.DimDate', 'U') IS NOT NULL
        DROP TABLE ecom.DimDate;
        
    IF OBJECT_ID('ecom.DimGeography', 'U') IS NOT NULL
        DROP TABLE ecom.DimGeography;
        
    IF OBJECT_ID('ecom.DimOrderStatus', 'U') IS NOT NULL
        DROP TABLE ecom.DimOrderStatus;
    
    -- Drop staging tables
    IF OBJECT_ID('ecom.StagingCustomers', 'U') IS NOT NULL
        DROP TABLE ecom.StagingCustomers;
        
    IF OBJECT_ID('ecom.StagingProducts', 'U') IS NOT NULL
        DROP TABLE ecom.StagingProducts;
        
    IF OBJECT_ID('ecom.StagingSellers', 'U') IS NOT NULL
        DROP TABLE ecom.StagingSellers;
        
    IF OBJECT_ID('ecom.StagingOrders', 'U') IS NOT NULL
        DROP TABLE ecom.StagingOrders;
        
    IF OBJECT_ID('ecom.StagingOrderItems', 'U') IS NOT NULL
        DROP TABLE ecom.StagingOrderItems;
        
    IF OBJECT_ID('ecom.StagingOrderPayments', 'U') IS NOT NULL
        DROP TABLE ecom.StagingOrderPayments;
        
    IF OBJECT_ID('ecom.StagingOrderReviews', 'U') IS NOT NULL
        DROP TABLE ecom.StagingOrderReviews;
        
    IF OBJECT_ID('ecom.StagingGeolocation', 'U') IS NOT NULL
        DROP TABLE ecom.StagingGeolocation;
        
    IF OBJECT_ID('ecom.StagingDate', 'U') IS NOT NULL
        DROP TABLE ecom.StagingDate;
    
    -- Drop procedures
    IF OBJECT_ID('ecom.PopulateDateDimension', 'P') IS NOT NULL
        DROP PROCEDURE ecom.PopulateDateDimension;
    
    IF OBJECT_ID('ecom.PopulateDateDimension', 'P') IS NOT NULL
    BEGIN
        PRINT 'Dropping existing procedure ecom.PopulateDateDimension';
        DROP PROCEDURE ecom.PopulateDateDimension;
    END

    -- Drop the schema
    DROP SCHEMA ecom;
END
GO

-- Create schema
CREATE SCHEMA ecom;
GO

-- ===============================
-- STAGING TABLES
-- ===============================

-- These tables will temporarily hold data during the ETL process
CREATE TABLE ecom.StagingCustomers (
    customer_id VARCHAR(100) NOT NULL,
    customer_unique_id VARCHAR(100),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(2)
) WITH (DISTRIBUTION = ROUND_ROBIN);

CREATE TABLE ecom.StagingProducts (
    product_id VARCHAR(100) NOT NULL,
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT,
    product_volume_cm3 FLOAT
) WITH (DISTRIBUTION = ROUND_ROBIN);

CREATE TABLE ecom.StagingSellers (
    seller_id VARCHAR(100) NOT NULL,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state VARCHAR(2)
) WITH (DISTRIBUTION = ROUND_ROBIN);

CREATE TABLE ecom.StagingOrders (
    order_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100),
    order_status VARCHAR(50),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    shipping_days INT,
    delivery_days INT,
    total_days INT,
    is_delayed BIT,
    delay_days INT,
    is_approved BIT,
    is_shipped BIT,
    is_delivered BIT
) WITH (DISTRIBUTION = HASH(order_id));

CREATE TABLE ecom.StagingOrderItems (
    order_id VARCHAR(100) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(100),
    seller_id VARCHAR(100),
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2)
) WITH (DISTRIBUTION = HASH(order_id));

CREATE TABLE ecom.StagingOrderPayments (
    order_id VARCHAR(100) NOT NULL,
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10,2)
) WITH (DISTRIBUTION = HASH(order_id));

CREATE TABLE ecom.StagingOrderReviews (
    review_id VARCHAR(100) NOT NULL,
    order_id VARCHAR(100),
    review_score INT,
    review_comment_message NVARCHAR(4000),
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME
) WITH (DISTRIBUTION = HASH(order_id));

CREATE TABLE ecom.StagingGeolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(2)
) WITH (DISTRIBUTION = ROUND_ROBIN);

CREATE TABLE ecom.StagingDate (
    Date DATE NOT NULL
) WITH (DISTRIBUTION = ROUND_ROBIN);

-- ===============================
-- DIMENSION TABLES
-- ===============================

CREATE TABLE ecom.DimCustomer (
    CustomerKey INT IDENTITY(1,1) NOT NULL,
    CustomerID VARCHAR(100) NOT NULL,
    CustomerUniqueID VARCHAR(100) NOT NULL,
    CustomerZipCodePrefix INT,
    CustomerCity VARCHAR(100),
    CustomerState VARCHAR(2),
    RowEffectiveDate DATETIME NOT NULL ,
    RowExpirationDate DATETIME NULL,
    CurrentFlag BIT NOT NULL DEFAULT 1,
    CONSTRAINT PK_DimCustomer PRIMARY KEY NONCLUSTERED (CustomerKey) NOT ENFORCED
) WITH (DISTRIBUTION = HASH(CustomerID), CLUSTERED COLUMNSTORE INDEX);

CREATE TABLE ecom.DimProduct (
    ProductKey INT IDENTITY(1,1) NOT NULL,
    ProductID VARCHAR(100) NOT NULL,
    ProductCategoryName VARCHAR(100),
    ProductCategoryNameEnglish VARCHAR(100),
    ProductWeightG FLOAT,
    ProductLengthCm FLOAT,
    ProductHeightCm FLOAT,
    ProductWidthCm FLOAT,
    ProductVolumeCm3 FLOAT,
    SizeCategory VARCHAR(20),
    RowEffectiveDate DATETIME NOT NULL ,
    RowExpirationDate DATETIME NULL,
    CurrentFlag BIT NOT NULL DEFAULT 1,
    CONSTRAINT PK_DimProduct PRIMARY KEY NONCLUSTERED (ProductKey) NOT ENFORCED
) WITH (DISTRIBUTION = HASH(ProductID), CLUSTERED COLUMNSTORE INDEX);

CREATE TABLE ecom.DimSeller (
    SellerKey INT IDENTITY(1,1) NOT NULL,
    SellerID VARCHAR(100) NOT NULL,
    SellerZipCodePrefix INT,
    SellerCity VARCHAR(100),
    SellerState VARCHAR(2),
    RowEffectiveDate DATETIME NOT NULL ,
    RowExpirationDate DATETIME NULL,
    CurrentFlag BIT NOT NULL DEFAULT 1,
    CONSTRAINT PK_DimSeller PRIMARY KEY NONCLUSTERED (SellerKey) NOT ENFORCED
) WITH (DISTRIBUTION = HASH(SellerID), CLUSTERED COLUMNSTORE INDEX);

CREATE TABLE ecom.DimDate (
    DateKey INT NOT NULL,
    DateID VARCHAR(10) NOT NULL,
    Date DATE NOT NULL,
    Year INT NOT NULL,
    Month INT NOT NULL,
    Day INT NOT NULL,
    Quarter INT NOT NULL,
    WeekOfYear INT NOT NULL,
    DayOfWeek INT NOT NULL,
    IsWeekend BIT NOT NULL,
    MonthName VARCHAR(10) NOT NULL,
    DayName VARCHAR(10) NOT NULL,
    FiscalYear INT NOT NULL,
    FiscalQuarter INT NOT NULL,
    Holiday VARCHAR(50) NULL,
    IsHoliday BIT NOT NULL DEFAULT 0,
    CONSTRAINT PK_DimDate PRIMARY KEY NONCLUSTERED (DateKey) NOT ENFORCED
) WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX);

CREATE TABLE ecom.DimGeography (
    GeographyKey INT IDENTITY(1,1) NOT NULL,
    ZipCodePrefix INT NOT NULL,
    City VARCHAR(100),
    State VARCHAR(2),
    Latitude FLOAT,
    Longitude FLOAT,
    Region VARCHAR(50),
    RowEffectiveDate DATETIME NOT NULL ,
    RowExpirationDate DATETIME NULL,
    CurrentFlag BIT NOT NULL DEFAULT 1,
    CONSTRAINT PK_DimGeography PRIMARY KEY NONCLUSTERED (GeographyKey) NOT ENFORCED
) WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX);

CREATE TABLE ecom.DimOrderStatus (
    StatusKey INT IDENTITY(1,1) NOT NULL,
    StatusID VARCHAR(50) NOT NULL,
    StatusDescription VARCHAR(200),
    IsApproved BIT,
    IsDelivered BIT,
    IsShipped BIT,
    IsCancelled BIT,
    CONSTRAINT PK_DimOrderStatus PRIMARY KEY NONCLUSTERED (StatusKey) NOT ENFORCED
) WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX);

-- ===============================
-- FACT TABLES
-- ===============================

CREATE TABLE ecom.FactSales (
    SalesKey BIGINT IDENTITY(1,1) NOT NULL,
    OrderID VARCHAR(100) NOT NULL,
    OrderItemID INT NOT NULL,
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    SellerKey INT NOT NULL,
    DateKey INT NOT NULL,
    StatusKey INT NOT NULL,
    GeographyKey INT NULL,
    OrderPurchaseTimestamp DATETIME,
    OrderDeliveredCustomerDate DATETIME,
    Price DECIMAL(10,2) NOT NULL,
    FreightValue DECIMAL(10,2) NOT NULL,
    TotalItemValue DECIMAL(10,2) NOT NULL,
    ShippingDays INT,
    DeliveryDays INT,
    TotalDays INT,
    IsDelayed BIT,
    DelayDays INT,
    IsCrossState BIT NOT NULL,
    PaymentType VARCHAR(50),
    CONSTRAINT PK_FactSales PRIMARY KEY NONCLUSTERED (SalesKey) NOT ENFORCED
) WITH (DISTRIBUTION = HASH(OrderID), CLUSTERED COLUMNSTORE INDEX);

CREATE TABLE ecom.FactReviews (
    ReviewKey BIGINT IDENTITY(1,1) NOT NULL,
    OrderID VARCHAR(100) NOT NULL,
    ReviewID VARCHAR(100) NOT NULL,
    CustomerKey INT NOT NULL,
    DateKey INT NOT NULL,
    ReviewScore INT NOT NULL,
    ReviewCommentMessage NVARCHAR(4000),
    ReviewCreationDate DATETIME,
    ReviewAnswerTimestamp DATETIME,
    ReviewResponseDays INT,
    CONSTRAINT PK_FactReviews PRIMARY KEY NONCLUSTERED (ReviewKey) NOT ENFORCED
) WITH (DISTRIBUTION = HASH(OrderID), CLUSTERED COLUMNSTORE INDEX);

-- ===============================
-- AGGREGATION TABLES
-- ===============================

CREATE TABLE ecom.FactSalesByCategory (
    CategoryKey INT IDENTITY(1,1) NOT NULL,
    ProductCategoryNameEnglish VARCHAR(100) NOT NULL,
    OrdersCount INT NOT NULL,
    UniqueCustomers INT NOT NULL,
    TotalSales DECIMAL(15,2) NOT NULL,
    AvgItemPrice DECIMAL(10,2) NOT NULL,
    AvgDeliveryTime FLOAT,
    DelayedOrders INT NOT NULL,
    LastUpdated DATETIME NOT NULL,
    CONSTRAINT PK_FactSalesByCategory PRIMARY KEY NONCLUSTERED (CategoryKey) NOT ENFORCED
) WITH (DISTRIBUTION = HASH(ProductCategoryNameEnglish), CLUSTERED COLUMNSTORE INDEX);

CREATE TABLE ecom.FactSalesByState (
    StateKey INT IDENTITY(1,1) NOT NULL,
    CustomerState VARCHAR(2) NOT NULL,
    OrdersCount INT NOT NULL,
    UniqueCustomers INT NOT NULL,
    TotalSales DECIMAL(15,2) NOT NULL,
    AvgItemPrice DECIMAL(10,2) NOT NULL,
    AvgDeliveryTime FLOAT,
    LastUpdated DATETIME NOT NULL,
    CONSTRAINT PK_FactSalesByState PRIMARY KEY NONCLUSTERED (StateKey) NOT ENFORCED
) WITH (DISTRIBUTION = HASH(CustomerState), CLUSTERED COLUMNSTORE INDEX);

CREATE TABLE ecom.FactSellerPerformance (
    SellerPerformanceKey INT IDENTITY(1,1) NOT NULL,
    SellerID VARCHAR(100) NOT NULL,
    SellerState VARCHAR(2),
    OrdersCount INT NOT NULL,
    TotalSales DECIMAL(15,2) NOT NULL,
    AvgShippingCost DECIMAL(10,2) NOT NULL,
    AvgDeliveryTime FLOAT,
    DelayedOrders INT NOT NULL,
    DelayRate FLOAT NOT NULL,
    LastUpdated DATETIME NOT NULL,
    CONSTRAINT PK_FactSellerPerformance PRIMARY KEY NONCLUSTERED (SellerPerformanceKey) NOT ENFORCED
) WITH (DISTRIBUTION = HASH(SellerID), CLUSTERED COLUMNSTORE INDEX);

CREATE TABLE ecom.FactMonthlySales (
    MonthlyKey INT IDENTITY(1,1) NOT NULL,
    Year INT NOT NULL,
    Month INT NOT NULL,
    OrdersCount INT NOT NULL,
    UniqueCustomers INT NOT NULL,
    TotalSales DECIMAL(15,2) NOT NULL,
    AvgItemPrice DECIMAL(10,2) NOT NULL,
    LastUpdated DATETIME NOT NULL,
    CONSTRAINT PK_FactMonthlySales PRIMARY KEY NONCLUSTERED (MonthlyKey) NOT ENFORCED
) WITH (DISTRIBUTION = HASH(Year), CLUSTERED COLUMNSTORE INDEX);

CREATE TABLE ecom.FactCrossStateAnalysis (
    CrossStateKey INT IDENTITY(1,1) NOT NULL,
    ProductCategoryNameEnglish VARCHAR(100) NOT NULL,
    IsCrossState BIT NOT NULL,
    OrdersCount INT NOT NULL,
    TotalSales DECIMAL(15,2) NOT NULL,
    AvgDeliveryTime FLOAT,
    DelayRate FLOAT,
    LastUpdated DATETIME NOT NULL,
    CONSTRAINT PK_FactCrossStateAnalysis PRIMARY KEY NONCLUSTERED (CrossStateKey) NOT ENFORCED
) WITH (DISTRIBUTION = HASH(ProductCategoryNameEnglish), CLUSTERED COLUMNSTORE INDEX);

CREATE TABLE ecom.FactSizeAnalysis (
    SizeKey INT IDENTITY(1,1) NOT NULL,
    ProductCategoryNameEnglish VARCHAR(100) NOT NULL,
    SizeCategory VARCHAR(20) NOT NULL,
    OrdersCount INT NOT NULL,
    TotalSales DECIMAL(15,2) NOT NULL,
    AvgShippingCost DECIMAL(10,2) NOT NULL,
    LastUpdated DATETIME NOT NULL,
    CONSTRAINT PK_FactSizeAnalysis PRIMARY KEY NONCLUSTERED (SizeKey) NOT ENFORCED
) WITH (DISTRIBUTION = HASH(ProductCategoryNameEnglish), CLUSTERED COLUMNSTORE INDEX);

CREATE TABLE ecom.FactPaymentAnalysis (
    PaymentKey INT IDENTITY(1,1) NOT NULL,
    PaymentType VARCHAR(50) NOT NULL,
    OrdersCount INT NOT NULL,
    TotalSales DECIMAL(15,2) NOT NULL,
    AvgOrderValue DECIMAL(10,2) NOT NULL,
    UniqueCustomers INT NOT NULL,
    LastUpdated DATETIME NOT NULL,
    CONSTRAINT PK_FactPaymentAnalysis PRIMARY KEY NONCLUSTERED (PaymentKey) NOT ENFORCED
) WITH (DISTRIBUTION = HASH(PaymentType), CLUSTERED COLUMNSTORE INDEX);

-- ===============================
-- DIMENSION DATA LOADING
-- ===============================

-- Order Status Dimension initial load - Modified for Synapse compatibility
INSERT INTO ecom.DimOrderStatus (StatusID, StatusDescription, IsApproved, IsDelivered, IsShipped, IsCancelled)
SELECT 'CREATED', 'Order created, not yet approved', 0, 0, 0, 0
UNION ALL SELECT 'APPROVED', 'Order approved, payment confirmed', 1, 0, 0, 0
UNION ALL SELECT 'SHIPPED', 'Order shipped to customer', 1, 0, 1, 0
UNION ALL SELECT 'DELIVERED', 'Order delivered to customer', 1, 1, 1, 0
UNION ALL SELECT 'CANCELLED', 'Order cancelled', 0, 0, 0, 1
UNION ALL SELECT 'UNAVAILABLE', 'Order status unavailable', 0, 0, 0, 0;

-- ===============================
-- STORED PROCEDURES
-- ===============================

-- Create a procedure to populate the Date dimension
GO
CREATE PROCEDURE ecom.PopulateDateDimension
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @CurrentDate DATE = @StartDate;
    DECLARE @DateKey INT;
    DECLARE @DateID VARCHAR(10);
    DECLARE @Year INT;
    DECLARE @Month INT;
    DECLARE @Day INT;
    DECLARE @Quarter INT;
    DECLARE @WeekOfYear INT;
    DECLARE @DayOfWeek INT;
    DECLARE @IsWeekend BIT;
    DECLARE @MonthName VARCHAR(10);
    DECLARE @DayName VARCHAR(10);
    DECLARE @FiscalYear INT;
    DECLARE @FiscalQuarter INT;
    
    -- Generate all dates
    WHILE @CurrentDate <= @EndDate
    BEGIN
        -- Check if date already exists
        SET @DateKey = CONVERT(INT, CONVERT(VARCHAR(8), @CurrentDate, 112));
        
        IF NOT EXISTS (SELECT 1 FROM ecom.DimDate WHERE DateKey = @DateKey)
        BEGIN
            -- Calculate all values first
            SET @DateID = CONVERT(VARCHAR(10), @CurrentDate, 120);
            SET @Year = YEAR(@CurrentDate);
            SET @Month = MONTH(@CurrentDate);
            SET @Day = DAY(@CurrentDate);
            SET @Quarter = DATEPART(QUARTER, @CurrentDate);
            SET @WeekOfYear = DATEPART(WEEK, @CurrentDate);
            SET @DayOfWeek = DATEPART(WEEKDAY, @CurrentDate);
            
            IF @DayOfWeek IN (1, 7)
                SET @IsWeekend = 1;
            ELSE
                SET @IsWeekend = 0;
                
            SET @MonthName = DATENAME(MONTH, @CurrentDate);
            SET @DayName = DATENAME(WEEKDAY, @CurrentDate);
            
            IF @Month > 6
                SET @FiscalYear = @Year + 1;
            ELSE
                SET @FiscalYear = @Year;
                
            IF @Month BETWEEN 7 AND 9
                SET @FiscalQuarter = 1;
            ELSE IF @Month BETWEEN 10 AND 12
                SET @FiscalQuarter = 2;
            ELSE IF @Month BETWEEN 1 AND 3
                SET @FiscalQuarter = 3;
            ELSE
                SET @FiscalQuarter = 4;
            
            -- Insert the date with variables
            INSERT INTO ecom.DimDate (
                DateKey, DateID, Date, Year, Month, Day, Quarter, WeekOfYear, 
                DayOfWeek, IsWeekend, MonthName, DayName, FiscalYear, FiscalQuarter, IsHoliday
            )
            VALUES (
                @DateKey, @DateID, @CurrentDate, @Year, @Month, @Day, @Quarter, @WeekOfYear,
                @DayOfWeek, @IsWeekend, @MonthName, @DayName, @FiscalYear, @FiscalQuarter, 0
            );
        END
        
        SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
    END;
END;
GO

-- Execute procedure to populate date dimension with error handling
BEGIN TRY
    PRINT 'Starting date dimension population...';
    EXEC ecom.PopulateDateDimension '2016-01-01', '2025-12-31';
    PRINT 'Date dimension population completed successfully.';
END TRY
BEGIN CATCH
    PRINT 'Error occurred while populating date dimension:';
    PRINT ERROR_MESSAGE();
END CATCH;
GO

-- ===============================
-- STATISTICS FOR PERFORMANCE
-- ===============================

-- Create statistics for query performance
CREATE STATISTICS ST_FactSales_OrderDate ON ecom.FactSales(OrderPurchaseTimestamp)
WITH FULLSCAN;

CREATE STATISTICS ST_FactSales_IsDelayed ON ecom.FactSales(IsDelayed)
WITH FULLSCAN;

CREATE STATISTICS ST_FactSales_IsCrossState ON ecom.FactSales(IsCrossState)
WITH FULLSCAN;

CREATE STATISTICS ST_DimProduct_CategoryEnglish ON ecom.DimProduct(ProductCategoryNameEnglish)
WITH FULLSCAN;

CREATE STATISTICS ST_DimCustomer_State ON ecom.DimCustomer(CustomerState)
WITH FULLSCAN;

CREATE STATISTICS ST_DimSeller_State ON ecom.DimSeller(SellerState)
WITH FULLSCAN;

-- ===============================
-- VIEWS FOR COMMON QUERIES
-- ===============================

-- Create views for common query patterns
GO
CREATE VIEW ecom.VW_OrdersByDateState
AS
SELECT 
    d.Year,
    d.Month,
    d.MonthName,
    c.CustomerState,
    COUNT(*) AS OrderCount,
    SUM(fs.TotalItemValue) AS TotalSales
FROM 
    ecom.FactSales fs
    JOIN ecom.DimDate d ON fs.DateKey = d.DateKey
    JOIN ecom.DimCustomer c ON fs.CustomerKey = c.CustomerKey
GROUP BY 
    d.Year,
    d.Month,
    d.MonthName,
    c.CustomerState;
GO

CREATE VIEW ecom.VW_ProductCategorySales
AS
SELECT 
    p.ProductCategoryNameEnglish,
    COUNT(*) AS OrderCount,
    SUM(fs.TotalItemValue) AS TotalSales,
    AVG(fs.Price) AS AvgPrice
FROM 
    ecom.FactSales fs
    JOIN ecom.DimProduct p ON fs.ProductKey = p.ProductKey
GROUP BY 
    p.ProductCategoryNameEnglish;
GO

-- ===============================
-- PERMISSIONS
-- ===============================

-- Grant read access to analytics users (uncomment when needed)
-- GRANT SELECT ON SCHEMA::ecom TO [AnalyticsUsers];

-- Grant write access to ETL process (uncomment when needed)
-- GRANT INSERT, UPDATE, DELETE ON SCHEMA::ecom TO [ETLProcess];