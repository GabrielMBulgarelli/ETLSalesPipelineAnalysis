-- Fact: Sales By Category
COPY INTO ecom.FactSalesByCategory (
    ProductCategoryNameEnglish,
    OrdersCount,
    UniqueCustomers,
    TotalSales,
    AvgItemPrice,
    AvgDeliveryTime,
    DelayedOrders,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/sales_by_category/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);

-- Fact: Size Analysis
COPY INTO ecom.FactSizeAnalysis (
    ProductCategoryNameEnglish,
    SizeCategory,
    OrdersCount,
    TotalSales,
    AvgShippingCost,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/size_analysis/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);

-- Fact: Seller Performance
COPY INTO ecom.FactSellerPerformance (
    SellerID,
    SellerState,
    OrdersCount,
    TotalSales,
    AvgShippingCost,
    AvgDeliveryTime,
    DelayedOrders,
    DelayRate,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/seller_performance/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);

-- Fact: Sales By State
COPY INTO ecom.FactSalesByState (
    CustomerState,
    OrdersCount,
    UniqueCustomers,
    TotalSales,
    AvgItemPrice,
    AvgDeliveryTime,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/sales_by_state/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);

-- Fact: Monthly Sales
COPY INTO ecom.FactMonthlySales (
    Year,
    Month,
    OrdersCount,
    UniqueCustomers,
    TotalSales,
    AvgItemPrice,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/monthly_sales/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);

-- Fact: Cross State Analysis
COPY INTO ecom.FactCrossStateAnalysis (
    ProductCategoryNameEnglish,
    IsCrossState,
    OrdersCount,
    TotalSales,
    AvgDeliveryTime,
    DelayRate,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/cross_state_analysis/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);

-- Fact: Payment Analysis
COPY INTO ecom.FactPaymentAnalysis (
    PaymentType,
    OrdersCount,
    TotalSales,
    AvgOrderValue,
    UniqueCustomers,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/payment_methods/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);

-- Fact: Order Status Analysis
COPY INTO ecom.FactOrderStatusAnalysis (
    ProductCategoryNameEnglish,
    OrderStatus,
    OrdersCount,
    TotalSales,
    UniqueCustomers,
    LastUpdated
)
FROM 'https://ecomsalessa.dfs.core.windows.net/curated/ecommerce-dataset-l1/aggregates/order_status/'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY='Managed Identity'),
    MAXERRORS = 10
);
