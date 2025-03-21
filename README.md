# <span style="font-size: 28px;">E-Commerce Sales Analysis and Optimization using Azure Synapse Analytics</span>

## Overview

This project implements a comprehensive Extract, Transform, and Load (ETL) pipeline leveraging the advanced capabilities of Azure Synapse Analytics. The pipeline efficiently extracts data from the Brazilian e-commerce dataset (Olist), performs sophisticated transformations through PySpark notebooks, and implements a three-layer data lakehouse architecture using Azure Data Lake Storage Gen2. The solution incorporates dimensional modeling techniques to create analytics-ready data structures stored in both Delta format for data lake querying and synced to dedicated SQL pools for enterprise reporting. Through this approach, the pipeline delivers actionable sales insights that enhance inventory management, marketing optimization, and overall business decision-making processes.

## Dataset

The [dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) used in this project contains comprehensive information on Brazilian e-commerce transactions from Olist, with approximately 100,000 orders spanning 2016-2018. It includes detailed order information, product categories, customer demographics, shipping details, payment methods, and customer reviews, allowing for in-depth analysis of sales patterns, customer behavior, and operational efficiency in a real-world e-commerce environment.

<div align="center">
  <img src="images/dataset_schema_overview.png" alt="Dataset Schema Overview" width="850">
  <p><i>Figure 1: Overview of the Olist dataset schema showing entity relationships and table structures</i></p>
</div>

The dataset schema above illustrates the complex relationships between the various tables in the Olist dataset, including customers, orders, products, sellers, and reviews. Understanding these relationships is crucial for building an effective dimensional model.

## Prerequisites

1. **Microsoft Azure Subscription**
2. **Azure Data Lake Storage Gen2**: A hierarchical namespace-enabled storage solution optimized for analytics workloads, providing the foundation for the data lakehouse architecture with enhanced performance and cost-effectiveness.
3. **Azure Synapse Analytics**: An integrated analytics service that brings together enterprise data warehousing and big data analytics, with dedicated SQL pools for high-performance queries and serverless SQL pools for flexible data exploration.
4. **Azure Synapse Spark Pools**: Apache Spark runtime environment within Synapse for scalable data processing and machine learning workloads, supporting Python, Scala, and SQL.
5. **Azure Key Vault**: Secure storage for sensitive credentials and connection strings, enabling safe access to Kaggle APIs and data sources.
6. **Azure Synapse Pipelines**: Orchestration service for coordinating data movement and transformation activities, scheduling notebook execution, and implementing monitoring capabilities.

## Data Architecture

The project implements a three-layer data lakehouse architecture:

1. **Raw Layer**: Original, immutable data ingested from Kaggle stored in its native format
2. **Processed Layer**: Cleansed data with standardized structures, data type conversions, and quality validations
3. **Curated Layer**: Business-ready dimensional model with fact and dimension tables supporting analytics use cases

## Data Flow

The ETL pipeline consists of sequential stages that transform raw e-commerce data into analytics-ready insights:

1. **Extract Data**: Retrieve e-commerce data from Kaggle using secure API integration, landing raw files in the Data Lake's raw zone
2. **Process Data**: Transform raw data through Spark notebooks, implementing data quality checks, standardization, and feature engineering in the processed layer
3. **Curate Models**: Develop dimensional models (star schema) with fact and dimension tables optimized for analytical queries in the curated layer
4. **Load to SQL**: Populate dedicated SQL pools with the dimensional model for enterprise reporting and dashboard creation
5. **Automation**: Orchestrate the end-to-end pipeline in Synapse Pipelines with parameterized execution, error handling, and monitoring

<div align="center">
  <img src="images/Flow.png" alt="ETL Data Flow" width="850">
  <p><i>Figure 2: End-to-end ETL data flow showing movement from raw data through to analytics consumption</i></p>
</div>

The flow diagram above illustrates how data moves through the three architectural layers, with raw data being extracted from Kaggle, transformed through a series of processing steps, and ultimately surfaced as analytics-ready insights in both Delta tables and SQL pools.

## Dimensional Model

The project implements a comprehensive star schema design for analytics optimization. The dimensional model consists of fact tables that capture business metrics and dimension tables that provide contextual attributes for analysis.

<div align="center">
  <img src="images/dimensions_facts_tables.svg" alt="Dimensional Model" width="850">
  <p><i>Figure 3: Star schema dimensional model showing fact tables and their relationships with dimension tables</i></p>
</div>

The dimensional model diagram illustrates how our fact tables (`fact_sales` and `fact_reviews`) connect to dimension tables (`dim_customer`, `dim_product`, `dim_seller`, `dim_geography`, and `dim_date`). This structure enables efficient querying for business intelligence applications and supports complex analytical queries with optimal performance.

## Aggregation Tables

To enhance query performance and support common analytical patterns, the pipeline creates pre-aggregated tables that summarize data at various levels of granularity:

<div align="center">
  <img src="images/aggregation_tables.svg" alt="Aggregation Tables" width="850">
  <p><i>Figure 4: Aggregation table structure showing summarized data for common analytical scenarios</i></p>
</div>

These aggregation tables provide pre-computed metrics for geographic analysis, temporal trends, product category performance, and seller effectiveness. By materializing these common aggregations, the solution dramatically improves query response times for dashboards and reports while reducing computational load on the analytical systems.

## Notebook Pipeline

The transformation logic is implemented through a sequence of PySpark notebooks in Azure Synapse Analytics. Each notebook handles specific aspects of the data processing pipeline:

<div align="center">
  <img src="images/simplified_notebook_pipeline.png" alt="Notebook Pipeline" width="850">
  <p><i>Figure 5: Sequential execution flow of Synapse notebooks showing transformation stages and dependencies</i></p>
</div>

This pipeline diagram shows the logical flow and dependencies between notebooks, illustrating how data progresses from initial extraction through transformation stages to final analytical outputs. The modular design allows for isolated testing, reusability, and maintenance of individual transformation components.

The notebook pipeline implements:
- Data extraction and initial validation
- Schema standardization and data cleansing
- Feature engineering and enrichment
- Dimensional model creation
- Analytics view generation

## <span style="font-size: 24px;">Analysis</span>

In this project, we conduct a comprehensive analysis of Brazilian e-commerce sales data using dimensional modeling techniques. Our analysis leverages the star schema design to explore key analytical dimensions that provide actionable insights across different aspects of the e-commerce business.

### <span style="font-size: 20px;">Analysis #1: Geographic Sales Distribution</span>

This [analysis](notebooks/output/csvs/agg_sales_by_state) provides a detailed breakdown of sales data across Brazilian states. It includes metrics such as order density, sales volume, average order value, and market penetration rates per geographic region. The insights enable targeted regional marketing campaigns, logistics optimization, and strategic expansion planning based on localized customer behaviors and preferences.

### <span style="font-size: 20px;">Analysis #2: Product Category Performance</span>

This [analysis](notebooks/output/csvs/agg_sales_by_category) examines performance metrics across product categories within the marketplace. It includes measures for category revenue, order frequency, and product popularity rankings. These insights support product assortment planning, pricing strategy optimization, and promotional calendar development to maximize category revenue and profitability.

### <span style="font-size: 20px;">Analysis #3: Monthly Sales Trends</span>

This [analysis](notebooks/output/csvs/agg_monthly_sales) provides temporal insights into sales patterns across months and seasons. It includes metrics on monthly revenue, order volume, and average ticket value. The data helps identify seasonality, growth trends, and anomalies, enabling more effective sales forecasting, promotion planning, and inventory management.

### <span style="font-size: 20px;">Analysis #4: Order Status Analysis</span>

This [analysis](notebooks/output/csvs/agg_order_status) examines the distribution and impact of different order statuses on the business. It includes metrics on order fulfillment rates, cancellation frequency, and status transition times. These insights help optimize the order fulfillment process, reduce cancellations, and improve the overall customer experience.

### <span style="font-size: 20px;">Analysis #5: Cross-State Commerce Analysis</span>

This [analysis](notebooks/output/csvs/agg_cross_state_analysis) investigates the patterns of interstate commerce, tracking orders where customers and sellers are in different states. It includes metrics on cross-state shipping volume, delivery times, and regional interconnections. The insights enable logistics optimization, shipping cost management, and strategic decisions about seller recruitment in underserved regions.

### <span style="font-size: 20px;">Analysis #6: Seller Performance Metrics</span>

This [analysis](notebooks/output/csvs/agg_seller_performance) evaluates marketplace seller performance across multiple dimensions. It includes metrics on seller order volume, customer satisfaction scores, delivery efficiency, and revenue generation. These insights support seller relationship management, performance-based incentives, and targeted interventions to improve overall marketplace quality.

### <span style="font-size: 20px;">Analysis #7: Product Size Impact Analysis</span>

This [analysis](notebooks/output/csvs/agg_size_analysis) examines how product size attributes influence sales performance across categories. It tracks metrics on size-specific order volumes, revenue contribution, and customer preferences. The insights help optimize inventory stocking by size, inform product development decisions, and create more effective merchandising strategies that align with customer size preferences.

## <span style="font-size: 24px;">Dimensional Tables</span>

The project creates a standard set of dimension tables to support the analytical models:

<table width="100%" border="1" style="font-size: 16px;">
  <tr>
    <th align="left">Dimension</th>
    <th align="left">Description</th>
  </tr>
  <tr>
    <td><a href="notebooks/output/csvs/dim_customer">Customer Dimension</a></td>
    <td>Contains customer demographic information, location data, and customer segmentation attributes</td>
  </tr>
  <tr>
    <td><a href="notebooks/output/csvs/dim_product">Product Dimension</a></td>
    <td>Stores product details including category hierarchies, physical attributes, and pricing information</td>
  </tr>
  <tr>
    <td><a href="notebooks/output/csvs/dim_seller">Seller Dimension</a></td>
    <td>Includes seller profiles, performance metrics, and geographic location</td>
  </tr>
  <tr>
    <td><a href="notebooks/output/csvs/dim_geography">Geography Dimension</a></td>
    <td>Maintains hierarchical location data with state, city, and postal code relationships</td>
  </tr>
  <tr>
    <td><a href="notebooks/output/csvs/dim_date">Date Dimension</a></td>
    <td>Provides calendar-related attributes for time-based analysis</td>
  </tr>
</table>

## <span style="font-size: 24px;">Fact Tables</span>

The core transaction data is stored in two primary fact tables:

<table width="100%" border="1" style="font-size: 16px;">
  <tr>
    <th align="left">Fact Table</th>
    <th align="left">Description</th>
  </tr>
  <tr>
    <td><a href="notebooks/output/csvs/fact_sales">Sales Fact Table</a></td>
    <td>Contains order-level transactional data with foreign keys to all relevant dimensions</td>
  </tr>
  <tr>
    <td><a href="notebooks/output/csvs/fact_reviews">Reviews Fact Table</a></td>
    <td>Stores customer review information linked to orders and products</td>
  </tr>
</table>

These fact tables form the foundation of the dimensional model, connecting the "facts" of business events with the contextual dimensions that describe them.

## Future Enhancements

Potential extensions to this project include:
- Inclusion of interactive Fabric Power BI dashboards on descriptions
- Implementation of machine learning models for demand forecasting and customer segmentation
- Integration with real-time data sources for near-real-time analytics
- Implementation of automated alerting based on key performance indicators