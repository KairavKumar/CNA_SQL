create database IF NOT EXISTS cna;

use cna;

-- Create raw inventory table
-- This table will be used to store the raw data before any transformations
CREATE TABLE inventory_raw (
    Date DATE,
    Store_ID VARCHAR(10),
    Product_ID VARCHAR(10),
    Category VARCHAR(50),
    Region VARCHAR(50),
    Inventory_Level INT,
    Units_Sold INT,
    Units_Ordered INT,
    Demand_Forecast FLOAT,
    Price FLOAT,
    Discount FLOAT,
    Weather_Condition VARCHAR(50),
    Holiday_Promotion VARCHAR(5),
    Competitor_Pricing FLOAT,
    Seasonality VARCHAR(20)
);

-- For MySQL import:
-- Here the following commands have to be run 
-- IN TERMINAL :  sudo mysql --local-infile=1 -u root -p
-- IN mysql : SET GLOBAL local_infile=1;
-- LOAD DATA LOCAL INFILE './retail_store_inventory.csv' INTO TABLE inventory_raw FIELDS TERMINATED BY ',' ignore 1 lines;


select * from inventory_raw;

-- Create dimension tables
-- Store dimension table
-- Create a fact table that preserves all original data
CREATE TABLE inventory_facts (
    Record_ID INT AUTO_INCREMENT PRIMARY KEY,
    Date DATE,
    Store_ID VARCHAR(10),
    Product_ID VARCHAR(10),
    Category VARCHAR(50),
    Region VARCHAR(50),
    Inventory_Level INT,
    Units_Sold INT,
    Units_Ordered INT,
    Demand_Forecast FLOAT,
    Price FLOAT,
    Discount FLOAT,
    Weather_Condition VARCHAR(50),
    Holiday_Promotion VARCHAR(5),
    Competitor_Pricing FLOAT,
    Seasonality VARCHAR(20),
    INDEX idx_store (Store_ID),
    INDEX idx_product (Product_ID),
    INDEX idx_date (Date),
    INDEX idx_region (Region),
    INDEX idx_category (Category)
);

-- Create a date dimension table for time-based analysis
CREATE TABLE dim_date (
    Date DATE PRIMARY KEY,
    Year INT,
    Month INT,
    Quarter INT,
    Day_Of_Week INT,
    Is_Weekend BOOLEAN,
    INDEX idx_year_month (Year, Month)
);

-- Create a weather dimension for weather analysis
CREATE TABLE dim_weather (
    Weather_Condition VARCHAR(50) PRIMARY KEY
);

-- Create a seasonality dimension
CREATE TABLE dim_seasonality (
    Seasonality VARCHAR(20) PRIMARY KEY
);
-- Populate the fact table with all original data
INSERT INTO inventory_facts (
    Date, Store_ID, Product_ID, Category, Region, 
    Inventory_Level, Units_Sold, Units_Ordered, Demand_Forecast,
    Price, Discount, Weather_Condition, Holiday_Promotion,
    Competitor_Pricing, Seasonality
)
SELECT
    Date, Store_ID, Product_ID, Category, Region,
    Inventory_Level, Units_Sold, Units_Ordered, Demand_Forecast,
    Price, Discount, Weather_Condition, Holiday_Promotion,
    Competitor_Pricing, Seasonality
FROM inventory_raw;

-- Populate the date dimension
INSERT IGNORE INTO dim_date (Date, Year, Month, Quarter, Day_Of_Week, Is_Weekend)
SELECT DISTINCT
    Date,
    YEAR(Date),
    MONTH(Date),
    QUARTER(Date),
    DAYOFWEEK(Date),
    CASE WHEN DAYOFWEEK(Date) IN (1, 7) THEN TRUE ELSE FALSE END
FROM inventory_raw;

-- Populate weather dimension
INSERT IGNORE INTO dim_weather (Weather_Condition)
SELECT DISTINCT Weather_Condition
FROM inventory_raw;

-- Populate seasonality dimension
INSERT IGNORE INTO dim_seasonality (Seasonality)
SELECT DISTINCT Seasonality
FROM inventory_raw;

-- Current stock levels across stores and products
SELECT 
    Store_ID, 
    Region,
    Product_ID,
    Category,
    Inventory_Level AS Current_Stock,
    Price,
    Units_Sold AS Last_Sales,
    Units_Ordered AS Upcoming_Orders,
    Demand_Forecast AS Expected_Demand
FROM inventory_facts
WHERE (Store_ID, Product_ID, Date) IN (
    SELECT Store_ID, Product_ID, MAX(Date)
    FROM inventory_facts
    GROUP BY Store_ID, Product_ID
)
ORDER BY Region, Store_ID, Category, Inventory_Level DESC;

-- Aggregate stock levels by region and category
SELECT 
    Region,
    Category,
    SUM(Inventory_Level) AS Total_Stock,
    AVG(Inventory_Level) AS Average_Stock,
    COUNT(DISTINCT Store_ID) AS Store_Count
FROM inventory_facts
WHERE (Store_ID, Product_ID, Date) IN (
    SELECT Store_ID, Product_ID, MAX(Date)
    FROM inventory_facts
    GROUP BY Store_ID, Product_ID
)
GROUP BY Region, Category
ORDER BY Region, Total_Stock DESC;


-- Calculate reorder points and identify low stock items
-- Step 1: Get the latest date per Store + Product
WITH LatestDatePerProduct AS (
    SELECT Store_ID, Product_ID, MAX(Date) AS LatestDate
    FROM inventory_facts
    GROUP BY Store_ID, Product_ID
),
-- Step 2: Get daily sales and current stock for latest date only
LatestProductSales AS (
    SELECT 
        f.Store_ID,
        f.Product_ID,
        f.Category,
        f.Region,
        f.Units_Sold,
        f.Inventory_Level,
        f.Price
    FROM inventory_facts f
    JOIN LatestDatePerProduct l
      ON f.Store_ID = l.Store_ID 
     AND f.Product_ID = l.Product_ID 
     AND f.Date = l.LatestDate
),

-- Step 3: Calculate average sales and reorder point
ProductSalesSummary AS (
    SELECT 
        f.Store_ID,
        f.Product_ID,
        f.Category,
        f.Region,
        ROUND(AVG(f.Units_Sold), 2) AS Avg_Daily_Sales,
        ROUND(STDDEV(f.Units_Sold), 2) AS StdDev_Sales,
        MAX(l.Inventory_Level) AS Current_Stock,
        MAX(l.Price) AS Unit_Price
    FROM inventory_facts f
    JOIN LatestProductSales l
      ON f.Store_ID = l.Store_ID 
     AND f.Product_ID = l.Product_ID
    GROUP BY f.Store_ID, f.Product_ID, f.Category, f.Region
)

-- Step 4: Final output
SELECT 
    Store_ID,
    Product_ID,
    Category,
    Region,
    Current_Stock,
    Avg_Daily_Sales,
    ROUND((7 * Avg_Daily_Sales) + (1.5 * StdDev_Sales)) AS Reorder_Point,
    CASE 
        WHEN Current_Stock <= 0 THEN 'Out of Stock'
        WHEN Current_Stock < ((7 * Avg_Daily_Sales) + (1.5 * StdDev_Sales)) THEN 'Below Reorder Point'
        WHEN Current_Stock < ((7 * Avg_Daily_Sales) + (1.5 * StdDev_Sales)) * 1.2 THEN 'Near Reorder Point'
        ELSE 'Adequate Stock'
    END AS Stock_Status,
    ROUND(Current_Stock / NULLIF(Avg_Daily_Sales, 0), 1) AS Days_Of_Supply
FROM ProductSalesSummary
ORDER BY 
    CASE 
        WHEN Current_Stock <= 0 THEN 1
        WHEN Current_Stock < ((7 * Avg_Daily_Sales) + (1.5 * StdDev_Sales)) THEN 2
        WHEN Current_Stock < ((7 * Avg_Daily_Sales) + (1.5 * StdDev_Sales)) * 1.2 THEN 3
        ELSE 4
    END,
    Days_Of_Supply;


-- Calculate reorder points and identify low stock items
-- Step 1: Get the latest date per Store + Product
WITH LatestDatePerProduct AS (
    SELECT Store_ID, Product_ID, MAX(Date) AS LatestDate
    FROM inventory_facts
    GROUP BY Store_ID, Product_ID
),

-- Step 2: Get daily sales and current stock for the latest date only
LatestProductSales AS (
    SELECT 
        f.Store_ID,
        f.Product_ID,
        f.Category,
        f.Region,
        f.Units_Sold,
        f.Inventory_Level,
        f.Price
    FROM inventory_facts f
    JOIN LatestDatePerProduct l
      ON f.Store_ID = l.Store_ID 
     AND f.Product_ID = l.Product_ID 
     AND f.Date = l.LatestDate
),

-- Step 3: Calculate average sales and reorder point (considering all sales history)
ProductSalesSummary AS (
    SELECT 
        f.Store_ID,
        f.Product_ID,
        f.Category,
        f.Region,
        ROUND(AVG(f.Units_Sold), 2) AS Avg_Daily_Sales,
        ROUND(STDDEV(f.Units_Sold), 2) AS StdDev_Sales,
        MAX(l.Inventory_Level) AS Current_Stock,
        MAX(l.Price) AS Unit_Price
    FROM inventory_facts f
    JOIN LatestProductSales l
      ON f.Store_ID = l.Store_ID 
     AND f.Product_ID = l.Product_ID
    GROUP BY f.Store_ID, f.Product_ID, f.Category, f.Region
)

-- Step 4: Final output
SELECT 
    Store_ID,
    Product_ID,
    Category,
    Region,
    Current_Stock,
    Avg_Daily_Sales,
    ROUND((7 * Avg_Daily_Sales) + (1.5 * StdDev_Sales)) AS Reorder_Point,
    CASE 
        WHEN Current_Stock <= 0 THEN 'Out of Stock'
        WHEN Current_Stock < ((7 * Avg_Daily_Sales) + (1.5 * StdDev_Sales)) THEN 'Below Reorder Point'
        WHEN Current_Stock < ((7 * Avg_Daily_Sales) + (1.5 * StdDev_Sales)) * 1.2 THEN 'Near Reorder Point'
        ELSE 'Adequate Stock'
    END AS Stock_Status,
    ROUND(Current_Stock / NULLIF(Avg_Daily_Sales, 0), 1) AS Days_Of_Supply
FROM ProductSalesSummary
ORDER BY 
    CASE 
        WHEN Current_Stock <= 0 THEN 1
        WHEN Current_Stock < ((7 * Avg_Daily_Sales) + (1.5 * StdDev_Sales)) THEN 2
        WHEN Current_Stock < ((7 * Avg_Daily_Sales) + (1.5 * StdDev_Sales)) * 1.2 THEN 3
        ELSE 4
    END,
    Days_Of_Supply;
-- Literally every single thing is apparently below reorder point i have tried working with different queries someone look at this
