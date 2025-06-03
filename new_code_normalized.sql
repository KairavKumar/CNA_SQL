-- Enhanced normalized schema for complete query support
CREATE DATABASE IF NOT EXISTS cna;
USE cna;

-- 1. STORES TABLE
CREATE TABLE stores (
    store_id VARCHAR(10) PRIMARY KEY,
    region VARCHAR(50) NOT NULL,
    INDEX idx_region (region)
);

-- 2. PRODUCTS TABLE
CREATE TABLE products (
    product_id VARCHAR(10) PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    base_price DECIMAL(10,2) NOT NULL, -- Base price the average price of the product across all stores
    INDEX idx_category (category)
);

-- 3. WEATHER CONDITIONS TABLE
CREATE TABLE weather_conditions (
    weather_id INT AUTO_INCREMENT PRIMARY KEY,
    weather_condition VARCHAR(50) UNIQUE NOT NULL
);

-- 4. SEASONALITY TABLE
CREATE TABLE seasonality (
    season_id INT AUTO_INCREMENT PRIMARY KEY,
    season_name VARCHAR(20) UNIQUE NOT NULL
);

-- 5. PROMOTIONS TABLE
CREATE TABLE promotions (
    promotion_id INT AUTO_INCREMENT PRIMARY KEY,
    is_holiday_promotion BOOLEAN NOT NULL DEFAULT FALSE,
    promotion_description VARCHAR(100)
);

-- 6. INVENTORY SNAPSHOTS TABLE (Time-series fact table)
CREATE TABLE inventory_snapshots (
    snapshot_id INT AUTO_INCREMENT PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    store_id VARCHAR(10) NOT NULL,
    product_id VARCHAR(10) NOT NULL,
    inventory_level INT NOT NULL,
    units_sold INT NOT NULL DEFAULT 0,
    units_ordered INT NOT NULL DEFAULT 0,
    demand_forecast DECIMAL(10,2),
    current_price DECIMAL(10,2) NOT NULL, -- Price can vary over time
    discount_percentage DECIMAL(5,2) DEFAULT 0,
    competitor_pricing DECIMAL(10,2),
    weather_id INT,
    season_id INT,
    promotion_id INT,
    
    -- Constraints
    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (weather_id) REFERENCES weather_conditions(weather_id),
    FOREIGN KEY (season_id) REFERENCES seasonality(season_id),
    FOREIGN KEY (promotion_id) REFERENCES promotions(promotion_id),
    
    -- Unique constraint to prevent duplicate snapshots
    UNIQUE KEY unique_snapshot (snapshot_date, store_id, product_id),
    
    -- Performance indexes
    INDEX idx_date (snapshot_date),
    INDEX idx_store_product_date (store_id, product_id, snapshot_date),
    INDEX idx_product_date (product_id, snapshot_date),
    INDEX idx_store_date (store_id, snapshot_date),
    INDEX idx_date_store_product (snapshot_date, store_id, product_id)
);

-- 7. PRICE HISTORY TABLE (For tracking price changes)
CREATE TABLE price_history (
    price_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(10) NOT NULL,
    effective_date DATE NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    discount_percentage DECIMAL(5,2) DEFAULT 0,
    
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    INDEX idx_product_date (product_id, effective_date)
);

CREATE TABLE inventory_raw_import (
    Date DATE,
    Store_ID VARCHAR(10),
    Product_ID VARCHAR(10),
    Category VARCHAR(50),
    Region VARCHAR(50),
    Inventory_Level INT,
    Units_Sold INT,
    Units_Ordered INT,
    Demand_Forecast DECIMAL(10,2),
    Price DECIMAL(10,2),
    Discount DECIMAL(5,2),
    Weather_Condition VARCHAR(50),
    Holiday_Promotion VARCHAR(5),
    Competitor_Pricing DECIMAL(10,2),
    Seasonality VARCHAR(20)
);

-- Load CSV data into raw table
-- Run this in terminal first: mysql --local-infile=1 -u root -p
-- Then in MySQL: SET GLOBAL local_infile=1;
LOAD DATA LOCAL INFILE './retail_store_inventory.csv' INTO TABLE inventory_raw_import FIELDS TERMINATED BY ',' ignore 1 lines;

-- Populate stores table
INSERT IGNORE INTO stores (store_id, region)
SELECT DISTINCT Store_ID, Region
FROM inventory_raw_import
WHERE Store_ID IS NOT NULL AND Region IS NOT NULL;

-- Populate products table
INSERT IGNORE INTO products (product_id, category,  base_price)
SELECT Product_ID, Category, AVG(Price)
FROM inventory_raw_import
WHERE Product_ID IS NOT NULL AND Category IS NOT NULL
GROUP BY Product_ID, Category;

-- Populate weather conditions table
INSERT IGNORE INTO weather_conditions (weather_condition)
SELECT DISTINCT Weather_Condition
FROM inventory_raw_import
WHERE Weather_Condition IS NOT NULL AND Weather_Condition != '';

-- Populate seasonality table
INSERT IGNORE INTO seasonality (season_name)
SELECT DISTINCT Seasonality
FROM inventory_raw_import
WHERE Seasonality IS NOT NULL AND Seasonality != '';

-- Populate promotions table
INSERT IGNORE INTO promotions (is_holiday_promotion, promotion_description)
VALUES 
(FALSE, 'No Promotion'),
(TRUE, 'Holiday Promotion');

-- Populate the main fact table (inventory_snapshots)
INSERT INTO inventory_snapshots (
    snapshot_date, store_id, product_id, inventory_level, units_sold, 
    units_ordered, demand_forecast, current_price, discount_percentage,
    competitor_pricing, weather_id, season_id, promotion_id
)
SELECT 
    i.Date,
    i.Store_ID,
    i.Product_ID,
    i.Inventory_Level,
    i.Units_Sold,
    i.Units_Ordered,
    i.Demand_Forecast,
    i.Price,
    i.Discount,
    i.Competitor_Pricing,
    w.weather_id,
    s.season_id,
    CASE WHEN i.Holiday_Promotion = '1' THEN 2 ELSE 1 END as promotion_id
FROM inventory_raw_import i
LEFT JOIN weather_conditions w ON i.Weather_Condition = w.weather_condition
LEFT JOIN seasonality s ON i.Seasonality = s.season_name
WHERE i.Store_ID IS NOT NULL 
  AND i.Product_ID IS NOT NULL 
  AND i.Date IS NOT NULL;

-- Populate price history table (for tracking price changes)
INSERT INTO price_history (product_id, effective_date, price, discount_percentage)
SELECT DISTINCT 
    Product_ID,
    Date,
    Price,
    Discount
FROM inventory_raw_import
WHERE Product_ID IS NOT NULL 
  AND Date IS NOT NULL 
  AND Price IS NOT NULL
ORDER BY Product_ID, Date;


-- Clean up raw import table after successful population
DROP TABLE inventory_raw_import;























-- Your query equivalent in normalized schema
SELECT 
    s.region,
    i.store_id,
    p.category,
    i.product_id,
    i.inventory_level AS Current_Stock,
    i.current_price,
    i.units_sold AS Last_Sales
FROM inventory_snapshots i
JOIN stores s ON i.store_id = s.store_id
JOIN products p ON i.product_id = p.product_id
WHERE (i.store_id, i.product_id, i.snapshot_date) IN (
    SELECT store_id, product_id, MAX(snapshot_date)
    FROM inventory_snapshots
    GROUP BY store_id, product_id
);


-- NON-SEASONAL REORDER POINTS 
WITH LatestDatePerProduct AS (
    SELECT store_id, product_id, MAX(snapshot_date) AS LatestDate
    FROM inventory_snapshots
    GROUP BY store_id, product_id
),

-- Get latest sales and inventory snapshot per product-store
LatestProductSales AS (
    SELECT 
        i.store_id,
        i.product_id,
        p.category,
        s.region,
        i.units_sold,
        i.inventory_level,
        i.current_price
    FROM inventory_snapshots i
    JOIN LatestDatePerProduct l
      ON i.store_id = l.store_id 
     AND i.product_id = l.product_id 
     AND i.snapshot_date = l.LatestDate
    JOIN stores s ON i.store_id = s.store_id
    JOIN products p ON i.product_id = p.product_id
),

-- Calculate average weekly sales and reorder point
ProductSalesSummary AS (
    SELECT 
        i.store_id,
        i.product_id,
        p.category,
        s.region,
        ROUND(AVG(i.units_sold), 2) AS Avg_Weekly_Sales,
        ROUND(STDDEV(i.units_sold), 2) AS StdDev_Sales,
        MAX(l.inventory_level) AS Current_Stock,
        MAX(l.current_price) AS Unit_Price
    FROM inventory_snapshots i
    JOIN LatestProductSales l
      ON i.store_id = l.store_id 
     AND i.product_id = l.product_id
    JOIN stores s ON i.store_id = s.store_id
    JOIN products p ON i.product_id = p.product_id
    GROUP BY i.store_id, i.product_id, p.category, s.region
)

-- Final output
SELECT 
    store_id,
    product_id,
    category,
    region,
    Current_Stock,
    Avg_Weekly_Sales,
    ROUND((Avg_Weekly_Sales + 1.5 * COALESCE(StdDev_Sales, 0))) AS Reorder_Point,
    CASE 
        WHEN Current_Stock <= 0 THEN 'Out of Stock'
        WHEN Current_Stock < (Avg_Weekly_Sales + 1.5 * COALESCE(StdDev_Sales, 0)) THEN 'Below Reorder Point'
        WHEN Current_Stock < (Avg_Weekly_Sales + 1.5 * COALESCE(StdDev_Sales, 0)) * 1.2 THEN 'Near Reorder Point'
        ELSE 'Adequate Stock'
    END AS Stock_Status,
    ROUND(Current_Stock / NULLIF(Avg_Weekly_Sales, 0), 1) AS Weeks_Of_Supply
FROM ProductSalesSummary
ORDER BY 
    CASE 
        WHEN Current_Stock <= 0 THEN 1
        WHEN Current_Stock < (Avg_Weekly_Sales + 1.5 * COALESCE(StdDev_Sales, 0)) THEN 2
        WHEN Current_Stock < (Avg_Weekly_Sales + 1.5 * COALESCE(StdDev_Sales, 0)) * 1.2 THEN 3
        ELSE 4
    END,
    Weeks_Of_Supply;

-- SEASONALITY ADJUSTED REORDER POINTS 
WITH LatestDatePerProduct AS (
    SELECT store_id, product_id, MAX(snapshot_date) AS LatestDate
    FROM inventory_snapshots
    GROUP BY store_id, product_id
),

LatestProductSales AS (
    SELECT 
        i.store_id, 
        i.product_id, 
        p.category, 
        s.region,
        i.units_sold, 
        i.inventory_level, 
        i.current_price,
        se.season_name AS seasonality
    FROM inventory_snapshots i
    JOIN LatestDatePerProduct l
      ON i.store_id = l.store_id 
     AND i.product_id = l.product_id 
     AND i.snapshot_date = l.LatestDate
    JOIN stores s ON i.store_id = s.store_id
    JOIN products p ON i.product_id = p.product_id
    LEFT JOIN seasonality se ON i.season_id = se.season_id
),

ProductSalesSummary AS (
    SELECT 
        i.store_id, 
        i.product_id, 
        p.category, 
        s.region,
        ROUND(AVG(i.units_sold)/7, 2) AS Avg_Daily_Sales, 
        ROUND(STDDEV(i.units_sold)/7, 2) AS StdDev_Sales,
        MAX(l.inventory_level) AS Current_Stock,
        MAX(l.current_price) AS Unit_Price,
        MAX(l.seasonality) AS Current_Season
    FROM inventory_snapshots i
    JOIN LatestProductSales l
      ON i.store_id = l.store_id 
     AND i.product_id = l.product_id
    JOIN stores s ON i.store_id = s.store_id
    JOIN products p ON i.product_id = p.product_id
    GROUP BY i.store_id, i.product_id, p.category, s.region
),

SeasonalFactors AS (
    SELECT 
        i.product_id,
        se.season_name AS seasonality,
        AVG(i.units_sold) / (
            SELECT AVG(i2.units_sold) 
            FROM inventory_snapshots i2 
            WHERE i2.product_id = i.product_id
        ) AS Seasonal_Factor
    FROM inventory_snapshots i
    LEFT JOIN seasonality se ON i.season_id = se.season_id
    WHERE se.season_name IS NOT NULL
    GROUP BY i.product_id, se.season_name
)

SELECT 
    p.store_id,
    p.product_id,
    p.category,
    p.region,
    p.Current_Stock,
    p.Avg_Daily_Sales,
    ROUND((7 * p.Avg_Daily_Sales) + (1.5 * COALESCE(p.StdDev_Sales, 0))) AS Standard_Reorder_Point,
    ROUND(((7 * p.Avg_Daily_Sales) + (1.5 * COALESCE(p.StdDev_Sales, 0))) * 
          COALESCE(s.Seasonal_Factor, 1)) AS Seasonal_Reorder_Point,
    COALESCE(s.Seasonal_Factor, 1) AS Applied_Seasonal_Factor,
    p.Current_Season,
    CASE 
        WHEN p.Current_Stock <= 0 THEN 'Out of Stock'
        WHEN p.Current_Stock < (((7 * p.Avg_Daily_Sales) + (1.5 * COALESCE(p.StdDev_Sales, 0))) * COALESCE(s.Seasonal_Factor, 1)) 
            THEN 'Below Seasonal Reorder Point'
        WHEN p.Current_Stock < (((7 * p.Avg_Daily_Sales) + (1.5 * COALESCE(p.StdDev_Sales, 0))) * COALESCE(s.Seasonal_Factor, 1)) * 1.2 
            THEN 'Near Seasonal Reorder Point'
        ELSE 'Adequate Stock'
    END AS Stock_Status,
    ROUND(p.Current_Stock / NULLIF(p.Avg_Daily_Sales * COALESCE(s.Seasonal_Factor, 1), 0), 1) AS Days_Of_Supply
FROM ProductSalesSummary p
LEFT JOIN SeasonalFactors s ON p.product_id = s.product_id AND p.Current_Season = s.seasonality
ORDER BY 
    CASE 
        WHEN p.Current_Stock <= 0 THEN 1
        WHEN p.Current_Stock < (((7 * p.Avg_Daily_Sales) + (1.5 * COALESCE(p.StdDev_Sales, 0))) * COALESCE(s.Seasonal_Factor, 1)) THEN 2
        WHEN p.Current_Stock < (((7 * p.Avg_Daily_Sales) + (1.5 * COALESCE(p.StdDev_Sales, 0))) * COALESCE(s.Seasonal_Factor, 1)) * 1.2 THEN 3
        ELSE 4
    END,
    Days_Of_Supply;


-- inventory turnover ratio
-- This query calculates the inventory turnover ratio for each product in each store
-- units based turn over ratio as cogs cannot be calculated


-- average turn over ratio by category (MONTHLY TURNOVER RATIO)
-- Average Monthly Turnover Ratio by Product Category
-- Using your working monthly turnover query as base
WITH monthly_turnover AS (
    SELECT 
        DATE_FORMAT(i.snapshot_date, '%Y-%m') AS YearMonth,
        i.store_id,
        s.region,
        i.product_id,
        p.category,
        SUM(i.units_sold) AS Total_Units_Sold,
        AVG(i.inventory_level) AS Avg_Inventory_Level,
        ROUND(SUM(i.units_sold) / NULLIF(AVG(i.inventory_level), 0), 2) AS Inventory_Turnover_Ratio
    FROM inventory_snapshots i
    JOIN stores s ON i.store_id = s.store_id
    JOIN products p ON i.product_id = p.product_id
    GROUP BY YearMonth, i.store_id, s.region, i.product_id, p.category
    HAVING Inventory_Turnover_Ratio IS NOT NULL
)
SELECT 
    category,
    COUNT(*) AS total_monthly_records,
    ROUND(AVG(Inventory_Turnover_Ratio), 2) AS avg_monthly_turnover_ratio,
    ROUND(MIN(Inventory_Turnover_Ratio), 2) AS min_turnover_ratio,
    ROUND(MAX(Inventory_Turnover_Ratio), 2) AS max_turnover_ratio,
    ROUND(STDDEV(Inventory_Turnover_Ratio), 2) AS turnover_stddev,
    CASE 
        WHEN AVG(Inventory_Turnover_Ratio) > 3 THEN 'High Turnover Category'
        WHEN AVG(Inventory_Turnover_Ratio) > 1 THEN 'Moderate Turnover Category'
        WHEN AVG(Inventory_Turnover_Ratio) > 0 THEN 'Low Turnover Category'
        ELSE 'No Sales Category'
    END AS category_performance
FROM monthly_turnover
GROUP BY category
ORDER BY avg_monthly_turnover_ratio DESC;


-- Inventory Turnover Ratio by Month, Store, and Product (MONTHLY TURNOVER RATIO)
-- INVENTORY TURNOVER RATIO WITH NORMALIZED TABLES
-- Includes store region and product category for better analysis
SELECT 
    DATE_FORMAT(i.snapshot_date, '%Y-%m') AS YearMonth,
    i.store_id,
    s.region,
    i.product_id,
    p.category,
    SUM(i.units_sold) AS Total_Units_Sold,
    AVG(i.inventory_level) AS Avg_Inventory_Level,
    ROUND(SUM(i.units_sold) / NULLIF(AVG(i.inventory_level), 0), 2) AS Inventory_Turnover_Ratio,
    -- Additional insights
    CASE 
        WHEN ROUND(SUM(i.units_sold) / NULLIF(AVG(i.inventory_level), 0), 2) > 3 THEN 'High Turnover'
        WHEN ROUND(SUM(i.units_sold) / NULLIF(AVG(i.inventory_level), 0), 2) > 1 THEN 'Moderate Turnover'
        WHEN ROUND(SUM(i.units_sold) / NULLIF(AVG(i.inventory_level), 0), 2) > 0 THEN 'Low Turnover'
        ELSE 'No Sales'
    END AS Turnover_Category,
    -- Days to sell inventory
    ROUND(30 / NULLIF(SUM(i.units_sold) / NULLIF(AVG(i.inventory_level), 0), 0), 1) AS Days_To_Sell_Inventory
FROM inventory_snapshots i
JOIN stores s ON i.store_id = s.store_id
JOIN products p ON i.product_id = p.product_id
GROUP BY YearMonth, i.store_id, s.region, i.product_id, p.category
ORDER BY YearMonth, s.region, i.store_id, p.category, i.product_id;

--  Stockout Risk Analysis (Days with Low or Zero Inventory)
-- Stockout Risk Analysis (Days with Low or Zero Inventory) - NORMALIZED TABLES
WITH ProductStats AS (
    SELECT 
        store_id,
        product_id,
        ROUND(AVG(units_sold) / 7, 2) AS Avg_Daily_Sales,
        ROUND(STDDEV(units_sold) / 7, 2) AS StdDev_Sales
    FROM inventory_snapshots
    GROUP BY store_id, product_id
),
ReorderPoints AS (
    SELECT 
        store_id,
        product_id,
        ROUND((7 * Avg_Daily_Sales) + (1.5 * COALESCE(StdDev_Sales, 0)), 2) AS Reorder_Point
    FROM ProductStats
),
LabeledData AS (
    SELECT 
        i.store_id,
        i.product_id,
        i.snapshot_date AS Date,
        i.inventory_level,
        rp.Reorder_Point,
        CASE 
            WHEN i.inventory_level <= rp.Reorder_Point THEN 1
            ELSE 0
        END AS Is_Low
    FROM inventory_snapshots i
    JOIN ReorderPoints rp 
      ON i.store_id = rp.store_id AND i.product_id = rp.product_id
)
SELECT 
    store_id AS Store_ID,
    product_id AS Product_ID,
    SUM(Is_Low) AS Low_Inventory_Days,
    COUNT(*) AS Total_Days,
    ROUND(SUM(Is_Low) / COUNT(*), 2) AS Risk_Ratio,
    CASE 
        WHEN ROUND(SUM(Is_Low) / COUNT(*), 2) >= 0.75 THEN 'High Risk'
        WHEN ROUND(SUM(Is_Low) / COUNT(*), 2) >= 0.4 THEN 'Moderate Risk'
        WHEN ROUND(SUM(Is_Low) / COUNT(*), 2) >= 0.2 THEN 'Low Risk'
        ELSE 'Safe'
    END AS Risk_Flag
FROM LabeledData
GROUP BY store_id, product_id
ORDER BY Risk_Ratio DESC;





