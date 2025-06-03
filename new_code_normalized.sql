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

-- STOCK LEVEL CALCULATIONS
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


-- NON-SEASONAL REORDER POINTS, Taking lead time as 1 week
-- NON‐SEASONAL REORDER POINTS (lead time = 1 week, daily -> weekly aggregation)
WITH

-- 1) Find latest date for each store+product
LatestDatePerProduct AS (
  SELECT 
    store_id,
    product_id,
    MAX(snapshot_date) AS latest_date
  FROM inventory_snapshots
  GROUP BY store_id, product_id
),

-- 2) Grab that one "latest daily" row to get current_stock & unit_price
LatestInventory AS (
  SELECT
    i.store_id,
    i.product_id,
    i.inventory_level AS current_stock,
    i.current_price   AS unit_price
  FROM inventory_snapshots i
  JOIN LatestDatePerProduct l
    ON i.store_id     = l.store_id
   AND i.product_id   = l.product_id
   AND i.snapshot_date = l.latest_date
),

-- 3) Aggregate daily rows into weekly buckets
WeeklySales AS (
  SELECT 
    store_id,
    product_id,
    DATE_FORMAT(snapshot_date, '%Y-%u') AS week_number,
    SUM(units_sold)   AS weekly_sales,
    AVG(inventory_level) AS weekly_avg_inventory
  FROM inventory_snapshots
  GROUP BY store_id, product_id, week_number
),

-- 4) Compute per‐product weekly stats
ProductStats AS (
  SELECT 
    w.store_id,
    w.product_id,
    ROUND(AVG(w.weekly_sales), 2)   AS avg_weekly_sales,
    ROUND(STDDEV(w.weekly_sales), 2) AS stddev_weekly_sales,
    -- lead_time_demand for 1 week
    ROUND(AVG(w.weekly_sales), 2)   AS lead_time_demand
  FROM WeeklySales w
  GROUP BY w.store_id, w.product_id
)

-- 5) Final reorder point join
SELECT 
  p.store_id,
  p.product_id,
  l.current_stock,
  p.avg_weekly_sales,
  -- ReorderPoint = average weekly demand + 1.5σ weekly
  ROUND(p.lead_time_demand + 1.5 * COALESCE(p.stddev_weekly_sales, 0)) AS reorder_point,
  CASE
    WHEN l.current_stock <= 0 THEN 'Out of Stock'
    WHEN l.current_stock < (p.lead_time_demand + 1.5 * COALESCE(p.stddev_weekly_sales, 0)) THEN 'Below Reorder Point'
    WHEN l.current_stock < (p.lead_time_demand + 1.5 * COALESCE(p.stddev_weekly_sales, 0)) * 1.2 THEN 'Near Reorder Point'
    ELSE 'Adequate Stock'
  END AS stock_status,
  ROUND(l.current_stock / NULLIF(p.avg_weekly_sales, 0), 1) AS weeks_of_supply
FROM ProductStats p
JOIN LatestInventory l
  ON p.store_id   = l.store_id
 AND p.product_id = l.product_id
ORDER BY
  CASE
    WHEN l.current_stock <= 0 THEN 1
    WHEN l.current_stock < (p.lead_time_demand + 1.5 * COALESCE(p.stddev_weekly_sales, 0)) THEN 2
    WHEN l.current_stock < (p.lead_time_demand + 1.5 * COALESCE(p.stddev_weekly_sales, 0)) * 1.2 THEN 3
    ELSE 4
  END,
  weeks_of_supply;


-- SEASONALITY ADJUSTED REORDER POINTS 
-- NON‐SEASONAL & SEASONAL REORDER POINTS (DAILY DATA → WEEKLY METRICS)
WITH

-- 1) Find the latest date for each (store, product)
LatestDatePerProduct AS (
  SELECT 
    store_id, 
    product_id, 
    MAX(snapshot_date) AS latest_date
  FROM inventory_snapshots
  GROUP BY store_id, product_id
),

-- 2) Using that date, pull exactly one “latest” row to get current_stock, current_price, and current_season
LatestSnapshot AS (
  SELECT
    i.store_id,
    i.product_id,
    i.inventory_level    AS current_stock,
    i.current_price      AS unit_price,
    se.season_name       AS current_season
  FROM inventory_snapshots AS i
  JOIN LatestDatePerProduct AS l
    ON i.store_id     = l.store_id
   AND i.product_id   = l.product_id
   AND i.snapshot_date = l.latest_date
  LEFT JOIN seasonality AS se
    ON i.season_id = se.season_id
),

-- 3) Aggregate all daily snapshots per (store, product) to get average daily sales and stddev
HistoricalStats AS (
  SELECT
    i.store_id,
    i.product_id,
    p.category,
    s.region,
    ROUND(AVG(i.units_sold), 2)   AS avg_daily_sales,
    ROUND(STDDEV(i.units_sold), 2) AS stddev_daily_sales
  FROM inventory_snapshots AS i
  JOIN products AS p
    ON i.product_id = p.product_id
  JOIN stores AS s
    ON i.store_id = s.store_id
  GROUP BY
    i.store_id,
    i.product_id,
    p.category,
    s.region
),

-- 4) For each product & season, compute the “seasonal factor” 
--    = (average units_sold in that season) / (overall average units_sold for that product)
SeasonalFactors AS (
  -- First, overall average per product:
  SELECT 
    product_id,
    ROUND(AVG(units_sold), 2) AS overall_avg_units_sold
  FROM inventory_snapshots
  GROUP BY product_id
),
SeasonalByProduct AS (
  -- Now average per (product, season)
  SELECT
    i.product_id,
    se.season_name   AS seasonality,
    ROUND(AVG(i.units_sold), 2) / NULLIF(sp.overall_avg_units_sold, 0) AS seasonal_factor
  FROM inventory_snapshots AS i
  JOIN LatestSnapshot AS ls
    ON i.store_id   = ls.store_id     -- only include rows for products that actually appear in LatestSnapshot
   AND i.product_id = ls.product_id
  LEFT JOIN seasonality AS se
    ON i.season_id = se.season_id
  JOIN SeasonalFactors AS sp
    ON i.product_id = sp.product_id
  WHERE se.season_name IS NOT NULL
  GROUP BY i.product_id, se.season_name
)

-- 5) Final result: join “historical stats” → “latest snapshot” → “seasonal factor”
SELECT 
  hs.store_id,
  hs.product_id,
  hs.category,
  hs.region,
  ls.current_stock,
  hs.avg_daily_sales,
  
  -- Weekly metrics (multiply daily by 7)
  ROUND(hs.avg_daily_sales * 7, 2) AS avg_weekly_sales,
  ROUND(hs.stddev_daily_sales * 7, 2) AS stddev_weekly_sales,
  
  -- Standard reorder point (no seasonality adjustment)
  ROUND( (hs.avg_daily_sales * 7) + (1.5 * COALESCE(hs.stddev_daily_sales * 7, 0)) ) AS standard_reorder_point,
  
  -- Lookup the seasonal factor for this product’s current season (default = 1 if no match)
  COALESCE(sb.seasonal_factor, 1) AS applied_seasonal_factor,
  
  -- Seasonal reorder point = (weekly demand + 1.5σ) * seasonal_factor
  ROUND( ((hs.avg_daily_sales * 7) + (1.5 * COALESCE(hs.stddev_daily_sales * 7, 0))) 
         * COALESCE(sb.seasonal_factor, 1) ) AS seasonal_reorder_point,
  
  ls.current_season AS seasonality,
  
  -- Stock status (seasonal)
  CASE 
    WHEN ls.current_stock <= 0 THEN 'Out of Stock'
    WHEN ls.current_stock < (((hs.avg_daily_sales * 7) + (1.5 * COALESCE(hs.stddev_daily_sales * 7, 0))) 
                              * COALESCE(sb.seasonal_factor, 1)) 
      THEN 'Below Seasonal Reorder Point'
    WHEN ls.current_stock < (((hs.avg_daily_sales * 7) + (1.5 * COALESCE(hs.stddev_daily_sales * 7, 0))) 
                              * COALESCE(sb.seasonal_factor, 1) ) * 1.2 
      THEN 'Near Seasonal Reorder Point'
    ELSE 'Adequate Stock'
  END AS stock_status,
  
  -- Days of supply (seasonally adjusted): current_stock ÷ (daily_sales × seasonal_factor)
  ROUND( ls.current_stock 
       / NULLIF( hs.avg_daily_sales * COALESCE(sb.seasonal_factor, 1), 0 ), 
       1
  ) AS days_of_supply

FROM HistoricalStats AS hs
JOIN LatestSnapshot    AS ls
  ON hs.store_id   = ls.store_id
 AND hs.product_id = ls.product_id
LEFT JOIN SeasonalByProduct AS sb
  ON hs.product_id = sb.product_id
 AND ls.current_season = sb.seasonality

ORDER BY 
  CASE 
    WHEN ls.current_stock <= 0 THEN 1
    WHEN ls.current_stock < (((hs.avg_daily_sales * 7) + (1.5 * COALESCE(hs.stddev_daily_sales * 7, 0))) 
                              * COALESCE(sb.seasonal_factor, 1)) THEN 2
    WHEN ls.current_stock < (((hs.avg_daily_sales * 7) + (1.5 * COALESCE(hs.stddev_daily_sales * 7, 0))) 
                              * COALESCE(sb.seasonal_factor, 1)) * 1.2 THEN 3
    ELSE 4
  END,
  days_of_supply;



-- inventory turnover ratio
-- This query calculates the inventory turnover ratio for each product in each store
-- units based turn over ratio as cogs cannot be calculated


-- average turn over ratio by category (MONTHLY TURNOVER RATIO)
-- Average Monthly Turnover Ratio by Product Category
-- Using your working monthly turnover query as base
-- Step 1: Compute monthly turnover per (store, product)
WITH monthly_turnover AS (
    SELECT
        DATE_FORMAT(i.snapshot_date, '%Y-%m') AS YearMonth,
        i.product_id,
        ROUND(
            SUM(i.units_sold) 
            / NULLIF(AVG(i.inventory_level), 0),
            2
        ) AS monthly_turnover_ratio
    FROM inventory_snapshots AS i
    GROUP BY
        YearMonth,
        i.store_id,
        i.product_id
)

-- Step 2: Aggregate to get the average monthly turnover per product
SELECT
    m.product_id,
    p.category,
    ROUND(AVG(m.monthly_turnover_ratio), 2) AS avg_monthly_turnover
FROM monthly_turnover AS m
JOIN products AS p
  ON m.product_id = p.product_id
GROUP BY
    m.product_id,
    p.category
ORDER BY
    avg_monthly_turnover DESC;




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
        -- Average daily sales (no division by 7)
        ROUND(AVG(units_sold), 2)   AS Avg_Daily_Sales,
        ROUND(STDDEV(units_sold), 2) AS StdDev_Daily_Sales
    FROM inventory_snapshots
    GROUP BY store_id, product_id
),
ReorderPoints AS (
    SELECT 
        store_id,
        product_id,
        -- Convert daily to weekly: (Avg_Daily_Sales * 7) + 1.5 * (StdDev_Daily_Sales * 7)
        ROUND(
            (Avg_Daily_Sales * 7) 
            + (1.5 * COALESCE(StdDev_Daily_Sales, 0) * 7),
            2
        ) AS Reorder_Point
    FROM ProductStats
),
LabeledData AS (
    SELECT 
        i.store_id,
        i.product_id,
        i.snapshot_date        AS Date,
        i.inventory_level,
        rp.Reorder_Point,
        CASE 
            WHEN i.inventory_level <= rp.Reorder_Point THEN 1
            ELSE 0
        END AS Is_Low
    FROM inventory_snapshots i
    JOIN ReorderPoints rp 
      ON i.store_id   = rp.store_id 
     AND i.product_id = rp.product_id
)
SELECT 
    store_id   AS Store_ID,
    product_id AS Product_ID,
    SUM(Is_Low)   AS Low_Inventory_Days,
    COUNT(*)      AS Total_Days,
    ROUND(SUM(Is_Low) / COUNT(*), 2) AS Risk_Ratio,
    CASE 
        WHEN ROUND(SUM(Is_Low) / COUNT(*), 2) >= 0.75 THEN 'High Risk'
        WHEN ROUND(SUM(Is_Low) / COUNT(*), 2) >= 0.4  THEN 'Moderate Risk'
        WHEN ROUND(SUM(Is_Low) / COUNT(*), 2) >= 0.2  THEN 'Low Risk'
        ELSE 'Safe'
    END AS Risk_Flag
FROM LabeledData
GROUP BY store_id, product_id
ORDER BY Risk_Ratio DESC;



---SUMMARY REPORTS with some kpi's----------------------------------------------------------------------------------------------------------- 


--1.INVENTORY AGE 
--This measures how long inventory has been sitting in the store since last major replenishment.
--Done by finding the date went last significant increase was seen in inventory.
--Then calculate number of days that have passed since then, threshold to consider as significant increase is 20%( factor of 1.2),

WITH InventoryIncreases AS (
  SELECT 
    store_id,
    product_id,
    snapshot_date,
    inventory_level,
    LAG(inventory_level) 
      OVER (PARTITION BY store_id, product_id 
            ORDER BY snapshot_date) AS Prev_Inventory
  FROM inventory_snapshots
), 
ReplenishmentDates AS (
  SELECT 
    store_id,
    product_id,
    snapshot_date AS Replenished_On
  FROM InventoryIncreases
  WHERE inventory_level > Prev_Inventory * 1.2
),
LatestStock AS (
  SELECT 
    store_id,
    product_id,
    MAX(snapshot_date) AS Latest_Date
  FROM inventory_snapshots
  GROUP BY store_id, product_id
),
InventoryAge AS (
  SELECT 
    l.store_id,
    l.product_id,
    MAX(r.Replenished_On) AS Last_Replenishment_Date,
    l.Latest_Date,
    DATEDIFF(l.Latest_Date, MAX(r.Replenished_On)) AS Inventory_Age_Days
  FROM LatestStock l
  JOIN ReplenishmentDates r 
    ON l.store_id   = r.store_id 
   AND l.product_id = r.product_id
  WHERE r.Replenished_On <= l.Latest_Date
  GROUP BY l.store_id, l.product_id, l.Latest_Date
)
SELECT 
  store_id   AS Store_ID,
  product_id AS Product_ID,
  DATE_FORMAT(Last_Replenishment_Date, '%Y-%m-%d') AS Last_Replenishment_Date,
  DATE_FORMAT(Latest_Date,              '%Y-%m-%d') AS Latest_Date,
  Inventory_Age_Days
FROM InventoryAge
ORDER BY Inventory_Age_Days DESC;



--2.STOCKOUT RATE---------------------------------------------------------------------
----How often a product was out of stock in terms of % available days.
-- Stockout Analysis with Store Region and Product Category
-- 2. STOCKOUT RATE
-- How often a product was out of stock, by store and category
SELECT 
    s.region           AS Store_Region,
    i.store_id         AS Store_ID,
    p.category         AS Product_Category,
    i.product_id       AS Product_ID,
    COUNT(CASE WHEN i.inventory_level = 0 THEN 1 END) AS Stockout_Days,
    COUNT(*)                                    AS Total_Days,
    ROUND(
      COUNT(CASE WHEN i.inventory_level = 0 THEN 1 END)
      / COUNT(*) * 100,
      2
    ) AS Stockout_Rate_Percent,
    CASE 
        WHEN COUNT(CASE WHEN i.inventory_level = 0 THEN 1 END) / COUNT(*) >= 0.3 THEN 'High Risk'
        WHEN COUNT(CASE WHEN i.inventory_level = 0 THEN 1 END) / COUNT(*) >= 0.1 THEN 'Moderate Risk'
        WHEN COUNT(CASE WHEN i.inventory_level = 0 THEN 1 END) / COUNT(*) > 0   THEN 'Low Risk'
        ELSE 'No Stockouts'
    END AS Risk_Category
FROM inventory_snapshots i
JOIN stores   s ON i.store_id    = s.store_id
JOIN products p ON i.product_id  = p.product_id
GROUP BY 
    s.region, 
    i.store_id, 
    p.category, 
    i.product_id
ORDER BY Stockout_Rate_Percent DESC;


-- Note: This query calculates the stockout rate for each product in each store,
-- grouped by store region and product category.
--we observe no product in any of the stores ever had a stockout on any given day.


--3.SELL THROUGH RATE---------------------------------------------------------------
--a) Sell through rates by region and month based on weighted average inventory
WITH DailyStats AS (
    SELECT 
        s.region                      AS Region,
        DATE(i.snapshot_date)         AS Day,
        DATE_FORMAT(i.snapshot_date, '%Y-%m') AS Month,
        SUM(i.units_sold)   AS Daily_Sales,
        SUM(i.inventory_level) AS Daily_Inventory
    FROM inventory_snapshots i
    JOIN stores s 
      ON i.store_id = s.store_id
    GROUP BY 
        s.region, 
        DATE(i.snapshot_date), 
        DATE_FORMAT(i.snapshot_date, '%Y-%m')
),
WeightedInventory AS (
    SELECT 
        Month,
        Region,
        ROUND(AVG(Daily_Inventory), 2) AS Weighted_Avg_Inventory,
        SUM(Daily_Sales)       AS Total_Units_Sold
    FROM DailyStats
    GROUP BY Region, Month
)
SELECT 
    Month,
    Region,
    Total_Units_Sold,
    Weighted_Avg_Inventory,
    ROUND(
      Total_Units_Sold 
      / NULLIF(Total_Units_Sold + Weighted_Avg_Inventory, 0) 
      * 100, 
      2
    ) AS Sell_Through_Rate_Percent
FROM WeightedInventory
ORDER BY Month, Region;


--b) Sell through rates by stores and month
SELECT 
    DATE_FORMAT(i.snapshot_date, '%Y-%m') AS Month,
    s.region                        AS Region,
    i.store_id                      AS Store_ID,
    SUM(i.units_sold)               AS Total_Units_Sold,
    MAX(i.inventory_level)          AS Ending_Inventory,
    ROUND(
      SUM(i.units_sold) 
      / NULLIF(SUM(i.units_sold) + MAX(i.inventory_level), 0) 
      * 100, 
      2
    ) AS Sell_Through_Rate_Percent
FROM inventory_snapshots i
JOIN stores s ON i.store_id = s.store_id
GROUP BY 
    Month, 
    Region, 
    Store_ID
ORDER BY Month, Region, Store_ID;


--4.AVERAGE STOCK LEVEL-----------------------------------------------------------
SELECT 
    s.region                      AS Region,
    p.category                    AS Category,
    DATE_FORMAT(i.snapshot_date, '%Y-%m') AS YearMonth,
    ROUND(AVG(i.inventory_level), 2)      AS Avg_Stock_Level
FROM inventory_snapshots i
JOIN stores s   ON i.store_id    = s.store_id
JOIN products p ON i.product_id  = p.product_id
GROUP BY 
    s.region, 
    p.category, 
    YearMonth
ORDER BY Region, Category, YearMonth;


--5. DEAD STOCK ANALYSIS--------------------------------------------------------------
SELECT
    i.store_id   AS Store_ID,
    i.product_id AS Product_ID,
    COUNT(*)                          AS Total_Days,
    SUM(CASE WHEN i.units_sold = 0 THEN 1 ELSE 0 END)   AS Zero_Sales_Days,
    ROUND(
      SUM(CASE WHEN i.units_sold = 0 THEN 1 ELSE 0 END) 
      / COUNT(*) * 100, 
      2
    ) AS Dead_Stock_Rate_Percent
FROM inventory_snapshots i
GROUP BY i.store_id, i.product_id
HAVING Dead_Stock_Rate_Percent > 0
ORDER BY Dead_Stock_Rate_Percent DESC;





