-- Enhanced normalized schema handling multiple regions per store
CREATE DATABASE IF NOT EXISTS cna;
USE cna;

-- 1. STORES TABLE (Base store information)
CREATE TABLE stores (
    store_id VARCHAR(10) PRIMARY KEY,
    store_name VARCHAR(100),
    INDEX idx_store_id (store_id)
);

-- 2. REGIONS TABLE 
CREATE TABLE regions (
    region_id INT AUTO_INCREMENT PRIMARY KEY,
    region_name VARCHAR(50) UNIQUE NOT NULL,
    INDEX idx_region_name (region_name)
);

-- 3. STORE_REGIONS TABLE (Many-to-Many relationship)
-- This handles the fact that one store can operate in multiple regions
CREATE TABLE store_regions (
    store_region_id INT AUTO_INCREMENT PRIMARY KEY,
    store_id VARCHAR(10) NOT NULL,
    region_id INT NOT NULL,
    is_primary_region BOOLEAN DEFAULT FALSE,
    
    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (region_id) REFERENCES regions(region_id),
    UNIQUE KEY unique_store_region (store_id, region_id),
    INDEX idx_store_id (store_id),
    INDEX idx_region_id (region_id)
);

-- 4. PRODUCTS TABLE
CREATE TABLE products (
    product_id VARCHAR(10) PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    base_price DECIMAL(10,2) NOT NULL,
    INDEX idx_category (category)
);

-- 5. WEATHER CONDITIONS TABLE
CREATE TABLE weather_conditions (
    weather_id INT AUTO_INCREMENT PRIMARY KEY,
    weather_condition VARCHAR(50) UNIQUE NOT NULL
);

-- 6. SEASONALITY TABLE
CREATE TABLE seasonality (
    season_id INT AUTO_INCREMENT PRIMARY KEY,
    season_name VARCHAR(20) UNIQUE NOT NULL
);

-- 7. PROMOTIONS TABLE
CREATE TABLE promotions (
    promotion_id INT AUTO_INCREMENT PRIMARY KEY,
    is_holiday_promotion BOOLEAN NOT NULL DEFAULT FALSE,
    promotion_description VARCHAR(100)
);

-- 8. INVENTORY SNAPSHOTS TABLE (Main fact table)
-- This represents inventory for a specific product at a specific store in a specific region
CREATE TABLE inventory_snapshots (
    snapshot_id INT AUTO_INCREMENT PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    store_id VARCHAR(10) NOT NULL,
    region_id INT NOT NULL,
    product_id VARCHAR(10) NOT NULL,
    inventory_level INT NOT NULL,
    units_sold INT NOT NULL DEFAULT 0,
    units_ordered INT NOT NULL DEFAULT 0,
    demand_forecast DECIMAL(10,2),
    current_price DECIMAL(10,2) NOT NULL,
    discount_percentage DECIMAL(5,2) DEFAULT 0,
    competitor_pricing DECIMAL(10,2),
    weather_id INT,
    season_id INT,
    promotion_id INT,
    
    -- Foreign Key Constraints
    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (region_id) REFERENCES regions(region_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (weather_id) REFERENCES weather_conditions(weather_id),
    FOREIGN KEY (season_id) REFERENCES seasonality(season_id),
    FOREIGN KEY (promotion_id) REFERENCES promotions(promotion_id),
    
    -- Unique constraint: one snapshot per date-store-region-product combination
    UNIQUE KEY unique_snapshot (snapshot_date, store_id, region_id, product_id),
    
    -- Performance indexes
    INDEX idx_date (snapshot_date),
    INDEX idx_store_region_product_date (store_id, region_id, product_id, snapshot_date),
    INDEX idx_product_date (product_id, snapshot_date),
    INDEX idx_store_date (store_id, snapshot_date),
    INDEX idx_region_date (region_id, snapshot_date),
    INDEX idx_date_store_region_product (snapshot_date, store_id, region_id, product_id)
);

-- 9. PRICE HISTORY TABLE
CREATE TABLE price_history (
    price_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(10) NOT NULL,
    region_id INT NOT NULL,
    effective_date DATE NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    discount_percentage DECIMAL(5,2) DEFAULT 0,
    
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (region_id) REFERENCES regions(region_id),
    INDEX idx_product_region_date (product_id, region_id, effective_date)
);

-- RAW IMPORT TABLE
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
LOAD DATA LOCAL INFILE './retail_store_inventory.csv' 
INTO TABLE inventory_raw_import 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(Date, Store_ID, Product_ID, Category, Region, Inventory_Level, 
 Units_Sold, Units_Ordered, Demand_Forecast, Price, Discount, 
 Weather_Condition, Holiday_Promotion, Competitor_Pricing, Seasonality);

-- DATA POPULATION PROCESS

-- 1. Populate stores table (get unique stores)
INSERT IGNORE INTO stores (store_id, store_name)
SELECT DISTINCT Store_ID, CONCAT('Store ', Store_ID)
FROM inventory_raw_import
WHERE Store_ID IS NOT NULL;

-- 2. Populate regions table
INSERT IGNORE INTO regions (region_name)
SELECT DISTINCT Region
FROM inventory_raw_import
WHERE Region IS NOT NULL AND Region != '';

-- 3. Populate store_regions relationship table
INSERT IGNORE INTO store_regions (store_id, region_id, is_primary_region)
SELECT DISTINCT 
    i.Store_ID,
    r.region_id,
    CASE 
        WHEN ROW_NUMBER() OVER (PARTITION BY i.Store_ID ORDER BY COUNT(*) DESC) = 1 
        THEN TRUE 
        ELSE FALSE 
    END as is_primary_region
FROM inventory_raw_import i
JOIN regions r ON i.Region = r.region_name
WHERE i.Store_ID IS NOT NULL AND i.Region IS NOT NULL
GROUP BY i.Store_ID, r.region_id, r.region_name
ORDER BY i.Store_ID, COUNT(*) DESC;

-- 4. Populate products table
INSERT IGNORE INTO products (product_id, category, base_price)
SELECT Product_ID, Category, ROUND(AVG(Price), 2)
FROM inventory_raw_import
WHERE Product_ID IS NOT NULL AND Category IS NOT NULL AND Price IS NOT NULL
GROUP BY Product_ID, Category;

-- 5. Populate weather conditions table
INSERT IGNORE INTO weather_conditions (weather_condition)
SELECT DISTINCT Weather_Condition
FROM inventory_raw_import
WHERE Weather_Condition IS NOT NULL AND Weather_Condition != '';

-- 6. Populate seasonality table
INSERT IGNORE INTO seasonality (season_name)
SELECT DISTINCT Seasonality
FROM inventory_raw_import
WHERE Seasonality IS NOT NULL AND Seasonality != '';

-- 7. Populate promotions table
INSERT IGNORE INTO promotions (is_holiday_promotion, promotion_description)
VALUES 
(FALSE, 'No Promotion'),
(TRUE, 'Holiday Promotion');

-- 8. Populate the main fact table (inventory_snapshots)
INSERT INTO inventory_snapshots (
    snapshot_date, store_id, region_id, product_id, inventory_level, units_sold, 
    units_ordered, demand_forecast, current_price, discount_percentage,
    competitor_pricing, weather_id, season_id, promotion_id
)
SELECT 
    i.Date,
    i.Store_ID,
    r.region_id,
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
JOIN regions r ON i.Region = r.region_name
LEFT JOIN weather_conditions w ON i.Weather_Condition = w.weather_condition
LEFT JOIN seasonality s ON i.Seasonality = s.season_name
WHERE i.Store_ID IS NOT NULL 
  AND i.Product_ID IS NOT NULL 
  AND i.Date IS NOT NULL
  AND i.Region IS NOT NULL;

-- 9. Populate price history table
INSERT INTO price_history (product_id, region_id, effective_date, price, discount_percentage)
SELECT DISTINCT 
    i.Product_ID,
    r.region_id,
    i.Date,
    i.Price,
    i.Discount
FROM inventory_raw_import i
JOIN regions r ON i.Region = r.region_name
WHERE i.Product_ID IS NOT NULL 
  AND i.Date IS NOT NULL 
  AND i.Price IS NOT NULL
  AND i.Region IS NOT NULL
ORDER BY i.Product_ID, r.region_id, i.Date;

-- Clean up raw import table
DROP TABLE inventory_raw_import;

-- ===========================================
-- UPDATED ANALYSIS QUERIES
-- ===========================================

-- 1. CURRENT STOCK LEVELS (Updated to handle store-region relationships)
SELECT 
    s.store_id,
    r.region_name,
    p.category,
    i.product_id,
    i.inventory_level AS Current_Stock,
    i.current_price,
    i.units_sold AS Last_Sales
FROM inventory_snapshots i
JOIN stores s ON i.store_id = s.store_id
JOIN regions r ON i.region_id = r.region_id
JOIN products p ON i.product_id = p.product_id
WHERE (i.store_id, i.region_id, i.product_id, i.snapshot_date) IN (
    SELECT store_id, region_id, product_id, MAX(snapshot_date)
    FROM inventory_snapshots
    GROUP BY store_id, region_id, product_id
)
ORDER BY s.store_id, r.region_name, p.category, i.product_id;

-- 2. REORDER POINTS ANALYSIS (Updated for store-region structure)
WITH 
-- Get latest inventory status for each store-region-product combination
LatestInventory AS (
  SELECT
    i.store_id,
    i.region_id,
    i.product_id,
    i.inventory_level AS current_stock,
    i.current_price AS unit_price
  FROM inventory_snapshots i
  INNER JOIN (
    SELECT store_id, region_id, product_id, MAX(snapshot_date) as max_date
    FROM inventory_snapshots
    GROUP BY store_id, region_id, product_id
  ) latest ON i.store_id = latest.store_id 
           AND i.region_id = latest.region_id
           AND i.product_id = latest.product_id 
           AND i.snapshot_date = latest.max_date
),

-- Calculate daily sales statistics per store-region-product
DailySalesStats AS (
  SELECT 
    store_id,
    region_id,
    product_id,
    AVG(units_sold) AS avg_daily_sales,
    STDDEV(units_sold) AS stddev_daily_sales,
    COUNT(*) AS observation_days
  FROM inventory_snapshots
  WHERE units_sold >= 0
  GROUP BY store_id, region_id, product_id
  HAVING COUNT(*) >= 7  -- At least a week of data
)

-- Final reorder point calculation
SELECT 
  s.store_id,
  r.region_name,
  p.category,
  l.product_id,
  l.current_stock,
  ROUND(stats.avg_daily_sales, 2) AS avg_daily_sales,
  ROUND(stats.avg_daily_sales * 7, 2) AS avg_weekly_sales,
  ROUND(1.5 * COALESCE(stats.stddev_daily_sales, 0) * SQRT(7), 2) AS safety_stock,
  ROUND((stats.avg_daily_sales * 7) + (1.5 * COALESCE(stats.stddev_daily_sales, 0) * SQRT(7)), 0) AS reorder_point,
  
  CASE
    WHEN l.current_stock <= 0 THEN 'Out of Stock'
    WHEN l.current_stock < ((stats.avg_daily_sales * 7) + (1.5 * COALESCE(stats.stddev_daily_sales, 0) * SQRT(7))) THEN 'Below Reorder Point'
    WHEN l.current_stock < ((stats.avg_daily_sales * 7) + (1.5 * COALESCE(stats.stddev_daily_sales, 0) * SQRT(7))) * 1.2 THEN 'Near Reorder Point'
    ELSE 'Adequate Stock'
  END AS stock_status,
  
  CASE 
    WHEN stats.avg_daily_sales > 0 THEN ROUND(l.current_stock / stats.avg_daily_sales, 1)
    ELSE NULL
  END AS days_of_supply

FROM LatestInventory l
JOIN DailySalesStats stats ON l.store_id = stats.store_id 
                           AND l.region_id = stats.region_id 
                           AND l.product_id = stats.product_id
JOIN stores s ON l.store_id = s.store_id
JOIN regions r ON l.region_id = r.region_id
JOIN products p ON l.product_id = p.product_id
ORDER BY
  CASE
    WHEN l.current_stock <= 0 THEN 1
    WHEN l.current_stock < ((stats.avg_daily_sales * 7) + (1.5 * COALESCE(stats.stddev_daily_sales, 0) * SQRT(7))) THEN 2
    WHEN l.current_stock < ((stats.avg_daily_sales * 7) + (1.5 * COALESCE(stats.stddev_daily_sales, 0) * SQRT(7))) * 1.2 THEN 3
    ELSE 4
  END,
  s.store_id, r.region_name, l.product_id;

-- 3. SEASONAL REORDER POINTS (Updated for store-region structure)
WITH
-- Latest snapshot with current season
LatestSnapshot AS (
  SELECT
    i.store_id,
    i.region_id,
    i.product_id,
    i.inventory_level AS current_stock,
    i.current_price AS unit_price,
    se.season_name AS current_season
  FROM inventory_snapshots i
  INNER JOIN (
    SELECT store_id, region_id, product_id, MAX(snapshot_date) as max_date
    FROM inventory_snapshots
    GROUP BY store_id, region_id, product_id
  ) latest ON i.store_id = latest.store_id 
           AND i.region_id = latest.region_id
           AND i.product_id = latest.product_id 
           AND i.snapshot_date = latest.max_date
  LEFT JOIN seasonality se ON i.season_id = se.season_id
),

-- Historical sales statistics
HistoricalStats AS (
  SELECT
    i.store_id,
    i.region_id,
    i.product_id,
    AVG(i.units_sold) AS avg_daily_sales,
    STDDEV(i.units_sold) AS stddev_daily_sales
  FROM inventory_snapshots i
  GROUP BY i.store_id, i.region_id, i.product_id
),

-- Seasonal factors by product and region
SeasonalFactors AS (
  SELECT 
    i.product_id,
    i.region_id,
    se.season_name,
    AVG(i.units_sold) / NULLIF(overall.overall_avg, 0) AS seasonal_factor
  FROM inventory_snapshots i
  JOIN seasonality se ON i.season_id = se.season_id
  JOIN (
    SELECT product_id, region_id, AVG(units_sold) AS overall_avg
    FROM inventory_snapshots
    GROUP BY product_id, region_id
  ) overall ON i.product_id = overall.product_id AND i.region_id = overall.region_id
  GROUP BY i.product_id, i.region_id, se.season_name
)

-- Final seasonal reorder point calculation
SELECT 
  s.store_id,
  r.region_name,
  p.category,
  ls.product_id,
  ls.current_stock,
  ROUND(hs.avg_daily_sales, 2) AS avg_daily_sales,
  ROUND(hs.avg_daily_sales * 7, 2) AS avg_weekly_sales,
  ROUND((hs.avg_daily_sales * 7) + (1.5 * COALESCE(hs.stddev_daily_sales * 7, 0)), 0) AS standard_reorder_point,
  COALESCE(sf.seasonal_factor, 1) AS seasonal_factor,
  ROUND(((hs.avg_daily_sales * 7) + (1.5 * COALESCE(hs.stddev_daily_sales * 7, 0))) * COALESCE(sf.seasonal_factor, 1), 0) AS seasonal_reorder_point,
  ls.current_season,
  
  CASE 
    WHEN ls.current_stock <= 0 THEN 'Out of Stock'
    WHEN ls.current_stock < (((hs.avg_daily_sales * 7) + (1.5 * COALESCE(hs.stddev_daily_sales * 7, 0))) * COALESCE(sf.seasonal_factor, 1)) THEN 'Below Seasonal Reorder Point'
    WHEN ls.current_stock < (((hs.avg_daily_sales * 7) + (1.5 * COALESCE(hs.stddev_daily_sales * 7, 0))) * COALESCE(sf.seasonal_factor, 1)) * 1.2 THEN 'Near Seasonal Reorder Point'
    ELSE 'Adequate Stock'
  END AS stock_status,
  
  ROUND(ls.current_stock / NULLIF(hs.avg_daily_sales * COALESCE(sf.seasonal_factor, 1), 0), 1) AS days_of_supply

FROM LatestSnapshot ls
JOIN HistoricalStats hs ON ls.store_id = hs.store_id 
                        AND ls.region_id = hs.region_id 
                        AND ls.product_id = hs.product_id
LEFT JOIN SeasonalFactors sf ON ls.product_id = sf.product_id 
                             AND ls.region_id = sf.region_id 
                             AND ls.current_season = sf.season_name
JOIN stores s ON ls.store_id = s.store_id
JOIN regions r ON ls.region_id = r.region_id
JOIN products p ON ls.product_id = p.product_id

ORDER BY 
  CASE 
    WHEN ls.current_stock <= 0 THEN 1
    WHEN ls.current_stock < (((hs.avg_daily_sales * 7) + (1.5 * COALESCE(hs.stddev_daily_sales * 7, 0))) * COALESCE(sf.seasonal_factor, 1)) THEN 2
    WHEN ls.current_stock < (((hs.avg_daily_sales * 7) + (1.5 * COALESCE(hs.stddev_daily_sales * 7, 0))) * COALESCE(sf.seasonal_factor, 1)) * 1.2 THEN 3
    ELSE 4
  END,
  s.store_id, r.region_name, days_of_supply;

-- 4. MONTHLY INVENTORY TURNOVER (Updated)
SELECT 
    DATE_FORMAT(i.snapshot_date, '%Y-%m') AS YearMonth,
    s.store_id,
    r.region_name,
    i.product_id,
    p.category,
    SUM(i.units_sold) AS Total_Units_Sold,
    AVG(i.inventory_level) AS Avg_Inventory_Level,
    ROUND(SUM(i.units_sold) / NULLIF(AVG(i.inventory_level), 0), 2) AS Inventory_Turnover_Ratio,
    CASE 
        WHEN ROUND(SUM(i.units_sold) / NULLIF(AVG(i.inventory_level), 0), 2) > 3 THEN 'High Turnover'
        WHEN ROUND(SUM(i.units_sold) / NULLIF(AVG(i.inventory_level), 0), 2) > 1 THEN 'Moderate Turnover'
        WHEN ROUND(SUM(i.units_sold) / NULLIF(AVG(i.inventory_level), 0), 2) > 0 THEN 'Low Turnover'
        ELSE 'No Sales'
    END AS Turnover_Category,
    ROUND(30 / NULLIF(SUM(i.units_sold) / NULLIF(AVG(i.inventory_level), 0), 0), 1) AS Days_To_Sell_Inventory
FROM inventory_snapshots i
JOIN stores s ON i.store_id = s.store_id
JOIN regions r ON i.region_id = r.region_id
JOIN products p ON i.product_id = p.product_id
GROUP BY YearMonth, s.store_id, r.region_name, i.product_id, p.category
ORDER BY YearMonth, s.store_id, r.region_name, p.category, i.product_id;

-- 5. STOCKOUT RISK ANALYSIS (Updated)
WITH ProductStats AS (
    SELECT 
        store_id,
        region_id,
        product_id,
        AVG(units_sold) AS avg_daily_sales,
        STDDEV(units_sold) AS stddev_daily_sales
    FROM inventory_snapshots
    GROUP BY store_id, region_id, product_id
),
ReorderPoints AS (
    SELECT 
        store_id,
        region_id,
        product_id,
        ROUND((avg_daily_sales * 7) + (1.5 * COALESCE(stddev_daily_sales, 0) * 7), 2) AS reorder_point
    FROM ProductStats
),
LabeledData AS (
    SELECT 
        i.store_id,
        i.region_id,
        i.product_id,
        i.snapshot_date,
        i.inventory_level,
        rp.reorder_point,
        CASE 
            WHEN i.inventory_level <= rp.reorder_point THEN 1
            ELSE 0
        END AS is_low
    FROM inventory_snapshots i
    JOIN ReorderPoints rp ON i.store_id = rp.store_id 
                          AND i.region_id = rp.region_id 
                          AND i.product_id = rp.product_id
)
SELECT 
    s.store_id,
    r.region_name,
    p.category,
    ld.product_id,
    SUM(ld.is_low) AS low_inventory_days,
    COUNT(*) AS total_days,
    ROUND(SUM(ld.is_low) / COUNT(*), 2) AS risk_ratio,
    CASE 
        WHEN ROUND(SUM(ld.is_low) / COUNT(*), 2) >= 0.75 THEN 'High Risk'
        WHEN ROUND(SUM(ld.is_low) / COUNT(*), 2) >= 0.4  THEN 'Moderate Risk'
        WHEN ROUND(SUM(ld.is_low) / COUNT(*), 2) >= 0.2  THEN 'Low Risk'
        ELSE 'Safe'
    END AS risk_flag
FROM LabeledData ld
JOIN stores s ON ld.store_id = s.store_id
JOIN regions r ON ld.region_id = r.region_id
JOIN products p ON ld.product_id = p.product_id
GROUP BY s.store_id, r.region_name, p.category, ld.product_id
ORDER BY risk_ratio DESC;

