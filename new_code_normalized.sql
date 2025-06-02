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
    base_price DECIMAL(10,2) NOT NULL,
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


-- Seasonal reorder points equivalent
WITH ProductSalesSummary AS (
    SELECT 
        i.store_id, 
        i.product_id, 
        p.category, 
        s.region,
        ROUND(AVG(i.units_sold)/7, 2) AS Avg_Daily_Sales,
        ROUND(STDDEV(i.units_sold)/7, 2) AS StdDev_Sales,
        MAX(CASE WHEN i.snapshot_date = (
            SELECT MAX(snapshot_date) 
            FROM inventory_snapshots i2 
            WHERE i2.store_id = i.store_id AND i2.product_id = i.product_id
        ) THEN i.inventory_level END) AS Current_Stock
    FROM inventory_snapshots i
    JOIN products p ON i.product_id = p.product_id
    JOIN stores s ON i.store_id = s.store_id
    GROUP BY i.store_id, i.product_id, p.category, s.region
)
-- Rest of your seasonal calculation logic works the same

SELECT 
    DATE_FORMAT(i.snapshot_date, '%Y-%m') AS YearMonth,
    i.store_id,
    i.product_id,
    SUM(i.units_sold) AS Total_Units_Sold,
    AVG(i.inventory_level) AS Avg_Inventory_Level,
    ROUND(SUM(i.units_sold) / NULLIF(AVG(i.inventory_level), 0), 2) AS Inventory_Turnover_Ratio
FROM inventory_snapshots i
GROUP BY YearMonth, i.store_id, i.product_id;

-- Your inventory age logic works with snapshot_date
WITH InventoryIncreases AS (
    SELECT 
        store_id, product_id, snapshot_date, inventory_level,
        LAG(inventory_level) OVER (PARTITION BY store_id, product_id ORDER BY snapshot_date) AS Prev_Inventory
    FROM inventory_snapshots
)
-- Rest of your logic remains the same

