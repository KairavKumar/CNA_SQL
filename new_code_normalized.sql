-- Enhanced normalized schema for complete query support
CREATE DATABASE IF NOT EXISTS cna_normalized;
USE cna_normalized;

-- 1. STORES TABLE
CREATE TABLE stores (
    store_id VARCHAR(10) PRIMARY KEY,
    region VARCHAR(50) NOT NULL,
    store_name VARCHAR(100),
    INDEX idx_region (region)
);

-- 2. PRODUCTS TABLE
CREATE TABLE products (
    product_id VARCHAR(10) PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    product_name VARCHAR(100),
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

