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

-- Calculate reorder points with adjusted time scale -------------------------------------------------------------------------------------------------
-- Assuming Units_Sold represents weekly sales as otherwise very non uniform answers are coming
WITH LatestDatePerProduct AS (
    SELECT Store_ID, Product_ID, MAX(Date) AS LatestDate
    FROM inventory_facts
    GROUP BY Store_ID, Product_ID
),
LatestProductSales AS (
    SELECT 
        f.Store_ID, 
        f.Product_ID, 
        f.Category, 
        f.Region,
        f.Units_Sold, 
        f.Inventory_Level, 
        f.Price,
        f.Seasonality
    FROM inventory_facts f
    JOIN LatestDatePerProduct l
      ON f.Store_ID = l.Store_ID 
     AND f.Product_ID = l.Product_ID 
     AND f.Date = l.LatestDate
),
ProductSalesSummary AS (
    SELECT 
        f.Store_ID, 
        f.Product_ID, 
        f.Category, 
        f.Region,
        ROUND(AVG(f.Units_Sold)/7, 2) AS Avg_Daily_Sales, 
        ROUND(STDDEV(f.Units_Sold)/7, 2) AS StdDev_Sales,
        MAX(l.Inventory_Level) AS Current_Stock,
        MAX(l.Price) AS Unit_Price,
        MAX(l.Seasonality) AS Current_Season
    FROM inventory_facts f
    JOIN LatestProductSales l
      ON f.Store_ID = l.Store_ID 
     AND f.Product_ID = l.Product_ID
    GROUP BY f.Store_ID, f.Product_ID, f.Category, f.Region
),
SeasonalFactors AS (
    SELECT 
        Product_ID,
        Seasonality,
        AVG(Units_Sold) / (
            SELECT AVG(Units_Sold) 
            FROM inventory_facts i2 
            WHERE i2.Product_ID = inventory_facts.Product_ID
        ) AS Seasonal_Factor
    FROM inventory_facts
    GROUP BY Product_ID, Seasonality
)
SELECT 
    p.Store_ID,
    p.Product_ID,
    p.Category,
    p.Region,
    p.Current_Stock,
    p.Avg_Daily_Sales,
    ROUND((7 * p.Avg_Daily_Sales) + (1.5 * p.StdDev_Sales)) AS Standard_Reorder_Point,
    ROUND(((7 * p.Avg_Daily_Sales) + (1.5 * p.StdDev_Sales)) * 
          COALESCE(s.Seasonal_Factor, 1)) AS Seasonal_Reorder_Point,
    CASE 
        WHEN p.Current_Stock <= 0 THEN 'Out of Stock'
        WHEN p.Current_Stock < (((7 * p.Avg_Daily_Sales) + (1.5 * p.StdDev_Sales)) * COALESCE(s.Seasonal_Factor, 1)) 
            THEN 'Below Seasonal Reorder Point'
        WHEN p.Current_Stock < (((7 * p.Avg_Daily_Sales) + (1.5 * p.StdDev_Sales)) * COALESCE(s.Seasonal_Factor, 1)) * 1.2 
            THEN 'Near Seasonal Reorder Point'
        ELSE 'Adequate Stock'
    END AS Stock_Status,
    ROUND(p.Current_Stock / NULLIF(p.Avg_Daily_Sales * COALESCE(s.Seasonal_Factor, 1), 0), 1) AS Days_Of_Supply
FROM ProductSalesSummary p
LEFT JOIN SeasonalFactors s ON p.Product_ID = s.Product_ID AND p.Current_Season = s.Seasonality
ORDER BY 
    CASE 
        WHEN p.Current_Stock <= 0 THEN 1
        WHEN p.Current_Stock < (((7 * p.Avg_Daily_Sales) + (1.5 * p.StdDev_Sales)) * COALESCE(s.Seasonal_Factor, 1)) THEN 2
        WHEN p.Current_Stock < (((7 * p.Avg_Daily_Sales) + (1.5 * p.StdDev_Sales)) * COALESCE(s.Seasonal_Factor, 1)) * 1.2 THEN 3
        ELSE 4
    END,
    Days_Of_Supply;


---------------------------------------------------------------------------------------------------------------
-- inventory turnover ratio
-- This query calculates the inventory turnover ratio for each product in each store\
-- units based turn over ratio as cogs cannot be calculated
SELECT 
    DATE_FORMAT(Date, '%Y-%m') AS YearMonth,
    Store_ID,
    Product_ID,
    SUM(Units_Sold) AS Total_Units_Sold,
    AVG(Inventory_Level) AS Avg_Inventory_Level,
    ROUND(SUM(Units_Sold) / NULLIF(AVG(Inventory_Level), 0), 2) AS Inventory_Turnover_Ratio
FROM inventory_facts
GROUP BY YearMonth, Store_ID, Product_ID
ORDER BY YearMonth, Store_ID, Product_ID;



--  Stockout Risk Analysis (Days with Low or Zero Inventory)
WITH ProductStats AS (
    SELECT 
        Store_ID,
        Product_ID,
        ROUND(AVG(Units_Sold) / 7, 2) AS Avg_Daily_Sales,
        ROUND(STDDEV(Units_Sold) / 7, 2) AS StdDev_Sales
    FROM inventory_facts
    GROUP BY Store_ID, Product_ID
),
ReorderPoints AS (
    SELECT 
        Store_ID,
        Product_ID,
        ROUND((7 * Avg_Daily_Sales) + (1.5 * StdDev_Sales), 2) AS Reorder_Point
    FROM ProductStats
),
LabeledData AS (
    SELECT 
        f.Store_ID,
        f.Product_ID,
        f.Date,
        f.Inventory_Level,
        rp.Reorder_Point,
        CASE 
            WHEN f.Inventory_Level <= rp.Reorder_Point THEN 1
            ELSE 0
        END AS Is_Low
    FROM inventory_facts f
    JOIN ReorderPoints rp 
      ON f.Store_ID = rp.Store_ID AND f.Product_ID = rp.Product_ID
)
SELECT 
    Store_ID,
    Product_ID,
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
GROUP BY Store_ID, Product_ID
ORDER BY Risk_Ratio DESC;




---SUMMARY REPORTS with some kpi's----------------------------------------------------------------------------------------------------------- 


--1.INVENTORY AGE 
--This measures how long inventory has been sitting in the store since last major replenishment.
--Done by finding the date went last significant increase was seen in inventory.
--Then calculate number of days that have passed since then, threshold to consider as significant increase is 20%( factor of 1.2),
--
WITH InventoryIncreases AS (
  SELECT 
    Store_ID,
    Product_ID,
    Date,
    Inventory_Level,
    LAG(Inventory_Level) OVER (PARTITION BY Store_ID, Product_ID ORDER BY Date) AS Prev_Inventory
  FROM inventory_facts
),
ReplenishmentDates AS (
  SELECT 
    Store_ID,
    Product_ID,
    Date AS Replenished_On
  FROM InventoryIncreases
  WHERE Inventory_Level > Prev_Inventory * 1.2
),
LatestStock AS (
  SELECT 
    Store_ID,
    Product_ID,
    MAX(Date) AS Latest_Date
  FROM inventory_facts
  GROUP BY Store_ID, Product_ID
),
InventoryAge AS (
  SELECT 
    l.Store_ID,
    l.Product_ID,
    MAX(r.Replenished_On) AS Last_Replenishment_Date,
    l.Latest_Date,
    DATEDIFF(l.Latest_Date, MAX(r.Replenished_On)) AS Inventory_Age_Days
  FROM LatestStock l
  JOIN ReplenishmentDates r 
    ON l.Store_ID = r.Store_ID 
   AND l.Product_ID = r.Product_ID
  WHERE r.Replenished_On <= l.Latest_Date
  GROUP BY l.Store_ID, l.Product_ID, l.Latest_Date
)
SELECT 
  Store_ID,
  Product_ID,
  DATE_FORMAT(Last_Replenishment_Date, '%Y-%m-%d') AS Last_Replenishment_Date,
  DATE_FORMAT(Latest_Date, '%Y-%m-%d') AS Latest_Date,
  Inventory_Age_Days
FROM InventoryAge
ORDER BY Inventory_Age_Days DESC;


--2.STOCKOUT RATE---------------------------------------------------------------------
----How often a product was out of stock in terms of % available days.
SELECT 
    Store_ID,
    Product_ID,
    COUNT(CASE WHEN Inventory_Level = 0 THEN 1 END) AS Stockout_Days,
    COUNT(*) AS Total_Days,
    ROUND(COUNT(CASE WHEN Inventory_Level = 0 THEN 1 END) / COUNT(*) * 100, 2) AS Stockout_Rate_Percent
FROM inventory_facts
GROUP BY Store_ID, Product_ID
ORDER BY Stockout_Rate_Percent DESC;
--we observe no product in any of the stores ever had a stockout on any given day.


--3.SELL THROUGH RATE---------------------------------------------------------------
--Measures how much stock was sold relative to total stock (sold + remaining).

--a) sell through rates by region and  month based on weighted average inventory
WITH DailyStats AS (
    SELECT 
        Region,
        DATE(Date) AS Day,
        DATE_FORMAT(Date, '%Y-%m') AS Month,
        SUM(Units_Sold) AS Daily_Sales,
        SUM(Inventory_Level) AS Daily_Inventory
    FROM inventory_facts
    GROUP BY Region, Day, Month
),
WeightedInventory AS (
    SELECT 
        Month,
        Region,
        ROUND(AVG(Daily_Inventory), 2) AS Weighted_Avg_Inventory,
        SUM(Daily_Sales) AS Total_Units_Sold
    FROM DailyStats
    GROUP BY Region, Month
)
SELECT 
    Month,
    Region,
    Total_Units_Sold,
    Weighted_Avg_Inventory,
    ROUND(
        Total_Units_Sold / NULLIF(Total_Units_Sold + Weighted_Avg_Inventory, 0) * 100, 2
    ) AS Sell_Through_Rate_Percent
FROM WeightedInventory
ORDER BY Month, Region;

--b) sell through rates by stores and month
SELECT 
    DATE_FORMAT(Date, '%Y-%m') AS Month,
    Region,
    Store_ID,
    SUM(Units_Sold) AS Total_Units_Sold,
    MAX(Inventory_Level) AS Ending_Inventory,
    ROUND(
        SUM(Units_Sold) / NULLIF(SUM(Units_Sold) + MAX(Inventory_Level), 0) * 100, 2
    ) AS Sell_Through_Rate_Percent
FROM inventory_facts
GROUP BY Month, Region, Store_ID
ORDER BY Month, Region, Store_ID;


--4.AVERAGE STOCK LEVEL-----------------------------------------------------------
---average inventory level held over a period of one month grouped by region
SELECT 
    Region,
    Category,
    DATE_FORMAT(Date, '%Y-%m') AS YearMonth,
    ROUND(AVG(Inventory_Level), 2) AS Avg_Stock_Level
FROM inventory_facts
GROUP BY Region, Category, YearMonth
ORDER BY Region, Category, YearMonth;


