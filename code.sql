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