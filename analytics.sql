-- =========================
-- Identify slow-moving and fast-moving products using the latest date in the data
-- =========================

WITH MaxDate AS (
    SELECT MAX(Date) AS max_date FROM inventory_facts
),
ProductTurnover AS (
    SELECT
        Product_ID,
        Category,
        SUM(Units_Sold) AS Total_Units_Sold,
        AVG(Inventory_Level) AS Avg_Inventory_Level,
        ROUND(SUM(Units_Sold) / (12*NULLIF(AVG(Inventory_Level), 0)), 2) AS Inventory_Turnover_Ratio
    FROM inventory_facts, MaxDate
    WHERE Date >= DATE_SUB(MaxDate.max_date, INTERVAL 12 MONTH)
    GROUP BY Product_ID, Category
)
SELECT
    Product_ID,
    Category,
    Total_Units_Sold,
    Avg_Inventory_Level,
    Inventory_Turnover_Ratio,
    CASE
        WHEN Inventory_Turnover_Ratio > 16 THEN 'Fast-moving'
        WHEN Inventory_Turnover_Ratio < 14 THEN 'Slow-moving'
        ELSE 'Moderate'
    END AS Movement_Status
FROM ProductTurnover
ORDER BY Inventory_Turnover_Ratio DESC;


-- =========================
-- 3-Month Rolling Inventory Turnover & Stock Adjustment Recommendations (Days of Supply)
-- =========================

WITH MaxDate AS (
    SELECT MAX(Date) AS max_date FROM inventory_facts
),

-- Rolling 3-month sales and average inventory per Store/Product
RollingTurnover AS (
    SELECT
        f.Store_ID,
        f.Product_ID,
        f.Category,
        f.Region,
        SUM(f.Units_Sold) AS Total_Units_Sold_3M,
        AVG(f.Inventory_Level) AS Avg_Inventory_Level_3M
    FROM inventory_facts f
    JOIN MaxDate m
      ON f.Date BETWEEN DATE_SUB(m.max_date, INTERVAL 3 MONTH) AND m.max_date
    GROUP BY f.Store_ID, f.Product_ID, f.Category, f.Region
),

-- Latest inventory and forecast per Store/Product
LatestSnapshot AS (
    SELECT
        f.Store_ID,
        f.Product_ID,
        f.Category,
        f.Region,
        f.Inventory_Level,
        f.Demand_Forecast,
        f.Seasonality,
        f.Date
    FROM inventory_facts f
    JOIN (
        SELECT Store_ID, Product_ID, MAX(Date) AS LatestDate
        FROM inventory_facts
        GROUP BY Store_ID, Product_ID
    ) l
      ON f.Store_ID = l.Store_ID AND f.Product_ID = l.Product_ID AND f.Date = l.LatestDate
),

-- Seasonality factor per product/season
SeasonalFactors AS (
    SELECT 
        Product_ID,
        Seasonality,
        AVG(Units_Sold) / NULLIF((SELECT AVG(Units_Sold) FROM inventory_facts WHERE Product_ID = sf.Product_ID), 0) AS Seasonal_Factor
    FROM inventory_facts sf
    GROUP BY Product_ID, Seasonality
),

Analysis AS (
    SELECT
        l.Store_ID,
        l.Product_ID,
        l.Category,
        l.Region,
        l.Inventory_Level,
        l.Demand_Forecast,
        l.Seasonality,
        r.Total_Units_Sold_3M,
        r.Avg_Inventory_Level_3M,
        ROUND(r.Total_Units_Sold_3M / NULLIF(r.Avg_Inventory_Level_3M, 0), 2) AS Inventory_Turnover_Ratio_3M,
        COALESCE(s.Seasonal_Factor, 1) AS Seasonal_Factor,
        ROUND(l.Inventory_Level / NULLIF(GREATEST(l.Demand_Forecast, r.Total_Units_Sold_3M/90), 0), 1) AS Days_Of_Supply
    FROM LatestSnapshot l
    LEFT JOIN RollingTurnover r ON l.Store_ID = r.Store_ID AND l.Product_ID = r.Product_ID
    LEFT JOIN SeasonalFactors s ON l.Product_ID = s.Product_ID AND l.Seasonality = s.Seasonality
)

SELECT
    Store_ID,
    Product_ID,
    Category,
    Region,
    Inventory_Level,
    Demand_Forecast,
    Total_Units_Sold_3M,
    Avg_Inventory_Level_3M,
    Inventory_Turnover_Ratio_3M,
    Days_Of_Supply,
    CASE
        WHEN Inventory_Turnover_Ratio_3M > 5 THEN 'Fast-moving'
        WHEN Inventory_Turnover_Ratio_3M < 2 THEN 'Slow-moving'
        ELSE 'Moderate'
    END AS Movement_Status,
    CASE
        WHEN Days_Of_Supply > 10 AND Inventory_Turnover_Ratio_3M < 2 THEN 'Reduce stock: Overstocked & slow-moving'
        WHEN Days_Of_Supply < 1.5 THEN 'Increase stock: Risk of stockout'
        WHEN Inventory_Turnover_Ratio_3M > 6.3 AND Days_Of_Supply < 10 THEN 'Increase stock: Fast-selling'
        ELSE 'Maintain current level'
    END AS Recommended_Action,
    CASE
        WHEN Days_Of_Supply > 10 AND Inventory_Turnover_Ratio_3M < 2 THEN ROUND(Inventory_Level - ((GREATEST(Demand_Forecast, Total_Units_Sold_3M/90))*2))
        WHEN Days_Of_Supply < 1.5 THEN ROUND((GREATEST(Demand_Forecast, Total_Units_Sold_3M/90))*2 - Inventory_Level)
        ELSE NULL
    END AS Suggested_Adjustment_Qty
FROM Analysis
ORDER BY Movement_Status, Recommended_Action, Days_Of_Supply DESC, Inventory_Turnover_Ratio_3M ASC;

-- =========================
-- Supplier inconsistencies by store and product
-- =========================

WITH MaxDate AS (
    SELECT MAX(Date) AS max_date FROM inventory_facts
),
RecentData AS (
    SELECT
        Store_ID,
        Product_ID,
        Region,
        Inventory_Level,
        Units_Sold,
        Units_Ordered,
        Date
    FROM inventory_facts f
    JOIN MaxDate m
      ON f.Date BETWEEN DATE_SUB(m.max_date, INTERVAL 3 MONTH) AND m.max_date
),
PerformanceSummary AS (
    SELECT
        Store_ID,
        Product_ID,
        Region,
        COUNT(*) AS Days_Tracked,
        SUM(CASE WHEN Inventory_Level <= 80 THEN 1 ELSE 0 END) AS Low_Stock_Days,
        ROUND(SUM(CASE WHEN Inventory_Level <= 80 THEN 1 ELSE 0 END) / COUNT(*), 2) AS Stockout_Rate,
        ROUND(STDDEV(Units_Ordered), 2) AS Order_StdDev,
        ROUND(AVG(Units_Ordered), 2) AS Avg_Units_Ordered,
        ROUND(STDDEV(Units_Sold), 2) AS Sales_StdDev,
        ROUND(AVG(Units_Sold), 2) AS Avg_Units_Sold
    FROM RecentData
    GROUP BY Store_ID, Product_ID, Region
)
SELECT
    Store_ID,
    Product_ID,
    Region,
    Days_Tracked,
    Low_Stock_Days,
    Stockout_Rate,
    Order_StdDev,
    Avg_Units_Ordered,
    Sales_StdDev,
    Avg_Units_Sold,
    CASE
        WHEN Stockout_Rate > 0.17 THEN 'Frequent Stockouts'
        WHEN Order_StdDev > Avg_Units_Ordered * 0.6 THEN 'Erratic Ordering'
        WHEN Sales_StdDev > Avg_Units_Sold * 0.9 THEN 'Erratic Fulfillment'
        ELSE 'Consistent'
    END AS Inconsistency_Flag
FROM PerformanceSummary
ORDER BY Inconsistency_Flag DESC, Stockout_Rate DESC, Order_StdDev DESC;

-- =========================
-- Seasonal/ Cylcic demand trends
-- =========================

WITH MonthlySales AS (
    SELECT
        Store_ID,
        Product_ID,
        YEAR(Date) AS Year,
        MONTH(Date) AS Month,
        SUM(Units_Sold) AS Total_Units_Sold
    FROM inventory_facts
    GROUP BY Store_ID, Product_ID, YEAR(Date), MONTH(Date)
),

Prev3MonthAvg AS (
    SELECT
        ms.Store_ID,
        ms.Product_ID,
        ms.Year,
        ms.Month,
        ms.Total_Units_Sold,
        (
            SELECT AVG(ms2.Total_Units_Sold)
            FROM MonthlySales ms2
            WHERE ms2.Store_ID = ms.Store_ID
              AND ms2.Product_ID = ms.Product_ID
              AND (
                    (ms2.Year < ms.Year)
                    OR (ms2.Year = ms.Year AND ms2.Month < ms.Month)
                  )
              AND STR_TO_DATE(CONCAT(ms2.Year, '-', LPAD(ms2.Month,2,'0'), '-01'), '%Y-%m-%d')
                  >= DATE_SUB(STR_TO_DATE(CONCAT(ms.Year, '-', LPAD(ms.Month,2,'0'), '-01'), '%Y-%m-%d'), INTERVAL 3 MONTH)
        ) AS Prev3Month_Avg
    FROM MonthlySales ms
),

MonthTrend AS (
    SELECT
        Store_ID,
        Product_ID,
        Month,
        Year,
        Total_Units_Sold,
        Prev3Month_Avg,
        CASE
            WHEN Prev3Month_Avg IS NULL THEN NULL
            WHEN Total_Units_Sold > Prev3Month_Avg * 1.1 THEN 'Upward'
            WHEN Total_Units_Sold < Prev3Month_Avg * 0.9 THEN 'Downward'
            ELSE 'Stable'
        END AS Trend
    FROM Prev3MonthAvg
),

TrendSummary AS (
    SELECT
        Store_ID,
        Product_ID,
        Month,
        COUNT(*) AS Years_Tracked,
        SUM(CASE WHEN Trend = 'Upward' THEN 1 ELSE 0 END) AS Upward_Count,
        SUM(CASE WHEN Trend = 'Downward' THEN 1 ELSE 0 END) AS Downward_Count,
        SUM(CASE WHEN Trend = 'Stable' THEN 1 ELSE 0 END) AS Stable_Count
    FROM MonthTrend
    WHERE Trend IS NOT NULL
    GROUP BY Store_ID, Product_ID, Month
)

SELECT
    Store_ID,
    Product_ID,
    Month,
    Years_Tracked,
    Upward_Count,
    Downward_Count,
    Stable_Count,
    CASE
        WHEN Upward_Count > Downward_Count AND Upward_Count > Stable_Count THEN 'Mostly Upward'
        WHEN Downward_Count > Upward_Count AND Downward_Count > Stable_Count THEN 'Mostly Downward'
        WHEN Stable_Count > Upward_Count AND Stable_Count > Downward_Count THEN 'Mostly Stable'
        ELSE 'Mixed'
    END AS Avg_Trend
FROM TrendSummary
ORDER BY Store_ID, Product_ID, Month;