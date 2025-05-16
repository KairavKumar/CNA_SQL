
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from db_connection import get_connection  # import the connection setup

# === Connect to database ===
conn = get_connection()
if conn is None:
    exit(1)

# === SQL Query ===
query = """
WITH DailyStats AS (
    SELECT 
        Region,
        DATE(Date) AS Day,
        DATE_FORMAT(Date, '%Y-%m') AS Month,
        SUM(Units_Sold) AS Daily_Sales,
        SUM(Inventory_Level) AS Daily_Inventory
    FROM inventory_facts
    GROUP BY Region, DATE(Date), DATE_FORMAT(Date, '%Y-%m')
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
"""

# === Fetch Data from MySQL ===
df = pd.read_sql(query, conn)
conn.close()

# === Data Cleaning ===
df['Region'] = df['Region'].astype(str).str.replace('\x00', '', regex=False).str.strip()
df['Month'] = df['Month'].astype(str).str.replace('\x00', '', regex=False).str.strip()
df['Month'] = pd.to_datetime(df['Month'], format='%Y-%m', errors='coerce')
df = df.dropna(subset=['Month'])

# === Plot Setup ===
plt.figure(figsize=(14, 7))
sns.set_theme(style="whitegrid")

# === Lineplot ===
sns.lineplot(
    data=df,
    x='Month',
    y='Sell_Through_Rate_Percent',
    hue='Region',
    marker='o',
    palette='tab10'
)

# === Custom Y-Axis Scaling ===
y_min = max(0, df["Sell_Through_Rate_Percent"].min() - 5)
y_max = df["Sell_Through_Rate_Percent"].max() + 5
range_span = y_max - y_min
step = 2 if range_span <= 20 else 5
plt.yticks(np.arange(y_min, y_max + 1, step))

# === Axis Labels and Title ===
plt.title('Monthly Sell-Through Rate (%) by Region', fontsize=16)
plt.xlabel('Month', fontsize=12)
plt.ylabel('Sell-Through Rate (%)', fontsize=12)
plt.xticks(rotation=45, ha='right')
plt.legend(title='Region')
plt.tight_layout()

# === Show Plot ===
plt.show()