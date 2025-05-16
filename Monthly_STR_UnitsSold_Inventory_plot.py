
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
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

# === Fetch and clean data ===
df = pd.read_sql(query, conn)
conn.close()

df['Region'] = df['Region'].astype(str).str.replace('\x00', '', regex=False).str.strip()
df['Month'] = df['Month'].astype(str).str.replace('\x00', '', regex=False).str.strip()
df['Month'] = pd.to_datetime(df['Month'], format='%Y-%m', errors='coerce')
df = df.dropna(subset=['Month'])

# === Plot setup ===
sns.set_theme(style="whitegrid")
regions = df['Region'].unique()
colors = sns.color_palette("tab10", n_colors=len(regions))

fig, ax1 = plt.subplots(figsize=(15, 8))

# Plot Units Sold and Inventory (left y-axis)
for i, region in enumerate(regions):
    sub_df = df[df['Region'] == region]
    ax1.plot(sub_df['Month'], sub_df['Total_Units_Sold'], label=f'{region} - Units Sold', linestyle='-', color=colors[i])
    ax1.plot(sub_df['Month'], sub_df['Weighted_Avg_Inventory'], label=f'{region} - Inventory', linestyle='--', color=colors[i], alpha=0.5)

ax1.set_xlabel("Month(quarterly)", fontsize=12)
ax1.set_ylabel("Units Sold / Inventory", fontsize=12)
ax1.tick_params(axis='x', rotation=45)

# Plot Sell-Through Rate (right y-axis)
ax2 = ax1.twinx()
for i, region in enumerate(regions):
    sub_df = df[df['Region'] == region]
    ax2.plot(sub_df['Month'], sub_df['Sell_Through_Rate_Percent'], label=f'{region} - STR (%)', linestyle=':', color=colors[i])

ax2.set_ylabel("Sell-Through Rate (%)", fontsize=12)

# Combine legends from both axes
lines_1, labels_1 = ax1.get_legend_handles_labels()
lines_2, labels_2 = ax2.get_legend_handles_labels()
all_lines = lines_1 + lines_2
all_labels = labels_1 + labels_2

# Place legend inside plot, centered horizontally and vertically ~0.5
fig.legend(
    all_lines,
    all_labels,
    loc='center',
    bbox_to_anchor=(0.5, 0.5),  # x=middle, y=middle of plot area
    ncol=4,
    fontsize=9,
    frameon=True,
    framealpha=0.7  # translucent background for readability
)

plt.title("Sell-Through Rate vs Units Sold and Inventory by Region", fontsize=16)
plt.tight_layout()
plt.show()