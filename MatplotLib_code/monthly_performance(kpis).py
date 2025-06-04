import pandas as pd
import matplotlib.pyplot as plt
from db_connection import get_connection

# === Connect to DB ===
conn = get_connection()
if conn is None:
    print("Database connection failed.")
    exit(1)

# === Fetch KPI Data ===
query = """
SELECT
    i.store_id,
    DATE_FORMAT(i.snapshot_date, '%Y-%m') AS month,
    ROUND(AVG(i.inventory_level), 2) AS average_stock_level,
    ROUND(SUM(CASE WHEN i.inventory_level = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS stockout_rate,
    ROUND(SUM(i.units_sold) / NULLIF(AVG(i.inventory_level), 0), 2) AS inventory_turnover,
    ROUND(SUM(i.units_sold) / NULLIF(SUM(i.units_sold) + MAX(i.inventory_level), 0) * 100, 2) AS sell_through_rate
FROM inventory_snapshots i
GROUP BY i.store_id, month
ORDER BY i.store_id, month;
"""
df = pd.read_sql(query, conn)
conn.close()

# === Traffic Light Function ===
def traffic_light(val, kpi):
    if kpi == 'average_stock_level':
        if val > 100: return 'green'
        elif val >= 70: return 'yellow'
        else: return 'red'
    elif kpi == 'stockout_rate':
        if val < 0.1: return 'green'
        elif val <= 0.3: return 'yellow'
        else: return 'red'
    elif kpi == 'inventory_turnover':
        if val > 2.5: return 'green'
        elif val >= 1.5: return 'yellow'
        else: return 'red'
    elif kpi == 'sell_through_rate':
        if val > 70: return 'green'
        elif val >= 50: return 'yellow'
        else: return 'red'
    return 'gray'

kpi_list = ['average_stock_level', 'stockout_rate', 'inventory_turnover', 'sell_through_rate']
color_map = {'green': '#4caf50', 'yellow': '#ffeb3b', 'red': '#f44336', 'gray': '#888888'}

# === Plot Multi-KPI Dashboard with Sparklines ===
stores = df['store_id'].unique()
months = sorted(df['month'].unique())

fig, axes = plt.subplots(len(kpi_list), 1, figsize=(16, 10), sharex=True)
for i, kpi in enumerate(kpi_list):
    ax = axes[i]
    for store in stores:
        sub = df[df['store_id'] == store]
        ax.plot(sub['month'], sub[kpi], marker='o', label=f'Store {store}')
        # Traffic lights
        colors = [color_map[traffic_light(v, kpi)] for v in sub[kpi]]
        ax.scatter(sub['month'], sub[kpi], c=colors, s=120, edgecolors='black', zorder=3)
    ax.set_ylabel(kpi.replace('_', ' ').title())
    ax.set_title(f"{kpi.replace('_', ' ').title()} (Traffic Light)")
    ax.legend(title='Store', loc='upper left')

plt.xlabel("Month")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# === Sparklines for last 3 months ===
import seaborn as sns
fig2, axes2 = plt.subplots(len(kpi_list), 1, figsize=(16, 7), sharex=True)
last3months = sorted(months)[-3:]
for i, kpi in enumerate(kpi_list):
    ax = axes2[i]
    for store in stores:
        sub = df[(df['store_id'] == store) & (df['month'].isin(last3months))]
        ax.plot(sub['month'], sub[kpi], marker='o', label=f'Store {store}')
    ax.set_ylabel(kpi.replace('_', ' ').title())
    ax.set_title(f"{kpi.replace('_', ' ').title()} (Last 3-Month Sparkline)")
    ax.legend(title='Store', loc='upper left')
plt.xlabel("Month")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
