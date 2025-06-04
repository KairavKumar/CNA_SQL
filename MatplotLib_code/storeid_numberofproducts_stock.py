import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from db_connection import get_connection

# --- DB Connection ---
conn = get_connection()
if conn is None:
    exit("Failed to connect to database.")

# --- SQL Query ---
query = """
WITH latest_date AS (
    SELECT MAX(snapshot_date) AS snapshot_date FROM inventory_snapshots
),
inventory_status AS (
    SELECT 
        s.store_id,
        r.region_name AS region,
        p.product_id,
        inv.inventory_level,
        CASE
            WHEN inv.inventory_level = 0 THEN 'Out of Stock'
            WHEN inv.inventory_level <= 10 THEN 'Below Reorder'
            WHEN inv.inventory_level <= 20 THEN 'Near Reorder'
            ELSE 'Adequate Stock'
        END AS stock_status
    FROM inventory_snapshots inv
    JOIN latest_date ld ON inv.snapshot_date = ld.snapshot_date
    JOIN stores s ON inv.store_id = s.store_id
    JOIN regions r ON inv.region_id = r.region_id
    JOIN products p ON inv.product_id = p.product_id
)
SELECT 
    store_id,
    region,
    stock_status,
    COUNT(product_id) AS product_count
FROM inventory_status
GROUP BY store_id, region, stock_status
ORDER BY store_id;
"""

# --- Fetch Data ---
df = pd.read_sql(query, conn)
conn.close()

# --- Pivot for Stacked Bar ---
pivot_df = df.pivot_table(index=['store_id', 'region'], columns='stock_status', values='product_count', fill_value=0).reset_index()

# Ensure consistent order
status_order = ['Out of Stock', 'Below Reorder', 'Near Reorder', 'Adequate Stock']
for status in status_order:
    if status not in pivot_df.columns:
        pivot_df[status] = 0

# Sort stores for consistent plotting
pivot_df = pivot_df.sort_values(by='store_id')

# --- Plot: Stacked Bar Chart ---
fig, ax = plt.subplots(figsize=(14, 8))
bottom = None
colors = {
    'Out of Stock': '#d73027',
    'Below Reorder': '#fc8d59',
    'Near Reorder': '#fee08b',
    'Adequate Stock': '#91cf60'
}

x = pivot_df['store_id']
for status in status_order:
    y = pivot_df[status]
    ax.bar(x, y, bottom=bottom, label=status, color=colors[status])
    bottom = y if bottom is None else bottom + y

ax.set_title('Inventory Status by Store (Stacked Bar)', fontsize=16)
ax.set_ylabel('Number of Products')
ax.set_xlabel('Store ID')
ax.legend(title='Stock Status')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.show()

# --- Plot: Heatmap by Region and Store ---
heatmap_df = pivot_df.copy()
heatmap_df['Total_Issues'] = heatmap_df['Out of Stock'] + heatmap_df['Below Reorder'] + heatmap_df['Near Reorder']
pivot_heat = heatmap_df.pivot(index='region', columns='store_id', values='Total_Issues').fillna(0)

plt.figure(figsize=(14, 6))
sns.heatmap(pivot_heat, cmap='Reds', annot=True, fmt='.0f', linewidths=.5)
plt.title('Heatmap of Product Stock Issues by Store and Region', fontsize=16)
plt.xlabel('Store ID')
plt.ylabel('Region')
plt.tight_layout()
plt.show()
