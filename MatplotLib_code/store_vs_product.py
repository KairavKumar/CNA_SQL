import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.colors import ListedColormap, BoundaryNorm
from db_connection import get_connection

# === Connect to DB ===
conn = get_connection()
if conn is None:
    exit(1)

# === SQL Query ===
query = """
SELECT 
    s.store_id,
    i.product_id,
    i.inventory_level,
    i.units_ordered
FROM inventory_snapshots i
JOIN stores s ON i.store_id = s.store_id
"""

# === Load Data ===
df = pd.read_sql(query, conn)
conn.close()

# === Define Stock Status Logic ===
def get_stock_status(row):
    if row['inventory_level'] == 0:
        return "Out of Stock"
    elif row['inventory_level'] < row['units_ordered'] * 0.5:
        return "Below Reorder"
    elif row['inventory_level'] < row['units_ordered']:
        return "Near Reorder"
    else:
        return "Adequate"

df["Stock_Status"] = df.apply(get_stock_status, axis=1)

# === Status to Code Mapping ===
status_labels = ["Out of Stock", "Below Reorder", "Near Reorder", "Adequate"]
status_colors = ["#d62728", "#ff7f0e", "#ffdf00", "#2ca02c"]
status_map = {label: i for i, label in enumerate(status_labels)}
df["Status_Code"] = df["Stock_Status"].map(status_map)

# === Pivot Table ===
pivot_df = df.pivot_table(
    index="product_id",
    columns="store_id",
    values="Status_Code",
    aggfunc="min"
)

# === Colormap and Normalization ===
cmap = ListedColormap(status_colors)
bounds = [-0.5, 0.5, 1.5, 2.5, 3.5]  # one bin per category
norm = BoundaryNorm(bounds, cmap.N)

# === Plot Heatmap ===
plt.figure(figsize=(14, 10))
heatmap = sns.heatmap(
    pivot_df,
    cmap=cmap,
    norm=norm,
    linewidths=0.5,
    linecolor='gray',
    cbar_kws={'ticks': [0, 1, 2, 3]}
)

# === Fix Colorbar Labels ===
colorbar = heatmap.collections[0].colorbar
colorbar.set_ticks([0, 1, 2, 3])
colorbar.set_ticklabels(status_labels)
colorbar.ax.tick_params(labelsize=10)
colorbar.set_label("Stock Status", fontsize=12)

# === Titles and Axes ===
plt.title("Product-Level Stock Status Heatmap Across Stores", fontsize=16)
plt.xlabel("Store ID", fontsize=12)
plt.ylabel("Product ID", fontsize=12)
plt.xticks(rotation=45, ha='right')
plt.yticks(fontsize=8)
plt.tight_layout()
plt.show()
