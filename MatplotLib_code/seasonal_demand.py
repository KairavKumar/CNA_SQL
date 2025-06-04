import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from db_connection import get_connection

# === Connect to DB ===
conn = get_connection()
if conn is None:
    print("Database connection failed.")
    exit(1)

# === SQL Query: Average units sold by category and season ===
query = """
SELECT 
    p.category, 
    se.season_name, 
    AVG(i.units_sold) AS avg_units_sold
FROM inventory_snapshots i
JOIN products p ON i.product_id = p.product_id
JOIN seasonality se ON i.season_id = se.season_id
GROUP BY p.category, se.season_name
ORDER BY p.category, se.season_name;
"""
df = pd.read_sql(query, conn)
conn.close()

# === Plot: Grouped Bar Chart ===
plt.figure(figsize=(12, 7))
sns.set_theme(style="whitegrid")

# Grouped barplot: x=season, hue=category
ax = sns.barplot(
    data=df,
    x='season_name',
    y='avg_units_sold',
    hue='category'
)

ax.set_title("Average Units Sold by Product Category and Season", fontsize=16)
ax.set_xlabel("Season", fontsize=12)
ax.set_ylabel("Average Units Sold", fontsize=12)
ax.legend(title="Product Category", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
