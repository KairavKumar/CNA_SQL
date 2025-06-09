# 📦 Inventory Inefficiencies Analytics – Urban Retail Co.

**A SQL + Python solution to diagnose and optimize retail inventory management.**  
*Powered by MySQL, Matplotlib & Python-ODBC*



## 🚀 Getting Started

### ✅ Prerequisites

- Python 3.8+  
- MySQL Server 8.0+  
- ODBC driver for MySQL  

### 🛠 Installation

```bash
git clone https://github.com/yourusername/inventory-inefficiencies-sql.git
cd inventory-inefficiencies-sql
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env            # edit .env with your MySQL creds 
```

# Retail Store Inventory Analysis 📊

## 🚀 Quick Start

### 1. **Import the dataset**
Load `retail_store_inventory.csv` into your MySQL database.

### 2. **Create schema & ETL**
```sql
SOURCE CODE.sql;
```

=

## 📂 Project Structure

```bash
/
├── MatplotLib_code/              # Python scripts for plots
├── Plots of Key Apis/            # Output visuals
├── ps_relevant_pdfs/             # Reference PDFs
├── venv/                         # Python virtual environment
├── .env.example                  # Sample env vars
├── .gitignore
├── analytics.sql                 # Analytics SQL queries
├── code.sql                      # Schema & ETL scripts
├── db_connection.py              # Python-MySQL connector
├── requirements.txt
├── retail_store_inventory.csv    # Main dataset
├── ER_Diagram_3NF_normalized.pdf #Depiction of the relation between the tables
└── README.md                     # This file
```

## 🏗️ Architecture

* **Data Layer**: MySQL – normalized schema for products, suppliers, sales, inventory
* **Analytics Layer**: Advanced SQL scripts for KPI extraction, trend analysis & reporting
* **Visualization Layer**: Python + Matplotlib for dashboards and plots
* **Integration**: Python-ODBC bridges MySQL and Python analytics

## ✨ Features

### SQL Analytics
* Stock level calculations across stores & warehouses
* Low-inventory detection & reorder alerts
* Inventory turnover & aging analysis
* KPI summaries: stockout rates, average stock levels

### Database Optimization
* Fully normalized schema
* Indexed joins, window functions & optimized queries

### Analytical Outputs
* Fast vs. slow-moving SKU identification
* Stock adjustment recommendations
* Supplier performance scoring
* Seasonal demand forecasting

### Visualizations
* Matplotlib-based dashboards & plots
* Outputs saved in `Plots of Key Apis/`

## 📝 Usage

**Run analytics in MySQL CLI or Workbench**
* Eg:
```sql
SELECT * FROM products WHERE units_in_stock < reorder_point;
```

**Generate plots**
```bash
python MatplotLib_code/inventory_turnover.py
```

**View dashboards**
Open files in `Plots of Key Apis/`

## 📈 Results & Insights

* Identified SKUs at risk of stockout
* Highlighted slow-moving items tying up capital
* Flagged supplier inconsistencies
* Forecasted demand spikes for proactive planning

## 🛠️ Dependencies

See `requirements.txt` for:
* mysql-connector-python
* matplotlib
* pandas
* python-dotenv

## 🔮 Future Work

* Integrate real-time data pipelines & live dashboards
* Expand to multi-channel (online/offline) data
* Build interactive dashboards using Dash or Streamlit
* Add machine-learning for advanced demand forecasting


