# CNA Project Summary and Insights

## Database Schema Overview

### Tables (Normalized to 3NF)

The database consists of 9 normalized tables designed to efficiently store and manage inventory data:

#### Entity Tables
1. **Stores** - Lists all stores in the database
2. **Regions** - Contains regions where stores operate
3. **Products** - Catalog of products potentially sold
4. **Weather_conditions** - Indexes weather types for daily correlation
5. **Seasonality** - Indexes seasons for product seasonality analysis
6. **Promotions** - Indexes promotional activities and their effects
7. **Inventory_snapshots** - Central data table with one-to-many relationships to all entity tables

#### Relationship Tables
7. **Store_regions** - Many-to-many relationship between stores and regions
8. **Price_history** - Historical pricing data indexed by product, region, and date

---

## Analysis Queries

### 1. Current Stock Levels
Returns real-time stock levels across all products, stores, and regions, providing comprehensive distribution visibility.

### 2. Reorder Point Analysis
Evaluates each product-store-region combination against historical statistics, categorizing stock status as:
- 🔴 **Out of stock**
- 🟠 **Below reorder point**
- 🟡 **Near reorder point**
- 🟢 **Adequate stock**

### 3. Seasonal Reorder Points
Similar to standard reorder analysis but uses season-specific historical data for more accurate seasonal planning.

### 4. Monthly Inventory Turnover
Measures stocking efficiency by calculating units sold divided by average inventory level, categorized as:
- 🔥 **High turnover**
- ⚡ **Moderate turnover**
- 🐌 **Low turnover**
- ❌ **No sales**

### 5. Stockout Risk Analysis
Counts days where inventory fell below reorder points to assess stockout risk:
- ⚠️ **High risk**
- 🔶 **Moderate risk**
- 🔷 **Low risk**
- ✅ **Safe**

---

## Summary Reports

### Operational Metrics
- **Inventory Age** - Duration since last stock replenishment by product and store
- **Stockout Rate** - Percentage of days a product was out of stock
- **Sell Through Rate** - Monthly percentage of products sold vs. weighted average stock
- **Average Stock Level** - Monthly average by product category and region
- **Dead Stock Analysis** - Percentage of zero-sale days by store

---

## Advanced Analytics

### 1. 3-Month Rolling Inventory Turnover
Provides actionable stock management recommendations:
- 📈 **Order more**
- 📉 **Reduce stock**
- 📊 **Hold steady**

### 2. Supplier Performance Analysis
Identifies supply chain issues:
- 🚨 **Frequent stockouts**
- 📊 **Erratic ordering**
- 🔄 **Erratic supply**
- ✅ **Consistent**

### 3. Seasonal Demand Trends
Forecasts demand patterns by season and store for strategic planning.

---

## Key Insights & Recommendations

### Immediate Actions (Daily Operations)
- **Fragile inventory management**: Although daily replenishments have prevented any stock‐outs to date, every store is constantly operating below its reorder point, and those reorder thresholds are set worryingly high. This razor-thin buffer means that any single day of delay—supplier hiccup, shipping hold-up, system glitch—would immediately trigger stock-outs and revenue loss. This is unacceptable risk. Action is required now: increase safety stock levels, tighten lead-time SLAs, and re-evaluate reorder points. Our stock‐out risk analysis, reorder‐point report, and inventory‐age drill-downs all confirm the urgency.
- **Erratic Supply**: As seen from our supplier inconsistency table product P0003 and P0009 have erratic supply from the producer's side which should be checked at the earliest.
- **Product Placement**: Monthly turnover data identifies fast-selling products for optimal store configuration

### Strategic Planning (Long-term)
- **Dynamic Safety-Stock Modeling**: Leverage stochastic demand forecasts and lead-time variability to compute SKU-specific safety-stock levels—minimizing capital tied up in excess inventory while virtually eliminating stock-outs.
- **Role-Based Analytics & Alerts**: Build customizable dashboards and automated alert pipelines that surface KPIs (e.g. turnover, fill-rate, supplier SLAs) tailored to executive, regional, and store-level managers—ensuring each stakeholder sees exactly the metrics they need.
- **Supplier Performance Scorecard & Risk Index**: Integrate multi-year purchase, quality, and on-time delivery data to compute a composite supplier score and real-time risk index—enabling pro-active sourcing decisions, renegotiation triggers, and dual-sourcing strategies to safeguard against disruptions.

### Advanced Optimization
- **Leverage seasonal trends with AI**: By feeding historical sales seasonality into a predictive AI/ML model, we can forecast demand spikes and troughs far more accurately—shifting from reactive restocking to truly proactive inventory management. This will dramatically reduce both overstock and stock‐out scenarios, unlocking substantial efficiency gains and cost saving 
- **Approximate Query Processing**: Use probabilistic data structures (e.g. Count-Min Sketch, HyperLogLog) for ultra-fast approximate counts and distinct-counts on massive historic logs

---

## Data Analysis Results

### Performance Metrics

#### Monthly Sell Through Rate
- **Stability**: Consistent ~94% across all months
- **Data Quality**: January 2024 data incomplete (single day) - requires improved automation

#### Regional Performance
- **Consistency**: Uniform sell through rates across regions
- **Inventory Stability**: Maintained at specified levels
- **Sales Variability**: Regional fluctuations identified (e.g., Western region mid-year dip)

#### Stock Management Insights
- **Reorder Optimization**: Heatmap analysis reveals inventory frequently below reorder points
- **Seasonal Adaptation**: Implement adaptive reorder points based on product lifecycle
- **Predictive Stocking**: Opportunity to optimize inventory levels using seasonal demand trends

### Key Performance Indicators (KPIs)

| Metric | Status | Recommendation |
|--------|--------|----------------|
| Average Stock Level | ✅ Consistent | Monitor seasonal variations |
| Stockout Rate | ✅ Stable at 0% | Maintain current practices |
| Inventory Turnover | ✅ Stable | Implement adaptive optimization |
| Sell Through Rate | ✅ Stable | Apply seasonal optimization methods |

### Strategic Observations

#### Seasonal Demand Patterns
- Clear seasonal variations by product category
- Potential for monthly-scale trend analysis with sufficient data
- Efficiency analysis needed for extended granularity

#### Inventory Distribution
- **Store-Level**: Inventory proportional to store size
- **Status Distribution**: All stores maintain adequate overall stock
- **Optimization Opportunity**: Risk of overstocking - use seasonal reorder points for borderline adequate levels

#### Critical Balance
While individual products may show below-reorder-point status, overall inventory remains adequate. Success requires both:
1. **Overall Adequacy** - Sufficient total inventory
2. **Proper Distribution** - Optimal allocation across products and locations

---

## Next Steps

1. **Automation Enhancement** - Improve data collection and real-time updates
2. **Predictive Integration** - Implement seasonal forecasting in daily operations  
3. **Access Control Implementation** - Deploy role-based system access
4. **Supplier Relationship Management** - Utilize inconsistency analysis for vendor negotiations
5. **Advanced Analytics** - Extend seasonal analysis to product-level monthly trends
