"""
Retail Sales Analytics Pipeline - Step 5: Power BI Export
Export cleaned data for Power BI dashboard creation
"""

import pandas as pd
import sqlite3
import os

print("="*60)
print("RETAIL SALES ANALYTICS - POWER BI EXPORT")
print("="*60)

# Ensure output directory exists
os.makedirs('data/processed', exist_ok=True)

# Connect to database
conn = sqlite3.connect('data/retail_analytics.db')

print("\n[1/5] Exporting dimension tables...")

# 1. Export Customers
query_customers = """
SELECT 
    customer_id,
    customer_name,
    email,
    city,
    state,
    customer_segment,
    registration_date
FROM customers
"""
df_customers = pd.read_sql(query_customers, conn)
df_customers.to_csv('data/processed/powerbi_customers.csv', index=False)
print(f"  ✓ Exported {len(df_customers):,} customers")

# 2. Export Products
query_products = """
SELECT 
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    unit_price,
    cost_price
FROM products
"""
df_products = pd.read_sql(query_products, conn)
df_products.to_csv('data/processed/powerbi_products.csv', index=False)
print(f"  ✓ Exported {len(df_products):,} products")

# 3. Export Stores
query_stores = """
SELECT 
    store_id,
    store_name,
    region,
    city,
    state,
    store_size_sqft,
    manager
FROM stores
"""
df_stores = pd.read_sql(query_stores, conn)
df_stores.to_csv('data/processed/powerbi_stores.csv', index=False)
print(f"  ✓ Exported {len(df_stores):,} stores")

print("\n[2/5] Exporting fact table (transactions)...")

# 4. Export main transactions (fact table)
query_transactions = """
SELECT 
    transaction_id,
    transaction_date,
    year,
    month,
    quarter,
    day_of_week,
    customer_id,
    product_id,
    store_id,
    quantity,
    unit_price,
    gross_amount,
    discount_percentage,
    discount_amount,
    net_amount,
    tax_amount,
    total_amount,
    profit,
    profit_margin,
    payment_method
FROM transactions
"""
df_transactions = pd.read_sql(query_transactions, conn)
df_transactions.to_csv('data/processed/powerbi_transactions.csv', index=False)
print(f"  ✓ Exported {len(df_transactions):,} transactions")

print("\n[3/5] Creating aggregated tables for performance...")

# 5. Monthly Summary
query_monthly = """
SELECT 
    strftime('%Y-%m', transaction_date) as year_month,
    year,
    month,
    quarter,
    COUNT(*) as total_transactions,
    COUNT(DISTINCT customer_id) as unique_customers,
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(SUM(profit), 2) as total_profit,
    ROUND(AVG(total_amount), 2) as avg_transaction_value,
    ROUND(SUM(profit) / SUM(total_amount) * 100, 2) as profit_margin_pct
FROM transactions
GROUP BY year_month, year, month, quarter
ORDER BY year_month
"""
df_monthly = pd.read_sql(query_monthly, conn)
df_monthly.to_csv('data/processed/powerbi_monthly_summary.csv', index=False)
print(f"  ✓ Exported {len(df_monthly):,} monthly summaries")

# 6. Category Performance
query_category = """
SELECT 
    category,
    subcategory,
    COUNT(*) as total_transactions,
    SUM(quantity) as total_units_sold,
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(SUM(profit), 2) as total_profit,
    ROUND(AVG(profit_margin), 2) as avg_profit_margin
FROM fact_sales
GROUP BY category, subcategory
ORDER BY total_revenue DESC
"""
df_category = pd.read_sql(query_category, conn)
df_category.to_csv('data/processed/powerbi_category_performance.csv', index=False)
print(f"  ✓ Exported {len(df_category):,} category records")

# 7. Regional Performance
query_regional = """
SELECT 
    region,
    store_id,
    COUNT(*) as total_transactions,
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(SUM(profit), 2) as total_profit,
    ROUND(AVG(total_amount), 2) as avg_transaction_value
FROM fact_sales
GROUP BY region, store_id
ORDER BY total_revenue DESC
"""
df_regional = pd.read_sql(query_regional, conn)
df_regional.to_csv('data/processed/powerbi_regional_performance.csv', index=False)
print(f"  ✓ Exported {len(df_regional):,} regional records")

print("\n[4/5] Creating KPI summary table...")

# 8. KPI Summary for Dashboard
query_kpi = """
SELECT 
    'Total Revenue' as kpi_name,
    ROUND(SUM(total_amount), 2) as kpi_value,
    'Currency' as kpi_type
FROM transactions
UNION ALL
SELECT 
    'Total Profit',
    ROUND(SUM(profit), 2),
    'Currency'
FROM transactions
UNION ALL
SELECT 
    'Total Transactions',
    COUNT(*),
    'Count'
FROM transactions
UNION ALL
SELECT 
    'Unique Customers',
    COUNT(DISTINCT customer_id),
    'Count'
FROM transactions
UNION ALL
SELECT 
    'Average Transaction Value',
    ROUND(AVG(total_amount), 2),
    'Currency'
FROM transactions
UNION ALL
SELECT 
    'Average Profit Margin',
    ROUND(AVG(profit_margin), 2),
    'Percentage'
FROM transactions
"""
df_kpi = pd.read_sql(query_kpi, conn)
df_kpi.to_csv('data/processed/powerbi_kpi_summary.csv', index=False)
print(f"  ✓ Exported {len(df_kpi):,} KPIs")

print("\n[5/5] Creating date dimension table...")

# 9. Date Dimension (for better time intelligence in Power BI)
query_date = """
SELECT DISTINCT
    transaction_date as date,
    year,
    quarter,
    month,
    CASE month
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END as month_name,
    day_of_week,
    week_of_year,
    CASE 
        WHEN month IN (1, 2, 3) THEN 'Q1'
        WHEN month IN (4, 5, 6) THEN 'Q2'
        WHEN month IN (7, 8, 9) THEN 'Q3'
        ELSE 'Q4'
    END as quarter_name,
    CASE 
        WHEN day_of_week IN ('Saturday', 'Sunday') THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type
FROM transactions
ORDER BY transaction_date
"""
df_date = pd.read_sql(query_date, conn)
df_date.to_csv('data/processed/powerbi_date_dimension.csv', index=False)
print(f"  ✓ Exported {len(df_date):,} date records")

conn.close()

# Create Power BI import guide
guide_content = """
================================================================================
POWER BI IMPORT GUIDE
================================================================================

Follow these steps to import the data into Power BI:

STEP 1: IMPORT DIMENSION TABLES
--------------------------------
1. Open Power BI Desktop
2. Click "Get Data" > "Text/CSV"
3. Import these files in order:
   - powerbi_customers.csv
   - powerbi_products.csv
   - powerbi_stores.csv
   - powerbi_date_dimension.csv

STEP 2: IMPORT FACT TABLES
---------------------------
4. Import the main fact table:
   - powerbi_transactions.csv

5. Import aggregated tables (optional, for performance):
   - powerbi_monthly_summary.csv
   - powerbi_category_performance.csv
   - powerbi_regional_performance.csv
   - powerbi_kpi_summary.csv

STEP 3: CREATE RELATIONSHIPS
-----------------------------
Go to "Model" view and create these relationships:

From powerbi_transactions:
  - customer_id → powerbi_customers[customer_id] (Many to One)
  - product_id → powerbi_products[product_id] (Many to One)
  - store_id → powerbi_stores[store_id] (Many to One)
  - transaction_date → powerbi_date_dimension[date] (Many to One)

STEP 4: CREATE MEASURES (DAX)
------------------------------
Create these calculated measures for your dashboard:

Total Revenue = SUM(powerbi_transactions[total_amount])

Total Profit = SUM(powerbi_transactions[profit])

Profit Margin % = 
    DIVIDE(
        SUM(powerbi_transactions[profit]),
        SUM(powerbi_transactions[net_amount]),
        0
    ) * 100

Average Transaction Value = AVERAGE(powerbi_transactions[total_amount])

Transaction Count = COUNT(powerbi_transactions[transaction_id])

STEP 5: RECOMMENDED VISUALIZATIONS
-----------------------------------
Page 1 - Executive Dashboard:
  - KPI Cards: Total Revenue, Total Profit, Profit Margin %, Transactions
  - Line Chart: Revenue Trend Over Time
  - Column Chart: Revenue by Region
  - Pie Chart: Revenue by Category

Page 2 - Product Analysis:
  - Table: Top Products by Revenue
  - Treemap: Category Performance
  - Bar Chart: Top Brands

Page 3 - Customer Analytics:
  - Column Chart: Customer Segmentation
  - Table: Top Customers

Page 4 - Regional Performance:
  - Map: Sales by Geography
  - Matrix: Store Performance

================================================================================
For more Power BI tutorials, visit: https://docs.microsoft.com/power-bi/
================================================================================
"""

with open('data/processed/POWER_BI_IMPORT_GUIDE.txt', 'w', encoding='utf-8') as f:
    f.write(guide_content)

print("\n" + "="*60)
print("✓ Power BI export completed successfully!")
print("="*60)
print("\nFiles exported to: data/processed/")
print("\nDimension Tables:")
print("  - powerbi_customers.csv")
print("  - powerbi_products.csv")
print("  - powerbi_stores.csv")
print("  - powerbi_date_dimension.csv")
print("\nFact Tables:")
print("  - powerbi_transactions.csv")
print("\nAggregated Tables:")
print("  - powerbi_monthly_summary.csv")
print("  - powerbi_category_performance.csv")
print("  - powerbi_regional_performance.csv")
print("  - powerbi_kpi_summary.csv")
print("\nImport Guide:")
print("  - POWER_BI_IMPORT_GUIDE.txt")
print("\n" + "="*60)
print("Ready for Power BI! See POWER_BI_IMPORT_GUIDE.txt for setup instructions.")
print("="*60)