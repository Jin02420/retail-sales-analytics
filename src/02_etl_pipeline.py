"""
Retail Sales Analytics Pipeline - Step 2: ETL Pipeline
Extracts, transforms, and loads data into SQLite database
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime

print("="*60)
print("RETAIL SALES ANALYTICS - ETL PIPELINE")
print("="*60)

# Create database connection
db_path = 'data/retail_analytics.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("\n[1/5] Loading raw data files...")

# Load raw data
df_customers = pd.read_csv('data/raw/customers.csv')
df_products = pd.read_csv('data/raw/products.csv')
df_stores = pd.read_csv('data/raw/stores.csv')
df_transactions = pd.read_csv('data/raw/transactions.csv')

print(f"  ✓ Customers: {len(df_customers):,} records")
print(f"  ✓ Products: {len(df_products):,} records")
print(f"  ✓ Stores: {len(df_stores):,} records")
print(f"  ✓ Transactions: {len(df_transactions):,} records")

# Data Quality Checks and Cleaning
print("\n[2/5] Data quality checks and cleaning...")

# Check for missing values
print("\n  Missing values check:")
for name, df in [('Customers', df_customers), ('Products', df_products), 
                 ('Stores', df_stores), ('Transactions', df_transactions)]:
    missing = df.isnull().sum().sum()
    print(f"    {name}: {missing} missing values")

# Remove duplicates
original_txn_count = len(df_transactions)
df_transactions = df_transactions.drop_duplicates(subset=['transaction_id'])
duplicates_removed = original_txn_count - len(df_transactions)
print(f"\n  Duplicates removed: {duplicates_removed}")

# Data validation
print("\n  Data validation:")

# Check for negative values
negative_amounts = df_transactions[df_transactions['total_amount'] < 0]
print(f"    Negative amounts: {len(negative_amounts)}")

# Check for invalid dates
df_transactions['transaction_date'] = pd.to_datetime(df_transactions['transaction_date'])
df_customers['registration_date'] = pd.to_datetime(df_customers['registration_date'])
df_stores['opening_date'] = pd.to_datetime(df_stores['opening_date'])

# Validate referential integrity
print("\n  Referential integrity check:")
invalid_customers = ~df_transactions['customer_id'].isin(df_customers['customer_id'])
invalid_products = ~df_transactions['product_id'].isin(df_products['product_id'])
invalid_stores = ~df_transactions['store_id'].isin(df_stores['store_id'])

print(f"    Invalid customer references: {invalid_customers.sum()}")
print(f"    Invalid product references: {invalid_products.sum()}")
print(f"    Invalid store references: {invalid_stores.sum()}")

# Data Transformations
print("\n[3/5] Applying data transformations...")

# Add calculated columns to transactions
df_transactions['transaction_datetime'] = pd.to_datetime(
    df_transactions['transaction_date'].astype(str) + ' ' + df_transactions['transaction_time']
)
df_transactions['year'] = df_transactions['transaction_date'].dt.year
df_transactions['month'] = df_transactions['transaction_date'].dt.month
df_transactions['quarter'] = df_transactions['transaction_date'].dt.quarter
df_transactions['day_of_week'] = df_transactions['transaction_date'].dt.day_name()
df_transactions['week_of_year'] = df_transactions['transaction_date'].dt.isocalendar().week

# Calculate profit
df_transactions_enriched = df_transactions.merge(
    df_products[['product_id', 'cost_price']], 
    on='product_id', 
    how='left'
)
df_transactions_enriched['cost_total'] = df_transactions_enriched['cost_price'] * df_transactions_enriched['quantity']
df_transactions_enriched['profit'] = df_transactions_enriched['net_amount'] - df_transactions_enriched['cost_total']
df_transactions_enriched['profit_margin'] = (
    df_transactions_enriched['profit'] / df_transactions_enriched['net_amount'] * 100
).round(2)

# Customer lifetime value calculation
customer_metrics = df_transactions.groupby('customer_id').agg({
    'transaction_id': 'count',
    'total_amount': 'sum',
    'transaction_date': ['min', 'max']
}).reset_index()
customer_metrics.columns = ['customer_id', 'total_purchases', 'lifetime_value', 'first_purchase', 'last_purchase']
customer_metrics['customer_tenure_days'] = (customer_metrics['last_purchase'] - customer_metrics['first_purchase']).dt.days

# Merge with customers
df_customers_enriched = df_customers.merge(customer_metrics, on='customer_id', how='left')
df_customers_enriched['total_purchases'] = df_customers_enriched['total_purchases'].fillna(0)
df_customers_enriched['lifetime_value'] = df_customers_enriched['lifetime_value'].fillna(0)

print("  ✓ Added date dimension columns")
print("  ✓ Calculated profit and margins")
print("  ✓ Computed customer lifetime value")

# Load data into SQLite database
print("\n[4/5] Loading data into SQLite database...")

# Drop existing tables if they exist
tables = ['customers', 'products', 'stores', 'transactions', 'fact_sales']
for table in tables:
    cursor.execute(f"DROP TABLE IF EXISTS {table}")

# Load dimension tables
df_customers_enriched.to_sql('customers', conn, index=False, if_exists='replace')
df_products.to_sql('products', conn, index=False, if_exists='replace')
df_stores.to_sql('stores', conn, index=False, if_exists='replace')

print("  ✓ Loaded customers table")
print("  ✓ Loaded products table")
print("  ✓ Loaded stores table")

# Load fact table (transactions)
df_transactions_enriched.to_sql('transactions', conn, index=False, if_exists='replace')
print("  ✓ Loaded transactions table")

# Create indexes for better query performance
print("\n[5/5] Creating database indexes...")

indexes = [
    "CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)",
    "CREATE INDEX IF NOT EXISTS idx_transactions_customer ON transactions(customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_transactions_product ON transactions(product_id)",
    "CREATE INDEX IF NOT EXISTS idx_transactions_store ON transactions(store_id)",
    "CREATE INDEX IF NOT EXISTS idx_customers_segment ON customers(customer_segment)",
    "CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)"
]

for idx_query in indexes:
    cursor.execute(idx_query)

print("  ✓ Created 6 indexes for query optimization")

# Create aggregated fact table for faster analytics
print("\n  Creating aggregated fact table...")

agg_query = """
CREATE TABLE fact_sales AS
SELECT 
    t.transaction_date,
    t.year,
    t.month,
    t.quarter,
    t.day_of_week,
    t.customer_id,
    c.customer_segment,
    c.city as customer_city,
    c.state as customer_state,
    t.product_id,
    p.category,
    p.subcategory,
    p.brand,
    t.store_id,
    s.region,
    s.city as store_city,
    t.quantity,
    t.gross_amount,
    t.discount_amount,
    t.net_amount,
    t.tax_amount,
    t.total_amount,
    t.profit,
    t.profit_margin,
    t.payment_method
FROM transactions t
LEFT JOIN customers c ON t.customer_id = c.customer_id
LEFT JOIN products p ON t.product_id = p.product_id
LEFT JOIN stores s ON t.store_id = s.store_id
"""

cursor.execute(agg_query)
cursor.execute("CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_sales(transaction_date)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_fact_category ON fact_sales(category)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_fact_region ON fact_sales(region)")

print("  ✓ Created fact_sales aggregated table")

# Commit and get statistics
conn.commit()

# Database statistics
print("\n" + "="*60)
print("ETL PIPELINE SUMMARY")
print("="*60)

stats_query = """
SELECT 
    'Customers' as table_name, COUNT(*) as record_count FROM customers
UNION ALL
SELECT 'Products', COUNT(*) FROM products
UNION ALL
SELECT 'Stores', COUNT(*) FROM stores
UNION ALL
SELECT 'Transactions', COUNT(*) FROM transactions
UNION ALL
SELECT 'Fact Sales', COUNT(*) FROM fact_sales
"""

stats = pd.read_sql(stats_query, conn)
print("\nDatabase Table Statistics:")
for _, row in stats.iterrows():
    print(f"  {row['table_name']}: {row['record_count']:,} records")

# Data quality summary
quality_query = """
SELECT 
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT product_id) as unique_products,
    COUNT(DISTINCT store_id) as unique_stores,
    MIN(transaction_date) as earliest_date,
    MAX(transaction_date) as latest_date,
    ROUND(SUM(total_amount), 2) as total_revenue,
    ROUND(AVG(total_amount), 2) as avg_transaction_value
FROM transactions
"""

quality_stats = pd.read_sql(quality_query, conn)
print("\nData Quality Summary:")
for col in quality_stats.columns:
    print(f"  {col}: {quality_stats[col].values[0]}")

conn.close()

print("\n" + "="*60)
print("✓ ETL pipeline completed successfully!")
print("="*60)
print(f"\nDatabase created: {db_path}")
print("\nTables created:")
print("  - customers (dimension)")
print("  - products (dimension)")
print("  - stores (dimension)")
print("  - transactions (fact)")
print("  - fact_sales (aggregated fact)")