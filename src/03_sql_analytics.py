"""
Retail Sales Analytics Pipeline - Step 3: SQL Analytics
Advanced SQL analysis using CTEs, window functions, and complex queries
"""

import pandas as pd
import sqlite3
from datetime import datetime

print("="*60)
print("RETAIL SALES ANALYTICS - SQL ANALYTICS")
print("="*60)

# Connect to database
conn = sqlite3.connect('data/retail_analytics.db')

# Create reports directory
import os
os.makedirs('reports', exist_ok=True)

# Open report file
report_file = open('reports/executive_summary.txt', 'w', encoding='utf-8')

def write_report(text):
    """Write to both console and report file"""
    print(text)
    report_file.write(text + '\n')

# Header
write_report("="*80)
write_report("RETAIL SALES ANALYTICS - EXECUTIVE SUMMARY REPORT")
write_report(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
write_report("="*80)

# 1. Overall Business Performance
write_report("\n" + "="*80)
write_report("1. OVERALL BUSINESS PERFORMANCE")
write_report("="*80)

query1 = """
WITH monthly_metrics AS (
    SELECT 
        strftime('%Y-%m', transaction_date) as month,
        COUNT(DISTINCT transaction_id) as total_transactions,
        COUNT(DISTINCT customer_id) as unique_customers,
        ROUND(SUM(total_amount), 2) as revenue,
        ROUND(SUM(profit), 2) as profit,
        ROUND(AVG(total_amount), 2) as avg_transaction_value,
        ROUND(SUM(profit) / SUM(total_amount) * 100, 2) as profit_margin_pct
    FROM transactions
    GROUP BY month
),
overall_metrics AS (
    SELECT 
        SUM(total_transactions) as total_txn,
        SUM(unique_customers) as total_customers,
        SUM(revenue) as total_revenue,
        SUM(profit) as total_profit,
        AVG(avg_transaction_value) as avg_txn_value,
        AVG(profit_margin_pct) as avg_margin
    FROM monthly_metrics
)
SELECT * FROM overall_metrics
"""

overall = pd.read_sql(query1, conn)
write_report(f"\nTotal Revenue: ${overall['total_revenue'].values[0]:,.2f}")
write_report(f"Total Profit: ${overall['total_profit'].values[0]:,.2f}")
write_report(f"Average Profit Margin: {overall['avg_margin'].values[0]:.2f}%")
write_report(f"Total Transactions: {overall['total_txn'].values[0]:,.0f}")
write_report(f"Average Transaction Value: ${overall['avg_txn_value'].values[0]:,.2f}")

# 2. Revenue Trends with Year-over-Year Growth
write_report("\n" + "="*80)
write_report("2. REVENUE TRENDS & YEAR-OVER-YEAR GROWTH")
write_report("="*80)

query2 = """
WITH monthly_revenue AS (
    SELECT 
        year,
        month,
        strftime('%Y-%m', transaction_date) as year_month,
        ROUND(SUM(total_amount), 2) as revenue
    FROM transactions
    GROUP BY year, month, year_month
),
yoy_comparison AS (
    SELECT 
        curr.year_month,
        curr.revenue as current_revenue,
        prev.revenue as prev_year_revenue,
        ROUND(((curr.revenue - prev.revenue) / prev.revenue * 100), 2) as yoy_growth_pct,
        ROUND(curr.revenue - prev.revenue, 2) as revenue_change
    FROM monthly_revenue curr
    LEFT JOIN monthly_revenue prev 
        ON curr.month = prev.month 
        AND curr.year = prev.year + 1
    WHERE prev.revenue IS NOT NULL
    ORDER BY curr.year_month DESC
    LIMIT 12
)
SELECT * FROM yoy_comparison
"""

yoy = pd.read_sql(query2, conn)
write_report("\nRecent 12-Month Year-over-Year Performance:")
write_report("-" * 80)
for _, row in yoy.iterrows():
    write_report(f"{row['year_month']}: ${row['current_revenue']:,.2f} "
                f"(YoY: {row['yoy_growth_pct']:+.2f}%)")

# 3. Top Performing Products
write_report("\n" + "="*80)
write_report("3. TOP PERFORMING PRODUCTS")
write_report("="*80)

query3 = """
WITH product_performance AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        p.brand,
        COUNT(DISTINCT t.transaction_id) as units_sold,
        ROUND(SUM(t.total_amount), 2) as total_revenue,
        ROUND(SUM(t.profit), 2) as total_profit,
        ROUND(AVG(t.profit_margin), 2) as avg_margin_pct,
        ROW_NUMBER() OVER (ORDER BY SUM(t.total_amount) DESC) as revenue_rank
    FROM transactions t
    JOIN products p ON t.product_id = p.product_id
    GROUP BY p.product_id, p.product_name, p.category, p.brand
)
SELECT * FROM product_performance
WHERE revenue_rank <= 10
ORDER BY revenue_rank
"""

top_products = pd.read_sql(query3, conn)
write_report("\nTop 10 Products by Revenue:")
write_report("-" * 80)
for _, row in top_products.iterrows():
    write_report(f"#{row['revenue_rank']}: {row['product_name'][:50]}")
    write_report(f"   Category: {row['category']} | Brand: {row['brand']}")
    write_report(f"   Revenue: ${row['total_revenue']:,.2f} | Profit: ${row['total_profit']:,.2f} | Margin: {row['avg_margin_pct']:.2f}%")
    write_report("")

# 4. Category Performance Analysis
write_report("\n" + "="*80)
write_report("4. PRODUCT CATEGORY ANALYSIS")
write_report("="*80)

query4 = """
SELECT 
    category,
    COUNT(*) as total_transactions,
    ROUND(SUM(total_amount), 2) as revenue,
    ROUND(SUM(profit), 2) as profit,
    ROUND(SUM(total_amount) * 100.0 / (SELECT SUM(total_amount) FROM fact_sales), 2) as revenue_share_pct,
    ROUND(AVG(profit_margin), 2) as avg_margin_pct
FROM fact_sales
GROUP BY category
ORDER BY revenue DESC
"""

categories = pd.read_sql(query4, conn)
write_report("\nCategory Performance:")
write_report("-" * 80)
for _, row in categories.iterrows():
    write_report(f"{row['category']}:")
    write_report(f"   Revenue: ${row['revenue']:,.2f} ({row['revenue_share_pct']:.1f}% of total)")
    write_report(f"   Profit: ${row['profit']:,.2f} | Avg Margin: {row['avg_margin_pct']:.2f}%")
    write_report("")

# 5. Customer Segmentation Analysis (RFM)
write_report("\n" + "="*80)
write_report("5. CUSTOMER SEGMENTATION (RFM ANALYSIS)")
write_report("="*80)

query5 = """
WITH customer_rfm AS (
    SELECT 
        customer_id,
        MAX(julianday('2024-12-31') - julianday(transaction_date)) as recency_days,
        COUNT(DISTINCT transaction_id) as frequency,
        ROUND(SUM(total_amount), 2) as monetary_value
    FROM transactions
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT 
        customer_id,
        recency_days,
        frequency,
        monetary_value,
        NTILE(5) OVER (ORDER BY recency_days DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) as f_score,
        NTILE(5) OVER (ORDER BY monetary_value ASC) as m_score
    FROM customer_rfm
),
customer_segments AS (
    SELECT 
        CASE 
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2 THEN 'Promising'
            WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
            WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
            ELSE 'Regular'
        END as segment,
        COUNT(*) as customer_count,
        ROUND(AVG(monetary_value), 2) as avg_lifetime_value,
        ROUND(SUM(monetary_value), 2) as total_segment_value
    FROM rfm_scores
    GROUP BY segment
    ORDER BY total_segment_value DESC
)
SELECT * FROM customer_segments
"""

segments = pd.read_sql(query5, conn)
write_report("\nCustomer Segments:")
write_report("-" * 80)
for _, row in segments.iterrows():
    write_report(f"{row['segment']}:")
    write_report(f"   Customers: {row['customer_count']:,} | Avg LTV: ${row['avg_lifetime_value']:,.2f}")
    write_report(f"   Total Value: ${row['total_segment_value']:,.2f}")
    write_report("")

# 6. Regional Performance Analysis
write_report("\n" + "="*80)
write_report("6. REGIONAL PERFORMANCE ANALYSIS")
write_report("="*80)

query6 = """
WITH regional_performance AS (
    SELECT 
        region,
        COUNT(DISTINCT store_id) as num_stores,
        COUNT(*) as total_transactions,
        ROUND(SUM(total_amount), 2) as total_revenue,
        ROUND(SUM(profit), 2) as total_profit,
        ROUND(AVG(total_amount), 2) as avg_transaction_value,
        ROUND(SUM(profit) / SUM(total_amount) * 100, 2) as profit_margin_pct
    FROM fact_sales
    GROUP BY region
    ORDER BY total_revenue DESC
)
SELECT * FROM regional_performance
"""

regions = pd.read_sql(query6, conn)
write_report("\nRegional Performance:")
write_report("-" * 80)
for _, row in regions.iterrows():
    write_report(f"{row['region']} Region:")
    write_report(f"   Stores: {row['num_stores']} | Revenue: ${row['total_revenue']:,.2f}")
    write_report(f"   Profit: ${row['total_profit']:,.2f} | Margin: {row['profit_margin_pct']:.2f}%")
    write_report("")

# 7. Seasonal Trends
write_report("\n" + "="*80)
write_report("7. SEASONAL SALES PATTERNS")
write_report("="*80)

query7 = """
WITH monthly_sales AS (
    SELECT 
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
        ROUND(AVG(monthly_revenue), 2) as avg_monthly_revenue,
        ROUND(SUM(total_transactions) / COUNT(DISTINCT year), 0) as avg_monthly_transactions
    FROM (
        SELECT 
            year,
            month,
            SUM(total_amount) as monthly_revenue,
            COUNT(*) as total_transactions
        FROM transactions
        GROUP BY year, month
    )
    GROUP BY month
    ORDER BY month
)
SELECT * FROM monthly_sales
"""

seasonality = pd.read_sql(query7, conn)
write_report("\nAverage Performance by Month:")
write_report("-" * 80)
for _, row in seasonality.iterrows():
    write_report(f"{row['month_name']}: ${row['avg_monthly_revenue']:,.2f} avg revenue "
                f"({row['avg_monthly_transactions']:,.0f} transactions)")

# Closing
write_report("\n" + "="*80)
write_report("END OF REPORT")
write_report("="*80)

report_file.close()
conn.close()

print("\n" + "="*60)
print("✓ SQL analytics completed successfully!")
print("="*60)
print("\nReport generated: reports/executive_summary.txt")