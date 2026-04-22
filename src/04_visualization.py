"""
Retail Sales Analytics Pipeline - Step 4: Visualization
Generate charts and visualizations for insights
"""

import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime

print("="*60)
print("RETAIL SALES ANALYTICS - VISUALIZATIONS")
print("="*60)

# Create charts directory
os.makedirs('reports/charts', exist_ok=True)

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

# Connect to database
conn = sqlite3.connect('data/retail_analytics.db')

# 1. Revenue Trend Over Time
print("\n[1/8] Generating revenue trend chart...")

query1 = """
SELECT 
    strftime('%Y-%m', transaction_date) as month,
    ROUND(SUM(total_amount), 2) as revenue,
    ROUND(SUM(profit), 2) as profit
FROM transactions
GROUP BY month
ORDER BY month
"""

df_trend = pd.read_sql(query1, conn)

fig, ax = plt.subplots(figsize=(14, 6))
ax.plot(df_trend['month'], df_trend['revenue'], marker='o', linewidth=2, label='Revenue', color='#2E86AB')
ax.plot(df_trend['month'], df_trend['profit'], marker='s', linewidth=2, label='Profit', color='#A23B72')
ax.set_xlabel('Month', fontsize=12, fontweight='bold')
ax.set_ylabel('Amount ($)', fontsize=12, fontweight='bold')
ax.set_title('Revenue and Profit Trends Over Time', fontsize=14, fontweight='bold', pad=20)
ax.legend(fontsize=11)
ax.grid(True, alpha=0.3)
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig('reports/charts/01_revenue_trend.png', dpi=300, bbox_inches='tight')
plt.close()
print("  ✓ Saved: 01_revenue_trend.png")

# 2. Category Performance
print("\n[2/8] Generating category performance chart...")

query2 = """
SELECT 
    category,
    ROUND(SUM(total_amount), 2) as revenue
FROM fact_sales
GROUP BY category
ORDER BY revenue DESC
LIMIT 10
"""

df_category = pd.read_sql(query2, conn)

fig, ax = plt.subplots(figsize=(12, 7))
bars = ax.barh(df_category['category'], df_category['revenue'], color=sns.color_palette("viridis", len(df_category)))
ax.set_xlabel('Revenue ($)', fontsize=12, fontweight='bold')
ax.set_ylabel('Category', fontsize=12, fontweight='bold')
ax.set_title('Top 10 Product Categories by Revenue', fontsize=14, fontweight='bold', pad=20)

# Add value labels
for i, (bar, value) in enumerate(zip(bars, df_category['revenue'])):
    ax.text(value, bar.get_y() + bar.get_height()/2, f'${value:,.0f}', 
            va='center', ha='left', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('reports/charts/02_category_performance.png', dpi=300, bbox_inches='tight')
plt.close()
print("  ✓ Saved: 02_category_performance.png")

# 3. Regional Sales Distribution
print("\n[3/8] Generating regional distribution chart...")

query3 = """
SELECT 
    region,
    ROUND(SUM(total_amount), 2) as revenue,
    COUNT(*) as transactions
FROM fact_sales
GROUP BY region
ORDER BY revenue DESC
"""

df_region = pd.read_sql(query3, conn)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Revenue pie chart
colors = sns.color_palette("Set2", len(df_region))
wedges, texts, autotexts = ax1.pie(df_region['revenue'], labels=df_region['region'], autopct='%1.1f%%',
                                     colors=colors, startangle=90, textprops={'fontsize': 11, 'fontweight': 'bold'})
ax1.set_title('Revenue Distribution by Region', fontsize=13, fontweight='bold', pad=20)

# Transaction count bar chart
bars = ax2.bar(df_region['region'], df_region['transactions'], color=colors)
ax2.set_xlabel('Region', fontsize=11, fontweight='bold')
ax2.set_ylabel('Number of Transactions', fontsize=11, fontweight='bold')
ax2.set_title('Transaction Count by Region', fontsize=13, fontweight='bold', pad=20)
ax2.tick_params(axis='x', rotation=45)

# Add value labels
for bar in bars:
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(height):,}', ha='center', va='bottom', fontsize=9, fontweight='bold')

plt.tight_layout()
plt.savefig('reports/charts/03_regional_distribution.png', dpi=300, bbox_inches='tight')
plt.close()
print("  ✓ Saved: 03_regional_distribution.png")

# 4. Customer Segmentation
print("\n[4/8] Generating customer segmentation chart...")

query4 = """
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
        COUNT(*) as customer_count
    FROM rfm_scores
    GROUP BY segment
)
SELECT * FROM customer_segments
ORDER BY customer_count DESC
"""

df_segments = pd.read_sql(query4, conn)

fig, ax = plt.subplots(figsize=(10, 7))
colors = ['#00CC96', '#636EFA', '#EF553B', '#FFA15A', '#AB63FA', '#19D3F3']
bars = ax.bar(df_segments['segment'], df_segments['customer_count'], color=colors[:len(df_segments)])
ax.set_xlabel('Customer Segment', fontsize=12, fontweight='bold')
ax.set_ylabel('Number of Customers', fontsize=12, fontweight='bold')
ax.set_title('Customer Segmentation (RFM Analysis)', fontsize=14, fontweight='bold', pad=20)
plt.xticks(rotation=45, ha='right')

# Add value labels
for bar in bars:
    height = bar.get_height()
    ax.text(bar.get_x() + bar.get_width()/2., height,
            f'{int(height):,}', ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('reports/charts/04_customer_segmentation.png', dpi=300, bbox_inches='tight')
plt.close()
print("  ✓ Saved: 04_customer_segmentation.png")

# 5. Monthly Sales Heatmap
print("\n[5/8] Generating monthly sales heatmap...")

query5 = """
SELECT 
    year,
    month,
    ROUND(SUM(total_amount), 2) as revenue
FROM transactions
GROUP BY year, month
ORDER BY year, month
"""

df_monthly = pd.read_sql(query5, conn)
pivot_data = df_monthly.pivot(index='month', columns='year', values='revenue')

fig, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(pivot_data, annot=True, fmt='.0f', cmap='YlOrRd', cbar_kws={'label': 'Revenue ($)'}, 
            linewidths=0.5, ax=ax)
ax.set_xlabel('Year', fontsize=12, fontweight='bold')
ax.set_ylabel('Month', fontsize=12, fontweight='bold')
ax.set_title('Monthly Revenue Heatmap', fontsize=14, fontweight='bold', pad=20)
ax.set_yticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
plt.tight_layout()
plt.savefig('reports/charts/05_monthly_heatmap.png', dpi=300, bbox_inches='tight')
plt.close()
print("  ✓ Saved: 05_monthly_heatmap.png")

# 6. Top 10 Products
print("\n[6/8] Generating top products chart...")

query6 = """
SELECT 
    p.product_name,
    ROUND(SUM(t.total_amount), 2) as revenue
FROM transactions t
JOIN products p ON t.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY revenue DESC
LIMIT 10
"""

df_products = pd.read_sql(query6, conn)
df_products['product_short'] = df_products['product_name'].str[:40]

fig, ax = plt.subplots(figsize=(12, 7))
bars = ax.barh(df_products['product_short'], df_products['revenue'], 
               color=sns.color_palette("rocket", len(df_products)))
ax.set_xlabel('Revenue ($)', fontsize=12, fontweight='bold')
ax.set_ylabel('Product', fontsize=12, fontweight='bold')
ax.set_title('Top 10 Products by Revenue', fontsize=14, fontweight='bold', pad=20)

for i, (bar, value) in enumerate(zip(bars, df_products['revenue'])):
    ax.text(value, bar.get_y() + bar.get_height()/2, f'${value:,.0f}', 
            va='center', ha='left', fontsize=9, fontweight='bold')

plt.tight_layout()
plt.savefig('reports/charts/06_top_products.png', dpi=300, bbox_inches='tight')
plt.close()
print("  ✓ Saved: 06_top_products.png")

# 7. Day of Week Analysis
print("\n[7/8] Generating day of week analysis...")

query7 = """
SELECT 
    day_of_week,
    COUNT(*) as transaction_count,
    ROUND(AVG(total_amount), 2) as avg_transaction_value
FROM transactions
GROUP BY day_of_week
"""

df_dow = pd.read_sql(query7, conn)
day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
df_dow['day_of_week'] = pd.Categorical(df_dow['day_of_week'], categories=day_order, ordered=True)
df_dow = df_dow.sort_values('day_of_week')

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

# Transaction count
ax1.bar(df_dow['day_of_week'], df_dow['transaction_count'], 
        color=sns.color_palette("mako", len(df_dow)))
ax1.set_xlabel('Day of Week', fontsize=11, fontweight='bold')
ax1.set_ylabel('Number of Transactions', fontsize=11, fontweight='bold')
ax1.set_title('Transaction Volume by Day of Week', fontsize=13, fontweight='bold', pad=15)
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

for i, (x, y) in enumerate(zip(df_dow['day_of_week'], df_dow['transaction_count'])):
    ax1.text(i, y, f'{y:,}', ha='center', va='bottom', fontsize=9, fontweight='bold')

# Average transaction value
ax2.plot(df_dow['day_of_week'], df_dow['avg_transaction_value'], 
         marker='o', linewidth=2.5, markersize=8, color='#E63946')
ax2.set_xlabel('Day of Week', fontsize=11, fontweight='bold')
ax2.set_ylabel('Average Transaction Value ($)', fontsize=11, fontweight='bold')
ax2.set_title('Average Transaction Value by Day of Week', fontsize=13, fontweight='bold', pad=15)
ax2.grid(True, alpha=0.3)
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

plt.tight_layout()
plt.savefig('reports/charts/07_day_of_week_analysis.png', dpi=300, bbox_inches='tight')
plt.close()
print("  ✓ Saved: 07_day_of_week_analysis.png")

# 8. Payment Method Distribution
print("\n[8/8] Generating payment method chart...")

query8 = """
SELECT 
    payment_method,
    COUNT(*) as transaction_count,
    ROUND(SUM(total_amount), 2) as revenue
FROM transactions
GROUP BY payment_method
ORDER BY revenue DESC
"""

df_payment = pd.read_sql(query8, conn)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

# Pie chart for transaction count
colors = sns.color_palette("pastel", len(df_payment))
wedges, texts, autotexts = ax1.pie(df_payment['transaction_count'], labels=df_payment['payment_method'], 
                                     autopct='%1.1f%%', colors=colors, startangle=90,
                                     textprops={'fontsize': 10, 'fontweight': 'bold'})
ax1.set_title('Transaction Count by Payment Method', fontsize=12, fontweight='bold', pad=20)

# Bar chart for revenue
bars = ax2.bar(df_payment['payment_method'], df_payment['revenue'], color=colors)
ax2.set_xlabel('Payment Method', fontsize=11, fontweight='bold')
ax2.set_ylabel('Revenue ($)', fontsize=11, fontweight='bold')
ax2.set_title('Revenue by Payment Method', fontsize=12, fontweight='bold', pad=20)
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

for bar in bars:
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height,
             f'${height:,.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold')

plt.tight_layout()
plt.savefig('reports/charts/08_payment_methods.png', dpi=300, bbox_inches='tight')
plt.close()
print("  ✓ Saved: 08_payment_methods.png")

conn.close()

print("\n" + "="*60)
print("✓ Visualizations completed successfully!")
print("="*60)
print("\nCharts saved in: reports/charts/")
print("  - 01_revenue_trend.png")
print("  - 02_category_performance.png")
print("  - 03_regional_distribution.png")
print("  - 04_customer_segmentation.png")
print("  - 05_monthly_heatmap.png")
print("  - 06_top_products.png")
print("  - 07_day_of_week_analysis.png")
print("  - 08_payment_methods.png")