"""
Retail Sales Analytics Pipeline - Step 1: Data Generation
Generates realistic synthetic retail transaction data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
import os
import random

# Initialize Faker for realistic data
fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# Configuration
NUM_TRANSACTIONS = 500000
NUM_CUSTOMERS = 50000
NUM_PRODUCTS = 1000
NUM_STORES = 50
START_DATE = '2022-01-01'
END_DATE = '2024-12-31'

print("="*60)
print("RETAIL SALES ANALYTICS - DATA GENERATION")
print("="*60)
print(f"\nConfiguration:")
print(f"  Transactions: {NUM_TRANSACTIONS:,}")
print(f"  Customers: {NUM_CUSTOMERS:,}")
print(f"  Products: {NUM_PRODUCTS:,}")
print(f"  Stores: {NUM_STORES}")
print(f"  Date Range: {START_DATE} to {END_DATE}")
print("\n" + "-"*60)

# Create directories
os.makedirs('data/raw', exist_ok=True)
os.makedirs('data/processed', exist_ok=True)

# 1. Generate Customers
print("\n[1/4] Generating customer data...")
customers = []
for i in range(NUM_CUSTOMERS):
    customers.append({
        'customer_id': f'CUST{i+1:06d}',
        'customer_name': fake.name(),
        'email': fake.email(),
        'phone': fake.phone_number(),
        'address': fake.street_address(),
        'city': fake.city(),
        'state': fake.state_abbr(),
        'zip_code': fake.zipcode(),
        'registration_date': fake.date_between(start_date='-5y', end_date='today'),
        'customer_segment': np.random.choice(['Gold', 'Silver', 'Bronze', 'Regular'], 
                                            p=[0.05, 0.15, 0.30, 0.50])
    })

df_customers = pd.DataFrame(customers)
df_customers.to_csv('data/raw/customers.csv', index=False)
print(f"  ✓ Generated {len(df_customers):,} customers")

# 2. Generate Products
print("\n[2/4] Generating product data...")
categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 
              'Toys', 'Food & Beverage', 'Health & Beauty', 'Automotive', 'Office Supplies']

products = []
for i in range(NUM_PRODUCTS):
    category = np.random.choice(categories)
    base_price = np.random.uniform(5, 500)
    
    products.append({
        'product_id': f'PROD{i+1:05d}',
        'product_name': f"{fake.catch_phrase()} {category.split()[0]} Item",
        'category': category,
        'subcategory': f"{category} - Type {np.random.randint(1, 6)}",
        'brand': fake.company(),
        'unit_price': round(base_price, 2),
        'cost_price': round(base_price * 0.6, 2),
        'stock_quantity': np.random.randint(0, 1000),
        'supplier': fake.company()
    })

df_products = pd.DataFrame(products)
df_products.to_csv('data/raw/products.csv', index=False)
print(f"  ✓ Generated {len(df_products):,} products")

# 3. Generate Stores
print("\n[3/4] Generating store data...")
regions = ['North', 'South', 'East', 'West', 'Central']
stores = []

for i in range(NUM_STORES):
    region = np.random.choice(regions)
    stores.append({
        'store_id': f'STORE{i+1:03d}',
        'store_name': f"{fake.city()} {np.random.choice(['Mall', 'Outlet', 'Downtown', 'Plaza'])}",
        'region': region,
        'city': fake.city(),
        'state': fake.state_abbr(),
        'store_size_sqft': np.random.randint(5000, 50000),
        'opening_date': fake.date_between(start_date='-10y', end_date='-1y'),
        'manager': fake.name()
    })

df_stores = pd.DataFrame(stores)
df_stores.to_csv('data/raw/stores.csv', index=False)
print(f"  ✓ Generated {len(df_stores):,} stores")

# 4. Generate Transactions
print("\n[4/4] Generating transaction data...")
print("  This may take 30-60 seconds...")

transactions = []
start_dt = datetime.strptime(START_DATE, '%Y-%m-%d')
end_dt = datetime.strptime(END_DATE, '%Y-%m-%d')
date_range = (end_dt - start_dt).days

# Customer purchase patterns
customer_weights = np.random.pareto(2, NUM_CUSTOMERS) + 1
customer_weights = customer_weights / customer_weights.sum()

# Product popularity
product_weights = np.random.zipf(1.5, NUM_PRODUCTS)
product_weights = product_weights / product_weights.sum()

for i in range(NUM_TRANSACTIONS):
    random_days = np.random.randint(0, date_range)
    transaction_date = start_dt + timedelta(days=random_days)
    
    month = transaction_date.month
    if month in [11, 12]:
        if np.random.random() > 0.3:
            pass
        else:
            continue
    
    customer_id = np.random.choice(df_customers['customer_id'].values, p=customer_weights)
    product_id = np.random.choice(df_products['product_id'].values, p=product_weights)
    store_id = np.random.choice(df_stores['store_id'].values)
    
    product_price = df_products[df_products['product_id'] == product_id]['unit_price'].values[0]
    
    quantity = np.random.choice([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 
                                p=[0.50, 0.25, 0.12, 0.05, 0.03, 0.02, 0.01, 0.01, 0.005, 0.005])
    
    discount_pct = np.random.choice([0, 5, 10, 15, 20], p=[0.60, 0.20, 0.10, 0.07, 0.03])
    
    gross_amount = round(product_price * quantity, 2)
    discount_amount = round(gross_amount * (discount_pct / 100), 2)
    net_amount = round(gross_amount - discount_amount, 2)
    tax_amount = round(net_amount * 0.08, 2)
    total_amount = round(net_amount + tax_amount, 2)
    
    transactions.append({
        'transaction_id': f'TXN{i+1:08d}',
        'transaction_date': transaction_date.strftime('%Y-%m-%d'),
        'transaction_time': f"{np.random.randint(8, 22):02d}:{np.random.randint(0, 60):02d}:{np.random.randint(0, 60):02d}",
        'customer_id': customer_id,
        'product_id': product_id,
        'store_id': store_id,
        'quantity': quantity,
        'unit_price': product_price,
        'gross_amount': gross_amount,
        'discount_percentage': discount_pct,
        'discount_amount': discount_amount,
        'net_amount': net_amount,
        'tax_amount': tax_amount,
        'total_amount': total_amount,
        'payment_method': np.random.choice(['Credit Card', 'Debit Card', 'Cash', 'Mobile Payment'], 
                                          p=[0.45, 0.30, 0.15, 0.10])
    })
    
    if (i + 1) % 100000 == 0:
        print(f"  Progress: {i+1:,} / {NUM_TRANSACTIONS:,} transactions")

df_transactions = pd.DataFrame(transactions)
df_transactions.to_csv('data/raw/transactions.csv', index=False)
print(f"\n  ✓ Generated {len(df_transactions):,} transactions")

# Summary Statistics
print("\n" + "="*60)
print("DATA GENERATION SUMMARY")
print("="*60)
print(f"\nTotal Revenue: ${df_transactions['total_amount'].sum():,.2f}")
print(f"Average Transaction Value: ${df_transactions['total_amount'].mean():,.2f}")
print(f"Date Range: {df_transactions['transaction_date'].min()} to {df_transactions['transaction_date'].max()}")

print("\n" + "="*60)
print("✓ Data generation completed successfully!")
print("="*60)
print("\nFiles saved:")
print("  - data/raw/customers.csv")
print("  - data/raw/products.csv")
print("  - data/raw/stores.csv")
print("  - data/raw/transactions.csv")