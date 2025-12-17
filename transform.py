import sqlite3
import pandas as pd
import os

def run_transform():
    BASE_DATA_DIR = "C:/Users/User/Desktop/adms_finals/data"
    STAGING_DIR = os.path.join(BASE_DATA_DIR, "Staging")
    TRANS_DIR = os.path.join(BASE_DATA_DIR, "Transformation")
    
    os.makedirs(TRANS_DIR, exist_ok=True)
    trans_conn = sqlite3.connect(os.path.join(TRANS_DIR, "transformation_layer.db"))
    
    # Process Japan (Standardizing to USD)
    j_conn = sqlite3.connect(os.path.join(STAGING_DIR, "japan_staging.db"))
    j_sales = pd.read_sql("SELECT * FROM stg_sales", j_conn)
    j_items = pd.read_sql("SELECT * FROM stg_items", j_conn)
    
    j_df = j_sales.merge(j_items, left_on='product_id', right_on='id')
    j_df['Total_USD'] = j_df['quantity'] * j_df['price'] * 0.0092
    j_df['Country'] = 'Japan'
    j_df.to_sql("transformed_japan", trans_conn, if_exists='replace', index=False)
    
    # Process Myanmar
    m_conn = sqlite3.connect(os.path.join(STAGING_DIR, "myanmar_staging.db"))
    m_sales = pd.read_sql("SELECT * FROM stg_sales", m_conn)
    m_items = pd.read_sql("SELECT * FROM stg_items", m_conn)
    
    m_df = m_sales.merge(m_items, left_on='product_id', right_on='id')
    m_df['Total_USD'] = m_df['quantity'] * m_df['price']
    m_df['Country'] = 'Myanmar'
    m_df.to_sql("transformed_myanmar", trans_conn, if_exists='replace', index=False)
    
    print("--- 2. Transform: Standardized data saved to data/transformation/ ---")
    trans_conn.close()