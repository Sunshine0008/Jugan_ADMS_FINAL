import pandas as pd
import sqlite3
import os

def run_extract():
    # Setup nested folder path
    BASE_DATA_DIR = "C:/Users/User/Desktop/adms_finals/data"
    STAGING_DIR = os.path.join(BASE_DATA_DIR, "Staging")
    
    # Create folders if they don't exist
    os.makedirs(STAGING_DIR, exist_ok=True)

    jobs = [
        {
            "db_path": os.path.join(STAGING_DIR, "japan_staging.db"),
            "files": {
                "stg_sales": "C:/Users/User/Desktop/adms_finals/source/japan_store/sales_data.csv",
                "stg_items": "C:/Users/User/Desktop/adms_finals/source/japan_store/japan_items.csv",
                "stg_customers": "C:/Users/User/Desktop/adms_finals/source/japan_store/japan_Customers.csv",
                "stg_branch": "C:/Users/User/Desktop/adms_finals/source/japan_store/japan_branch.csv",
                "stg_payment": "C:/Users/User/Desktop/adms_finals/source/japan_store/japan_payment.csv"
            }
        },
        {
            "db_path": os.path.join(STAGING_DIR, "myanmar_staging.db"),
            "files": {
                "stg_sales": "C:/Users/User/Desktop/adms_finals/source/myanmar_store/sales_data_mynr.csv",
                "stg_items": "C:/Users/User/Desktop/adms_finals/source/myanmar_store/myanmar_items.csv",
                "stg_customers": "C:/Users/User/Desktop/adms_finals/source/myanmar_store/myanmar_customers.csv",
                "stg_branch": "C:/Users/User/Desktop/adms_finals/source/myanmar_store/myanmar_branch.csv",
                "stg_payment": "C:/Users/User/Desktop/adms_finals/source/myanmar_store/myanmar_payment.csv"
            }
        }
    ]

    print("--- 1. Extract: Loading to data/staging/ ---")
    for job in jobs:
        conn = sqlite3.connect(job["db_path"])
        for table, csv in job["files"].items():
            if os.path.exists(csv):
                df = pd.read_csv(csv)
                df.columns = [c.replace("'", "").strip() for c in df.columns]
                df.to_sql(table, conn, if_exists='replace', index=False)
                print(f"  ✅ {table} created in staging folder")
            else:
                print(f"  ❌ Missing: {csv}")
        conn.close()

if __name__ == "__main__":
    run_extract()