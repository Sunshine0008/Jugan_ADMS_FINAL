import sqlite3
import pandas as pd
import os

def run_load():
    BASE_DATA_DIR = "C:/Users/User/Desktop/adms_finals/data"
    TRANS_DIR = os.path.join(BASE_DATA_DIR, "Transformation")
    PRES_DIR = os.path.join(BASE_DATA_DIR, "Presentation")
    
    os.makedirs(PRES_DIR, exist_ok=True)
    
    trans_conn = sqlite3.connect(os.path.join(TRANS_DIR, "transformation_layer.db"))
    big_table_conn = sqlite3.connect(os.path.join(PRES_DIR, "BIG_TABLE.db"))
    
    df = pd.concat([
        pd.read_sql("SELECT * FROM transformed_japan", trans_conn),
        pd.read_sql("SELECT * FROM transformed_myanmar", trans_conn)
    ], ignore_index=True)
    
    df.to_sql("consolidated_sales", big_table_conn, if_exists='replace', index=False)
    print(f"--- 3. Load: Final BIG_TABLE created in {PRES_DIR} ---")
    
    trans_conn.close()
    big_table_conn.close()

if __name__ == "__main__":
    run_load()