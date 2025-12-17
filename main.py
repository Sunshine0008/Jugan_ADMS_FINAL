import os
import sys
import time
from extract import run_extract
from transform import run_transform
from load import run_load

def run_pipeline():
    print("🚀 Starting ETL Pipeline...")
    start_time = time.time()

    try:
        # 1. Extraction Phase
        print("\n--- Phase 1: Extraction ---")
        run_extract()
        
        # 2. Transformation Phase
        print("\n--- Phase 2: Transformation ---")
        run_transform()
        
        # 3. Loading Phase
        print("\n--- Phase 3: Loading ---")
        run_load()

        end_time = time.time()
        duration = round(end_time - start_time, 2)
        
        print(f"\n✅ ETL Pipeline completed successfully in {duration} seconds!")
        print(f"📍 All databases are located in the '{os.getcwd()}/data' folder.")

    except Exception as e:
        print(f"\n❌ Pipeline Failed!")
        print(f"Error details: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()