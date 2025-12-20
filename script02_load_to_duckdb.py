import duckdb
import os
from glob import glob

def load_to_bronze():
    # 1. Define paths
    source_folder = 'batch_files'
    db_file = 'nyc_transit_bronze.duckdb'

    # 2. Connect to DuckDB (this creates the file if it doesn't exist)
    # The read_only=False flag allows us to write data
    con = duckdb.connect(db_file, read_only=False)

    print(f"🔌 Connected to database: {db_file}")
    
    # 3. Get list of all .txt files (GTFS data often uses .txt for CSVs)
    files = glob(os.path.join(source_folder, "*.txt"))

    if not files:
        print(f"⚠️  No .txt files found in '{source_folder}'. Please check the directory.")
        return

    print(f"📂 Found {len(files)} files to process...")

    # 4. Iterate and Load
    for file_path in files:
        try:
            # Extract filename without extension to use as Table Name
            # e.g., 'batch_files/agency.txt' -> 'agency'
            base_name = os.path.basename(file_path)
            table_name = os.path.splitext(base_name)[0]

            print(f"   Processing: {table_name}...", end=" ")

            # SQL Query to Create Table directly from CSV
            # read_csv_auto handles type inference and headers automatically
            # OR REPLACE ensures we overwrite old data if re-running the batch
            query = f"""
                CREATE OR REPLACE TABLE {table_name} AS 
                SELECT * FROM read_csv_auto('{file_path}', header=True);
            """
            
            con.execute(query)
            print("✅ Done.")

        except Exception as e:
            print(f"\n❌ Error loading {file_path}: {e}")

    # 5. Verify the load
    print("\n📊 Summary of Loaded Tables:")
    tables = con.execute("SHOW TABLES").fetchall()
    
    for table in tables:
        t_name = table[0]
        count = con.execute(f"SELECT COUNT(*) FROM {t_name}").fetchone()[0]
        print(f"   - {t_name}: {count:,} rows")

    # 6. Close connection
    con.close()
    print("\n🚀 Batch ingestion to Bronze Layer complete.")

if __name__ == "__main__":
    load_to_bronze()