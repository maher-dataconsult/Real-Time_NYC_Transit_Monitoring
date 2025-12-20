import duckdb
import dlt
import os

# 1. Configuration
os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE"] = "NYC_TRANSIT"
os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD"] = "@|EHSmK13DELAW"
os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME"] = "KILLSPORT13"
os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE"] = "ACCOUNTADMIN"

# FIX from previous step: Host must be Account ID only (no .snowflakecomputing.com suffix)
os.environ["DESTINATION__SNOWFLAKE__CREDENTIALS__HOST"] = "FYHTBNY-UZ80283"


# Helper function to get list of tables from DuckDB
def get_table_names(db_path):
    conn = duckdb.connect(db_path, read_only=True)
    tables = conn.execute("SHOW TABLES").fetchall()
    conn.close()
    return [t[0] for t in tables]

# 2. Define the Source
@dlt.resource(write_disposition="replace")
def load_table_resource(db_path, table_name):
    print(f"   Reading table: {table_name}...")
    conn = duckdb.connect(db_path, read_only=True)
    
    batch_size = 50000 
    offset = 0
    
    while True:
        query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
        
        # Get the result as an Arrow object
        # Depending on the version, this returns a Table OR a RecordBatchReader
        arrow_obj = conn.execute(query).arrow()
        
        # FIX: Check if it's a Reader (stream) and convert to Table so we can check .num_rows
        if hasattr(arrow_obj, 'read_all'):
            tbl = arrow_obj.read_all()
        else:
            tbl = arrow_obj
        
        # Now we can safely check if the batch is empty
        if tbl.num_rows == 0:
            break
            
        yield tbl
        offset += batch_size
        
    conn.close()

def run_pipeline():
    # 3. Setup
    db_file = 'nyc_transit_bronze.duckdb'
    
    if not os.path.exists(db_file):
        print(f"❌ Error: Database file '{db_file}' not found.")
        return

    tables = get_table_names(db_file)
    print(f"📂 Found tables to load: {tables}")

    # 4. Define the Pipeline
    pipeline = dlt.pipeline(
        pipeline_name="duckdb_to_snowflake",
        destination="snowflake",
        dataset_name="raw_data"
    )

    resources = [load_table_resource(db_file, t).with_name(t) for t in tables]

    if resources:
        print("🚀 Starting load to Snowflake...")
        info = pipeline.run(resources)
        print(info)
    else:
        print("⚠️ No tables found to load.")

if __name__ == "__main__":
    run_pipeline()