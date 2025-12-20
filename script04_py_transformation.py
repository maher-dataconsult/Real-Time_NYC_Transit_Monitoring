import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

# Create connection
conn = snowflake.connector.connect(
    user="KILLSPORT13",
    password="@|EHSmK13DELAW",
    account="FYHTBNY-UZ80283",
    warehouse="COMPUTE_WH",
    database="NYC_TRANSIT",
    schema="STAGING"
)

# extract data
query = "SELECT * FROM STG_AGENCY"
df1=pd.read_sql(query, conn)
STG_AGENCY=df1.copy()

query = "SELECT * FROM STG_CALENDAR"
df2=pd.read_sql(query, conn)
STG_CALENDAR=df2.copy()

query = "SELECT * FROM STG_CALENDAR_DATES"
df3=pd.read_sql(query, conn)
STG_CALENDAR_DATES=df3.copy()

query = "SELECT * FROM STG_ROUTES"
df4=pd.read_sql(query, conn)
STG_ROUTES=df4.copy()

query = "SELECT * FROM STG_STOPS"
df6=pd.read_sql(query, conn)
STG_STOPS=df6.copy()

query = "SELECT * FROM STG_STOP_TIMES"
df7=pd.read_sql(query, conn)
STG_STOP_TIMES=df7.copy()

query = "SELECT * FROM STG_TRIPS"
df9=pd.read_sql(query, conn)
STG_TRIPS=df9.copy()

# 1-STG_ROUTES
STG_ROUTES.drop(["AGENCY_ID", "ROUTE_URL", "ROUTE_COLOR", "ROUTE_TEXT_COLOR"], axis=1, inplace=True)

# 2-STG_STOPS
STG_STOPS['LOCATION_TYPE'] = STG_STOPS['LOCATION_TYPE'].fillna(0)
STG_STOPS['PARENT_STATION'] = STG_STOPS['PARENT_STATION'].replace({None: -1})
STG_STOPS['LOCATION_TYPE'] = STG_STOPS['LOCATION_TYPE'].astype(int)

# 3-STG_STOP_TIMES
STG_STOP_TIMES = STG_STOP_TIMES.sort_values(["TRIP_ID", "STOP_SEQUENCE"])

# Shift previous row's arrival time within each trip
STG_STOP_TIMES["PREV_ARR"] = STG_STOP_TIMES.groupby("TRIP_ID")["ARRIVAL_TIME_SECONDS"].shift(1)

# For the first stop of each trip, PREV_ARR = NaN → duration = 0
STG_STOP_TIMES["INCREMENT"] = (STG_STOP_TIMES["DEPARTURE_TIME_SECONDS"] - STG_STOP_TIMES["PREV_ARR"]).fillna(0)

# Accumulative trip time
STG_STOP_TIMES["ACCUMULATIVE_TRIP_TIME_IN_SECONDS"] = STG_STOP_TIMES.groupby("TRIP_ID")["INCREMENT"].cumsum()
STG_STOP_TIMES['ACCUMULATIVE_TRIP_TIME_IN_SECONDS'] = STG_STOP_TIMES['ACCUMULATIVE_TRIP_TIME_IN_SECONDS'].astype(int)
STG_STOP_TIMES["ACCUMULATIVE_TRIP_TIME"] = STG_STOP_TIMES['ACCUMULATIVE_TRIP_TIME_IN_SECONDS'].apply(lambda x: f"{x//3600:02d}:{(x%3600)//60:02d}:{x%60:02d}")

# Drop columns
STG_STOP_TIMES = STG_STOP_TIMES.drop(['ARRIVAL_TIME_SECONDS', 'DEPARTURE_TIME_SECONDS','PREV_ARR','INCREMENT'], axis=1)


#############################
######end transformation#####
#############################

# Data Modeling
# 1.DIM_ROUTES
DIM_ROUTES = STG_ROUTES.copy()

# 2.DIM_STOPS
DIM_STOPS = STG_STOPS.copy()

# 3.DIM_TRIPS (Previously #4)
DIM_TRIPS = STG_TRIPS.copy()

# 4.FACT_STOP_TIMES (Previously #6)
FACT_STOP_TIMES = STG_STOP_TIMES.copy()

# Add ROUTE_ID and SHAPE_ID from TRIPS
# Note: Keeping SHAPE_ID here as it comes from TRIPS, even though DIM_SHAPES table is removed.
FACT_STOP_TIMES = FACT_STOP_TIMES.merge(
    DIM_TRIPS[['TRIP_ID', 'ROUTE_ID', 'SHAPE_ID', 'SERVICE_ID']],
    on='TRIP_ID',
    how='left'
)

tables = {
    'DIM_ROUTES': DIM_ROUTES,
    'DIM_STOPS': DIM_STOPS,
    'DIM_TRIPS': DIM_TRIPS,
    'FACT_STOP_TIMES': FACT_STOP_TIMES
}

cur = conn.cursor()
new_schema = 'NYC_TRANSIT'
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {new_schema}")
print(f"Schema {new_schema} created or already exists")

cur.execute(f"USE SCHEMA {new_schema}")

def create_table_from_df(df, table_name, schema):
    col_defs = []
    for col, dtype in zip(df.columns, df.dtypes):
        if 'int' in str(dtype):
            col_type = 'NUMBER'
        elif 'float' in str(dtype):
            col_type = 'FLOAT'
        elif 'bool' in str(dtype):
            col_type = 'BOOLEAN'
        else:
            col_type = 'VARCHAR'
        col_defs.append(f"{col.upper()} {col_type}")
    
    col_defs_sql = ", ".join(col_defs)
    create_sql = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({col_defs_sql})"
    cur.execute(create_sql)
    print(f"Table {schema}.{table_name} created")

# Create tables
for table_name, df in tables.items():
    create_table_from_df(df, table_name, new_schema)

DIM_STOPS['PARENT_STATION'] = DIM_STOPS['PARENT_STATION'].astype(str)

success, nchunks, nrows, _ = write_pandas(
        conn,
        df=DIM_ROUTES,
        table_name='DIM_ROUTES',
        schema='NYC_TRANSIT'
    )
print(f"Inserted {nrows} rows into {new_schema}.DIM_ROUTES")

success, nchunks, nrows, _ = write_pandas(
        conn,
        df=DIM_STOPS,
        table_name='DIM_STOPS',
        schema='NYC_TRANSIT'
    )
print(f"Inserted {nrows} rows into {new_schema}.DIM_STOPS")

success, nchunks, nrows, _ = write_pandas(
        conn,
        df=DIM_TRIPS,
        table_name='DIM_TRIPS',
        schema='NYC_TRANSIT'
    )
print(f"Inserted {nrows} rows into {new_schema}.DIM_TRIPS")

success, nchunks, nrows, _ = write_pandas(
        conn,
        df=FACT_STOP_TIMES,
        table_name='FACT_STOP_TIMES',
        schema='NYC_TRANSIT'
    )
print(f"Inserted {nrows} rows into {new_schema}.FACT_STOP_TIMES")


pk=[
    'ALTER TABLE NYC_TRANSIT.DIM_TRIPS ADD CONSTRAINT PK_DIM_TRIPS PRIMARY KEY (TRIP_ID);',
    'ALTER TABLE NYC_TRANSIT.DIM_ROUTES ADD CONSTRAINT PK_DIM_ROUTES PRIMARY KEY (ROUTE_ID);',
    'ALTER TABLE NYC_TRANSIT.DIM_STOPS ADD CONSTRAINT PK_DIM_STOPS PRIMARY KEY (STOP_ID);',
    'ALTER TABLE NYC_TRANSIT.FACT_STOP_TIMES ADD CONSTRAINT PK_FACT_STOP_TIMES PRIMARY KEY (TRIP_ID,STOP_SEQUENCE);'    
   ]

for pk_sql in pk:
    try:
        cur.execute(pk_sql)
        print(f"Executed: {pk_sql}")
    except Exception as e:
        print(f"Error executing {pk_sql}: {e}")


foreign_keys = [
    "ALTER TABLE NYC_TRANSIT.FACT_STOP_TIMES ADD CONSTRAINT FK_STOP_TIMES_TRIPS FOREIGN KEY (TRIP_ID) REFERENCES NYC_TRANSIT.DIM_TRIPS(TRIP_ID)",
    "ALTER TABLE NYC_TRANSIT.FACT_STOP_TIMES ADD CONSTRAINT FK_STOP_TIMES_ROUTES FOREIGN KEY (ROUTE_ID) REFERENCES NYC_TRANSIT.DIM_ROUTES(ROUTE_ID)",
    "ALTER TABLE NYC_TRANSIT.FACT_STOP_TIMES ADD CONSTRAINT FK_STOP_TIMES_STOPS FOREIGN KEY (STOP_ID) REFERENCES NYC_TRANSIT.DIM_STOPS(STOP_ID)",
    "ALTER TABLE NYC_TRANSIT.DIM_STOPS ADD CONSTRAINT FK_PARENT_STOP FOREIGN KEY (PARENT_STATION) REFERENCES NYC_TRANSIT.DIM_STOPS(STOP_ID)"
]

for fk_sql in foreign_keys:
    try:
        cur.execute(fk_sql)
        print(f"Executed: {fk_sql}")
    except Exception as e:
        print(f"Error executing {fk_sql}: {e}")