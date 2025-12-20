import subprocess
from pathlib import Path
from datetime import timedelta
from prefect import flow, task


SCRIPTS_DIR = Path(__file__).parent


@task(name="Run GTFS Scrapper")
def run_script_01():
    subprocess.run(["python", SCRIPTS_DIR / "script01_gtfs_scrapper.py"], check=True)


@task(name="Load to DuckDB")
def run_script_02():
    subprocess.run(["python", SCRIPTS_DIR / "script02_load_to_duckdb.py"], check=True)


@task(name="Load to Snowflake")
def run_script_03():
    subprocess.run(["python", SCRIPTS_DIR / "script03_load_to_snowflake.py"], check=True)


@task(name="Run dbt")
def run_dbt():
    # Run dbt and capture output (dbt has a logging bug that causes exit code 1 even on success)
    result = subprocess.run(
        ["dbt", "run"], 
        cwd=SCRIPTS_DIR / "dbt_nyc_transit", 
        capture_output=True, 
        text=True
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    
    # Check if dbt actually succeeded (look for success message, not exit code)
    if "Completed successfully" not in result.stdout and result.returncode != 0:
        raise RuntimeError(f"dbt run failed with exit code {result.returncode}")


@task(name="Python Transformation")
def run_script_04():
    subprocess.run(["python", SCRIPTS_DIR / "script04_py_transformation.py"], check=True)


@flow(name="NYC Transit Pipeline", log_prints=True)
def nyc_transit_pipeline():
    run_script_01()
    run_script_02()
    run_script_03()
    run_dbt()
    run_script_04()


if __name__ == "__main__":
    from prefect.client.schemas.schedules import IntervalSchedule
    
    # Run immediately on startup
    print("🚀 Running pipeline immediately...")
    nyc_transit_pipeline()
    
    # Then serve with 10-day schedule
    print("📅 Starting scheduler (every 10 days)...")
    nyc_transit_pipeline.serve(
        name="nyc-transit-pipeline",
        schedule=IntervalSchedule(interval=timedelta(days=10))
    )
