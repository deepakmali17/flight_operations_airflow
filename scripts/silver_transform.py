import json
import pandas as pd
from pathlib import Path

def run_silver_transform(**kwargs):
    execution_date = kwargs['ds_nodash']
  
    bronze_file = kwargs['ti'].xcom_pull(key = "bronze_file", task_ids="bronze_ingest")
    if not bronze_file:
        raise ValueError("bronze file path not found in Xcom")
    
    silver_path = Path("/opt/airflow/data/silver")
    silver_path.mkdir(parents=True, exist_ok=True)

    with open(bronze_file) as f:
        raw = json.load(f)
    df_raw = pd.DataFrame(raw["states"])
    df_raw.columns = [
        "icao24","callsign","origin_country","time_position","last_contact","longitude","latitude","baro_altitude","on_ground","velocity","true_track","vertical_rate"
        ,"sensors","geo_altitude","squawk","spi","position_source"]
    
    df = df_raw[
        ["icao24", "origin_country","velocity","geo_altitude"]
    ]

    output_file = silver_path/f"flights_silver_{execution_date}.csv"
    df.to_csv(output_file,index=False)

    kwargs['ti'].xcom_push(key="silver_path", value=str(output_file))
