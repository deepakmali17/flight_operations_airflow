import pandas as pd
from pathlib import Path

def run_gold_aggregate(**kwargs):
    execution_date = kwargs['ds_nodash']
    silver_file = kwargs['ti'].xcom_pull(key="silver_path", task_ids="silver_transform")

    gold_path = Path("/opt/airflow/data/gold")
    gold_path.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(silver_file)
    agg = (
        df.groupby("origin_country")
        .agg(
            total_flights=("icao24", "count"),
            avg_velocity=("velocity", "mean")).reset_index()
    )


    gold_file = gold_path/f"total_flights_{execution_date}.csv"
    agg.to_csv(gold_file, index=False)
    kwargs['ti'].xcom_push(key="gold_file", value=str(gold_file))
