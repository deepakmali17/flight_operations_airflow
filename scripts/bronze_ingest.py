import requests 
import json
from datetime import datetime
from pathlib import Path

URL = "https://opensky-network.org/api/states/all"

def run_bronze_ingestion(**kwargs):
    response = requests.get(URL, timeout = 30)
    response.raise_for_status()
    data = response.json()

    timestamp = datetime.utcnow().strftime("%d%m%Y%H%M%S")
    path = Path(f"/opt/airflow/data/bronze/flights_{timestamp}.json")

    with open(path, "w") as f:
        json.dump(data,f)
    
    kwargs['ti'].xcom_push(key="bronze_file", value=str(path))


