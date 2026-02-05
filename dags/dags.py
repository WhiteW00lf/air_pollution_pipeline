from google.cloud import storage
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from run_bg import make_tables
import requests
import pandas as pd
import os

load_dotenv()

places = [
    "Delhi",
    "Mumbai",
    "Bangalore",
    "Chennai",
    "Kolkata",
    "Hyderabad",
    "Pune",
    "Ahmedabad",
    "Jaipur",
    "Lucknow",
    "Kanpur",
    "Nagpur",
    "Thane",
    "Bhopal",
    "Patna",
    "Shillong",
    "Dehradun",
    "Manali",
    "Itanagar",
    "Shillong",
    "Gangtok",
    "Agartala",
    "Aizawl",
    "Kohima",
]
BUCKET_NAME = "pollution_raw"


def fetch_pollution_data():
    api_key = os.getenv("APP_KEY")
    for place in places:
        url = f"https://api.waqi.info/feed/{place}/?token={api_key}"
        response = requests.get(url)
        if response.status_code == 200:
            # print(f"{place} - 200")
            data = response.json()
            # created_at = data_from_api['data'].get('time').get('iso')

            if data["status"] != "ok":
                print(f"Data not available for {place}.")
                continue

            aqi = data["data"]["aqi"]

            city = data["data"]["city"]["name"]

            pm25 = (
                data["data"]["iaqi"]["pm25"]["v"]
                if "pm25" in data["data"]["iaqi"]
                else None
            )
            pm10 = (
                data["data"]["iaqi"]["pm10"]["v"]
                if "pm10" in data["data"]["iaqi"]
                else None
            )
            o3 = (
                data["data"]["iaqi"]["o3"]["v"]
                if "o3" in data["data"]["iaqi"]
                else None
            )
            no2 = (
                data["data"]["iaqi"]["no2"]["v"]
                if "no2" in data["data"]["iaqi"]
                else None
            )

            created_at = (
                data["data"]["time"]["iso"] if "iso" in data["data"]["time"] else None
            )

            poll_df = pd.DataFrame(
                {
                    "created_at": [created_at],
                    "city": [city],
                    "aqi": [aqi],
                    "pm25": [pm25],
                    "pm10": [pm10],
                    "o3": [o3],
                    "no2": [no2],
                }
            )

            poll_df.to_csv(
                "raw/pollution_data.csv", index=False, mode="a", header=False
            )

        else:
            print(
                f"Failed to fetch data for {place}. Status code: {response.status_code}"
            )



def upload_to_gcs():
    today = pd.Timestamp.now().strftime("%Y-%m-%d")
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"raw/{today}/pollution_data.csv")
    if blob.exists():
        return print("File already exists")
    else:
        blob.upload_from_filename("raw/pollution_data.csv")
        print("File uploaded to GCS successfully.")



with DAG (
    dag_id="pollution_data_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 2, 5),
    catchup=False,

    
) as dag:
    
    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_pollution_data,
    )

    upload_data = PythonOperator(
        task_id="upload_data",
        python_callable=upload_to_gcs,
    )

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=make_tables,
    )

    fetch_data >> upload_data >> create_tables