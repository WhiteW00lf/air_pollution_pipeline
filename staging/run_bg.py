from google.cloud import bigquery
from pathlib import Path

client = bigquery.Client()
dataset_id = "deportfolio-486507.pollution_data"
fct_path = Path(__file__).parent.parent /"facts" /"create_fact_tables.sql"
dim_path = Path(__file__).parent.parent /"facts" /"create_dim_tables.sql"

with open("create_stg_pollution.sql", "r") as f:
    create_table_query = f.read()
    job = client.query(create_table_query)
    job.result()
    print("Staging table created successfully.")


with open(fct_path, "r") as f:
    create_table_query = f.read()
    job = client.query(create_table_query)
    job.result()
    print("Facts table created successfully.")


with open(dim_path, "r") as f:
    create_table_query = f.read()
    job = client.query(create_table_query)
    job.result()
    print("Dimension table created successfully.")