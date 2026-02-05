from google.cloud import bigquery

client = bigquery.Client()
dataset_id = "deportfolio-486507.pollution_data"

with open("create_stg_pollution.sql", "r") as f:
    create_table_query = f.read()
    job = client.query(create_table_query)
    job.result()
    print("Staging table created successfully.")