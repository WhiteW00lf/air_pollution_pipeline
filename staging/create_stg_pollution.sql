CREATE EXTERNAL TABLE IF NOT EXISTS `deportfolio-486507.pollution_data.stg_pollution` (

    created_at TIMESTAMP ,
    city STRING ,
    aqi FLOAT64 ,
    pm25 FLOAT64 ,
    pm10 FLOAT64 ,
    o3 FLOAT64 ,
    no2 FLOAT64 




)
OPTIONS (
    format = 'CSV',
    uris = ['gs://pollution_raw/raw/*/pollution_data.csv'],
    skip_leading_rows = 1,
    field_delimiter = ','

);