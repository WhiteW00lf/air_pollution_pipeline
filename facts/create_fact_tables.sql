CREATE OR REPLACE TABLE `deportfolio-486507.pollution_data.fact_pollution_data`
PARTITION BY created_date
CLUSTER BY city_name 
AS
SELECT 
    CAST(created_at AS DATE) AS created_date,
    city AS city_name,
    aqi,
    pm25,
    pm10,
    o3,
    no2
FROM `deportfolio-486507.pollution_data.stg_pollution_data`
WHERE created_at IS NOT NULL
AND city IS NOT NULL
AND aqi IS NOT NULL
AND pm25 IS NOT NULL
AND pm10 IS NOT NULL
AND o3 IS NOT NULL
AND no2 IS NOT NULL;