CREATE OR REPLACE TABLE `deportfolio-486507.pollution_data.dim_city`
AS
SELECT DISTINCT
    FARM_FINGERPRINT(city) AS city_id,
    city AS city_name
FROM `deportfolio-486507.pollution_data.stg_pollution`
WHERE city IS NOT NULL;