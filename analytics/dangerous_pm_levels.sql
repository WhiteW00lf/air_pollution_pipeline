WITH dangerous_levels AS (
    SELECT 
        created_date,
        city_name,
        pm25
 
    FROM `deportfolio-486507.pollution_data.fact_pollution`
    WHERE pm25 > 150
)

SELECT
    d.city_name,
    dl.pm25
    FROM dangerous_levels dl
    INNER JOIN `deportfolio-486507.pollution_data.dim_city` d
    ON dl.city_name = d.city_name
    ORDER BY dl.pm25 DESC