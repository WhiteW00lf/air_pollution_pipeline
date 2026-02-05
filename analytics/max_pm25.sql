SELECT 
f.created_date,
d.city_name,
f.pm25,
f.pm10
FROM `deportfolio-486507.pollution_data.fact_pollution` f
INNER JOIN `deportfolio-486507.pollution_data.dim_city` d
ON f.city_name = d.city_name
WHERE f.pm25 IS NOT NULL 
AND f.pm10 IS NOT NULL 
AND f.created_date = '2026-02-05'
ORDER BY f.pm25 DESC
