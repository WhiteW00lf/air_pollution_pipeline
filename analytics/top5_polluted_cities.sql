SELECT 
d.city_name,
f.aqi 
FROM `deportfolio-486507.pollution_data.fact_pollution` f
INNER JOIN `deportfolio-486507.pollution_data.dim_city` d
ON f.city_name = d.city_name
ORDER BY f.aqi DESC
LIMIT 5;