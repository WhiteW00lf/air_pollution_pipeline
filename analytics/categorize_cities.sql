SELECT 
p.pm25,
p.pm10, 
d.city_name,
CASE 
WHEN p.pm25 > 150 THEN 'Hazardous'
WHEN p.pm25 > 100 THEN 'Unhealthy'
WHEN p.pm25 > 50 THEN 'Moderate'
WHEN p.pm25 > 0 THEN 'Good'
ELSE 'Unknown'
END AS pm25_category
FROM 
`deportfolio-486507.pollution_data.fact_pollution` p
INNER JOIN 
`deportfolio-486507.pollution_data.dim_city` d 
ON p.city_name = d.city_name;


