SET SERVEROUTPUT ON;
EXEC predict_temperature_recovery('Olsztyn');
EXEC show_volatility_rank;
EXEC analyze_wind_storm('Olsztyn', 2);
EXEC analyze_humidity_buckets('Warszawa', 7);
EXEC show_current_comfort('Kraków');

SELECT 
    city_name,
    observed_at_utc,
    temperature_c as "Celsjusz",
    convert_temperature(temperature_c) as "Fahrenheit",       
    convert_temperature(temperature_c, 'K') as "Kelwin"      
FROM weather w
JOIN cities c ON w.city_id = c.city_id
ORDER BY observed_at_utc DESC
FETCH FIRST 5 ROWS ONLY;