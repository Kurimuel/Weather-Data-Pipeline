
-- อุณหภูมิเฉลี่ยรายวัน พร้อม window function เทียบกับวันก่อนหน้า
SELECT 
    location_name,
    DATE(reading_time) as date,
    AVG(temperature_c) as avg_temp,
    AVG(temperature_c) - LAG(AVG(temperature_c)) OVER (
        PARTITION BY location_name ORDER BY DATE(reading_time)
    ) as temp_change_from_yesterday
FROM weather_readings
GROUP BY location_name, DATE(reading_time)
ORDER BY date;
