-- รันไฟล์นี้ใน Supabase > SQL Editor เพื่อสร้างตารางก่อนใช้งาน pipeline

CREATE TABLE IF NOT EXISTS weather_readings (
    id SERIAL PRIMARY KEY,
    location_name VARCHAR(100) NOT NULL,
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL,
    reading_time TIMESTAMP NOT NULL,       -- เวลาที่ "ข้อมูล" นี้เป็นของช่วงเวลานั้น (จาก API)
    temperature_c DECIMAL(4,1),
    humidity_percent INTEGER,
    wind_speed_kmh DECIMAL(4,1),
    fetched_at TIMESTAMP DEFAULT NOW(),    -- เวลาที่ pipeline ไปดึงข้อมูลมาจริง
    UNIQUE(location_name, reading_time)    -- กันข้อมูลซ้ำ ถ้ารัน pipeline ซ้ำในรอบเดียวกัน
);

-- index ช่วยให้ query ตามช่วงเวลาเร็วขึ้น (จะมีประโยชน์ตอนทำ dashboard)
CREATE INDEX IF NOT EXISTS idx_weather_reading_time ON weather_readings(reading_time);

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
