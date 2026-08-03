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
