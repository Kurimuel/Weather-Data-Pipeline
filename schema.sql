-- รันไฟล์นี้ใน Supabase > SQL Editor เพื่อสร้างตารางก่อนใช้งาน pipeline
--
-- ไฟล์นี้คือ schema "หลัง normalize" (ตรงกับ DECISIONS.md ข้อ 15)
-- ใช้สำหรับเริ่มต้นใหม่ที่ยังไม่มีข้อมูลเท่านั้น
--   - ถ้ามีข้อมูลเก่าที่เป็น schema ก่อน normalize อยู่แล้ว ให้ใช้
--     migration_001_normalize_locations.sql แทน (ไม่ต้องรันไฟล์นี้)
--   - ถ้าต้องการ similarity search (pgvector) ให้รัน
--     migration_002_add_pgvector.sql ต่อจากไฟล์นี้
--
-- ทุกคำสั่งใช้ IF NOT EXISTS จึงรันซ้ำได้โดยไม่ error
-- และไม่แตะต้องตารางที่มีอยู่แล้ว

-- ============================================================
-- ตาราง locations: เก็บข้อมูลเมือง 1 แถวต่อ 1 เมือง
-- ============================================================
CREATE TABLE IF NOT EXISTS locations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,   -- UNIQUE จำเป็น: get_or_create_location_id() ใช้ ON CONFLICT (name)
    country VARCHAR(100),
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL
);

-- ============================================================
-- ตาราง weather_readings: ข้อมูลอากาศ 1 แถวต่อ 1 เมือง ต่อ 1 ช่วงเวลา
-- ============================================================
CREATE TABLE IF NOT EXISTS weather_readings (
    id SERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES locations(id),
    reading_time TIMESTAMPTZ NOT NULL,       -- เวลาที่ "ข้อมูล" นี้เป็นของช่วงเวลานั้น (จาก API)
    temperature_c DECIMAL(4,1),
    humidity_percent INTEGER,
    wind_speed_kmh DECIMAL(4,1),
    fetched_at TIMESTAMPTZ DEFAULT NOW(),    -- เวลาที่ pipeline ไปดึงข้อมูลมาจริง
    CONSTRAINT weather_readings_location_id_reading_time_key
        UNIQUE (location_id, reading_time)   -- กันข้อมูลซ้ำ ถ้ารัน pipeline ซ้ำในรอบเดียวกัน
);

-- index ช่วยให้ query ตามช่วงเวลาเร็วขึ้น (จะมีประโยชน์ตอนทำ dashboard)
CREATE INDEX IF NOT EXISTS idx_weather_reading_time ON weather_readings(reading_time);

-- index สำหรับ JOIN / กรองตาม location_id
CREATE INDEX IF NOT EXISTS idx_weather_location_id ON weather_readings(location_id);
