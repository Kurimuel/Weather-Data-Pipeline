-- Migration 001: Normalize locations ออกจาก weather_readings
--
-- รันไฟล์นี้ "แทน" schema.sql ถ้ามีข้อมูลอยู่แล้วในตาราง weather_readings
-- แบบเก่า (ที่มี location_name, latitude, longitude อยู่ในตารางเดียวกัน)
-- ไฟล์นี้จะย้ายข้อมูลเดิมไปตาราง locations ใหม่โดยไม่เสียข้อมูล
--
-- รันทีละ statement ทีละก้อน ไม่ควรรันรวดเดียวทั้งไฟล์ เผื่อต้องเช็ค
-- ผลลัพธ์ระหว่างทาง (โดยเฉพาะ step 2 ที่ควรเช็คว่าข้อมูลย้ายถูกต้อง
-- ก่อนจะ DROP column เดิมทิ้งใน step ท้ายสุด)

-- ============================================================
-- STEP 1: สร้างตาราง locations ใหม่
-- ============================================================
CREATE TABLE IF NOT EXISTS locations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    country VARCHAR(100),
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(9,6) NOT NULL
);

-- ============================================================
-- STEP 2: ย้ายเมืองที่ไม่ซ้ำกันจาก weather_readings เดิม เข้า locations
-- ============================================================
INSERT INTO locations (name, latitude, longitude)
SELECT DISTINCT location_name, latitude, longitude
FROM weather_readings
ON CONFLICT (name) DO NOTHING;

-- เช็คผลลัพธ์ตรงนี้ก่อนไปต่อ:
-- SELECT * FROM locations;

-- ============================================================
-- STEP 3: เพิ่ม column location_id เข้า weather_readings (ยังไม่บังคับ NOT NULL)
-- ============================================================
ALTER TABLE weather_readings ADD COLUMN IF NOT EXISTS location_id INTEGER;

-- ============================================================
-- STEP 4: เติมค่า location_id ให้ทุกแถวที่มีอยู่แล้ว โดยจับคู่จากชื่อเมือง
-- ============================================================
UPDATE weather_readings wr
SET location_id = l.id
FROM locations l
WHERE wr.location_name = l.name
  AND wr.location_id IS NULL;

-- เช็คว่าทุกแถวมี location_id ครบก่อนไปต่อ (ผลลัพธ์ควรเป็น 0):
-- SELECT COUNT(*) FROM weather_readings WHERE location_id IS NULL;

-- ============================================================
-- STEP 5: บังคับ NOT NULL + สร้าง foreign key constraint
-- ============================================================
ALTER TABLE weather_readings ALTER COLUMN location_id SET NOT NULL;
ALTER TABLE weather_readings
    ADD CONSTRAINT fk_weather_location
    FOREIGN KEY (location_id) REFERENCES locations(id);

-- ============================================================
-- STEP 6: ลบ UNIQUE constraint เก่าที่อิง location_name แล้วสร้างใหม่
-- ที่อิง location_id แทน
-- ============================================================
ALTER TABLE weather_readings DROP CONSTRAINT IF EXISTS weather_readings_location_name_reading_time_key;
ALTER TABLE weather_readings ADD CONSTRAINT weather_readings_location_id_reading_time_key
    UNIQUE (location_id, reading_time);

-- ============================================================
-- STEP 7: สร้าง index ใหม่สำหรับ location_id
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_weather_location_id ON weather_readings(location_id);

-- ============================================================
-- STEP 8: ลบ column เก่าที่ไม่ต้องใช้แล้วทิ้ง (ทำหลังเช็คทุกอย่างถูกต้องแล้วเท่านั้น)
-- ⚠️ ขั้นตอนนี้ลบข้อมูลถาวร แนะนำให้ backup หรือเช็คผลลัพธ์จนมั่นใจก่อนรัน
-- ============================================================
ALTER TABLE weather_readings DROP COLUMN IF EXISTS location_name;
ALTER TABLE weather_readings DROP COLUMN IF EXISTS latitude;
ALTER TABLE weather_readings DROP COLUMN IF EXISTS longitude;
