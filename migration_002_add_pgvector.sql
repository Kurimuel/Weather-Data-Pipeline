-- Migration 002: เพิ่ม pgvector extension + column สำหรับเก็บ embedding
-- รันหลังจาก migration_001 (normalize locations) เรียบร้อยแล้ว

-- ============================================================
-- STEP 1: เปิดใช้ pgvector extension
-- ============================================================
CREATE EXTENSION IF NOT EXISTS vector;

-- ============================================================
-- STEP 2: เพิ่ม column เก็บ text description + vector embedding
-- ============================================================
-- text description: คำอธิบายสภาพอากาศแบบข้อความ เช่น "hot and humid"
-- แปลงมาจากตัวเลข (temperature_c, humidity_percent, wind_speed_kmh)
ALTER TABLE weather_readings ADD COLUMN IF NOT EXISTS weather_description TEXT;

-- vector(384): ขนาด 384 มิติ ตรงกับ model "all-MiniLM-L6-v2" ที่จะใช้
-- (model ฟรีขนาดเล็ก เหมาะกับงาน demo แบบนี้ ไม่ต้องใช้ model ใหญ่)
ALTER TABLE weather_readings ADD COLUMN IF NOT EXISTS embedding vector(384);

-- ============================================================
-- STEP 3: สร้าง index สำหรับ similarity search ให้เร็วขึ้น
-- ============================================================
-- ivfflat คือ index type ที่ pgvector ใช้สำหรับ approximate nearest
-- neighbor search - เร็วกว่าการเทียบทุกแถวแบบตรงๆ (brute force) มาก
-- เมื่อข้อมูลเยอะขึ้น (lists=100 คือค่าเริ่มต้นที่เหมาะกับข้อมูลระดับ
-- หลักพัน-หมื่นแถว)
CREATE INDEX IF NOT EXISTS idx_weather_embedding
    ON weather_readings USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);
