
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

-- ไฟล์นี้เก็บ query สำหรับ "วิเคราะห์" ข้อมูล (รันบ่อยๆ ตามต้องการ)
-- ต่างจาก schema.sql ที่รันครั้งเดียวตอนตั้งค่า database

-- ============================================================
-- Query 1: อุณหภูมิเฉลี่ยรายวัน พร้อมเทียบกับวันก่อนหน้า
-- ใช้ window function (LAG) เพื่อดึงค่าของ "แถวก่อนหน้า" มาเทียบ
-- โดยไม่ต้อง JOIN ตารางกับตัวเอง (self-join) ซึ่งจะซับซ้อนกว่านี้มาก
-- ============================================================
SELECT
    location_name,
    DATE(reading_time) AS date,
    AVG(temperature_c) AS avg_temp,
    AVG(temperature_c) - LAG(AVG(temperature_c)) OVER (
        PARTITION BY location_name ORDER BY DATE(reading_time)
    ) AS temp_change_from_yesterday
FROM weather_readings
GROUP BY location_name, DATE(reading_time)
ORDER BY location_name, date;


-- ============================================================
-- Query 2: ช่วงเวลาที่อุณหภูมิเปลี่ยนแปลงเร็วที่สุด (ระหว่าง reading ติดกัน)
-- มีประโยชน์สำหรับตรวจจับความผิดปกติ (anomaly) เช่น sensor error
-- ============================================================
SELECT
    location_name,
    reading_time,
    temperature_c,
    temperature_c - LAG(temperature_c) OVER (
        PARTITION BY location_name ORDER BY reading_time
    ) AS temp_diff_from_previous_reading
FROM weather_readings
ORDER BY ABS(
    temperature_c - LAG(temperature_c) OVER (
        PARTITION BY location_name ORDER BY reading_time
    )
) DESC
LIMIT 10;


-- ============================================================
-- Query 3: จำนวน reading ต่อวัน (เช็คว่า pipeline รันครบตามที่ตั้งไว้ไหม)
-- มีประโยชน์สำหรับ monitoring ว่า automation (Phase 4) ทำงานปกติหรือเปล่า
-- ============================================================
SELECT
    location_name,
    DATE(reading_time) AS date,
    COUNT(*) AS reading_count
FROM weather_readings
GROUP BY location_name, DATE(reading_time)
ORDER BY date DESC;
