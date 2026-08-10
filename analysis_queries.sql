-- ไฟล์นี้เก็บ query สำหรับ "วิเคราะห์" ข้อมูล (รันบ่อยๆ ตามต้องการ)
-- ต่างจาก schema.sql ที่รันครั้งเดียวตอนตั้งค่า database
--
-- หลัง normalize schema (แยกตาราง locations ออกมา) query ที่อยาก
-- ได้ชื่อเมืองต้อง JOIN กับตาราง locations เสมอ (ดู DECISIONS.md ข้อ 15)

-- ============================================================
-- Query 1: อุณหภูมิเฉลี่ยรายวัน พร้อม window function เทียบกับวันก่อนหน้า
-- ============================================================
SELECT
    l.name AS location_name,
    l.country,
    DATE(wr.reading_time) AS date,
    AVG(wr.temperature_c) AS avg_temp,
    AVG(wr.temperature_c) - LAG(AVG(wr.temperature_c)) OVER (
        PARTITION BY l.name ORDER BY DATE(wr.reading_time)
    ) AS temp_change_from_yesterday
FROM weather_readings wr
JOIN locations l ON wr.location_id = l.id
GROUP BY l.name, l.country, DATE(wr.reading_time)
ORDER BY l.name, date;


-- ============================================================
-- Query 2: ช่วงเวลาที่อุณหภูมิเปลี่ยนแปลงเร็วที่สุด (ระหว่าง reading ติดกัน)
-- มีประโยชน์สำหรับตรวจจับความผิดปกติ (anomaly) เช่น sensor error
-- ============================================================
SELECT
    l.name AS location_name,
    wr.reading_time,
    wr.temperature_c,
    wr.temperature_c - LAG(wr.temperature_c) OVER (
        PARTITION BY l.name ORDER BY wr.reading_time
    ) AS temp_diff_from_previous_reading
FROM weather_readings wr
JOIN locations l ON wr.location_id = l.id
ORDER BY ABS(
    wr.temperature_c - LAG(wr.temperature_c) OVER (
        PARTITION BY l.name ORDER BY wr.reading_time
    )
) DESC
LIMIT 10;


-- ============================================================
-- Query 3: จำนวน reading ต่อวัน (เช็คว่า pipeline รันครบตามที่ตั้งไว้ไหม)
-- มีประโยชน์สำหรับ monitoring ว่า automation ทำงานปกติหรือเปล่า
-- ============================================================
SELECT
    l.name AS location_name,
    DATE(wr.reading_time) AS date,
    COUNT(*) AS reading_count
FROM weather_readings wr
JOIN locations l ON wr.location_id = l.id
GROUP BY l.name, DATE(wr.reading_time)
ORDER BY date DESC, l.name;


-- ============================================================
-- Query 4: เปรียบเทียบอุณหภูมิเฉลี่ยระหว่างประเทศ (ใช้ประโยชน์จาก
-- normalize schema ที่มี country เก็บแยกไว้ใน locations)
-- ============================================================
SELECT
    l.country,
    COUNT(DISTINCT l.name) AS num_cities,
    AVG(wr.temperature_c) AS avg_temp_across_country
FROM weather_readings wr
JOIN locations l ON wr.location_id = l.id
GROUP BY l.country
ORDER BY avg_temp_across_country DESC;


-- ============================================================
-- Query 5: เมืองที่อุณหภูมิผันผวนมากที่สุด (ใช้ standard deviation)
-- ============================================================
SELECT
    l.name AS location_name,
    l.country,
    ROUND(STDDEV(wr.temperature_c)::numeric, 2) AS temp_std_dev,
    ROUND(AVG(wr.temperature_c)::numeric, 2) AS avg_temp
FROM weather_readings wr
JOIN locations l ON wr.location_id = l.id
GROUP BY l.name, l.country
HAVING COUNT(*) >= 5
ORDER BY temp_std_dev DESC;
