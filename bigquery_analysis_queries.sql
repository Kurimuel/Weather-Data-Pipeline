-- Query ตัวอย่างสำหรับรันใน BigQuery Console
-- ใช้ syntax เฉพาะของ BigQuery ที่ต่างจาก PostgreSQL (ดู analysis_queries.sql
-- เปรียบเทียบ) เช่น backtick ` ` แทน double quote สำหรับชื่อ table/column
-- แบบเต็ม project.dataset.table

-- ============================================================
-- Query 1: อุณหภูมิเฉลี่ยรายวัน (เทียบกับ Postgres: ใช้ DATE() เหมือนกัน
-- แต่ต้องอ้าง table แบบเต็ม project.dataset.table)
-- ============================================================
SELECT
    location_name,
    DATE(reading_time) AS date,
    AVG(temperature_c) AS avg_temp
FROM `your-project-id.weather_pipeline.weather_readings`
GROUP BY location_name, date
ORDER BY date DESC;


-- ============================================================
-- Query 2: ใช้ DATE_TRUNC เฉพาะของ BigQuery (Postgres ก็มีแต่ syntax ต่างกัน)
-- หาค่าเฉลี่ยรายสัปดาห์แทนรายวัน
-- ============================================================
SELECT
    location_name,
    DATE_TRUNC(DATE(reading_time), WEEK) AS week_start,
    AVG(temperature_c) AS avg_temp,
    AVG(humidity_percent) AS avg_humidity
FROM `your-project-id.weather_pipeline.weather_readings`
GROUP BY location_name, week_start
ORDER BY week_start DESC;


-- ============================================================
-- Query 3: ใช้ APPROX_QUANTILES (ฟังก์ชันเฉพาะของ BigQuery สำหรับ
-- วิเคราะห์ข้อมูลขนาดใหญ่แบบประหยัด resource - หาค่า median โดยประมาณ
-- แทนการคำนวณแบบละเอียดที่กิน compute เยอะกว่า)
-- ============================================================
SELECT
    location_name,
    APPROX_QUANTILES(temperature_c, 2)[OFFSET(1)] AS median_temp
FROM `your-project-id.weather_pipeline.weather_readings`
GROUP BY location_name;


-- ============================================================
-- Query 4: เช็คค่าใช้จ่าย/ปริมาณข้อมูลที่ query นี้จะ scan ก่อนรันจริง
-- (BigQuery คิดเงินตามปริมาณข้อมูลที่ scan ควรเช็คก่อน query ตารางใหญ่)
-- รันผ่าน bq command-line tool: bq query --dry_run --use_legacy_sql=false
-- ============================================================
-- SELECT * FROM `your-project-id.weather_pipeline.weather_readings`;
