# Weather Data Pipeline

โปรเจกต์ฝึกทักษะ Data Engineering: ดึงข้อมูลสภาพอากาศจาก [Open-Meteo API](https://open-meteo.com)
แบบอัตโนมัติ แล้วเก็บลง PostgreSQL (Supabase) เพื่อสร้างชุดข้อมูล time-series
ที่นำไปวิเคราะห์หรือแสดงผลต่อได้

Live Dashboard: https://weather-data-pipeline-fb7py9fcfze9c8dpn7u64t.streamlit.app/

ดูเหตุผลของการตัดสินใจออกแบบแต่ละจุดได้ที่ [DECISIONS.md](./DECISIONS.md)

## สถานะปัจจุบัน

- [x] ทดสอบ API ใช้งานได้จริง
- [x] ออกแบบ database schema
- [x] เขียน script ดึงข้อมูล + เก็บลง database (manual run)
- [x] เพิ่ม data validation ก่อนเก็บลง database
- [x] เพิ่ม automation (รันเองตามตาราง)
- [x] SQL analytics queries
- [x] Deploy + ทำ dashboard แสดงผล
- [x] BigQuery sync (OLAP layer)

## วิธีติดตั้งและรัน

### 1. เตรียม environment

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. ตั้งค่า database

1. เปิด Supabase project ของคุณ ไปที่ **SQL Editor**
2. รันคำสั่งใน [`schema.sql`](./schema.sql) เพื่อสร้างตาราง

### 3. ตั้งค่า environment variables

```bash
cp .env.example .env
```

แล้วเปิดไฟล์ `.env` ใส่ค่า connection string จริงจาก
Supabase > Project Settings > Database > Connection string

**⚠️ อย่า commit ไฟล์ `.env` ขึ้น GitHub เด็ดขาด** (มี password อยู่ข้างใน)
ไฟล์นี้ควรอยู่ใน `.gitignore` อยู่แล้ว

### 4. รัน pipeline

```bash
python3 fetch_weather.py
```

ถ้าสำเร็จ จะเห็นข้อความแบบนี้:
```
=== เริ่มดึงข้อมูล (2026-07-31T10:00:00) ===

กำลังดึงข้อมูลของ Bangkok...
  บันทึกสำเร็จ: 31.0°C, ความชื้น 71%

=== เสร็จสิ้น (2026-07-31T10:00:05) ===
```

## โครงสร้างข้อมูล (Data Source)

ใช้ [Open-Meteo Forecast API](https://open-meteo.com/en/docs) ดึงเฉพาะข้อมูลปัจจุบัน
(`current`) ของ: อุณหภูมิ (°C), ความชื้นสัมพัทธ์ (%), ความเร็วลม (km/h)

ไม่ต้องใช้ API key สำหรับการใช้งานแบบ non-commercial

## Architecture

```
Open-Meteo API
      │ fetch ทุก 1 ชม. (GitHub Actions)
      ▼
validate_data.py
      ▼
Supabase (PostgreSQL) ── เก็บข้อมูลสด (OLTP)
      │ sync วันละครั้ง (GitHub Actions)
      ▼
BigQuery ── วิเคราะห์ข้อมูลก้อนใหญ่ (OLAP)
```

ดูเหตุผลของการแยก OLTP/OLAP ได้ที่ DECISIONS.md ข้อ 12-13

## การตั้งค่า Database

ถ้าเริ่มต้นใหม่ (ยังไม่มีข้อมูลเลย): รัน schema.sql ตรงๆ

ถ้ามีข้อมูลอยู่แล้วจากเวอร์ชันก่อน normalize: รัน migration_001_normalize_locations.sql แทน (มีคำแนะนำละเอียดในไฟล์ว่าควรเช็คผลลัพธ์ระหว่างทางตรงไหนบ้าง ก่อนจะลบ column เดิมทิ้งจริง)

## การตั้งค่า BigQuery
1. สร้าง Google Cloud Project + เปิดใช้ BigQuery API
2. สร้าง Service Account ให้สิทธิ์ BigQuery Data Editor
3. ดาวน์โหลด JSON key ของ Service Account (ห้าม commit ขึ้น GitHub)
4. สร้าง dataset ชื่อ weather_pipeline ใน BigQuery
5. ตั้งค่าใน .env: GOOGLE_APPLICATION_CREDENTIALS, BQ_PROJECT_ID
6. รัน python bigquery_sync.py เพื่อ sync ครั้งแรก

Query ตัวอย่างสำหรับ BigQuery dialect ดูได้ที่ bigquery_analysis_queries.sql

## Roadmap ถัดไป

1. เพิ่มเมืองอื่นนอกจากกรุงเทพฯ
2. ทดสอบ failure scenarios (ตัด internet, ใส่พิกัดผิด) แล้วบันทึกผลใน DECISIONS.md
3. Sync ข้อมูลไป BigQuery (OLAP layer) — ดูรายละเอียดใน DECISIONS.md
4. เพิ่ม pgvector similarity search demo
5. สร้าง View สำหรับแสดงเวลาไทย (Asia/Bangkok) ให้คนอื่นใช้ query ง่ายขึ้น โดยไม่กระทบข้อมูลจริงที่ยังเก็บเป็น UTC ตามมาตรฐาน
