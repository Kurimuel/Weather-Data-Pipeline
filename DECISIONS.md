# Decision Log

บันทึกเหตุผลของการตัดสินใจสำคัญระหว่างพัฒนาโปรเจกต์นี้
เก็บไว้เพื่อใช้ตอบคำถามสัมภาษณ์ และเพื่อให้ตัวเองย้อนกลับมาดูได้ว่าทำไมถึงเลือกทางนี้

---

## 1. เลือก Open-Meteo แทน Air4Thai

**ตัดสินใจ:** ใช้ Open-Meteo API แทน Air4Thai (คุณภาพอากาศ)

**เหตุผล:**
- ทดสอบ fetch endpoint ของ Air4Thai (`getNewAQI_JSON.php`) ตรงๆ แล้วพบว่าถูก
  robots.txt ของเว็บ block ไม่ให้เข้าถึงแบบอัตโนมัติ
- Air4Thai มีช่องทางที่ถูกต้องผ่าน data.go.th แต่ต้องลงทะเบียนผูกกับ
  หน่วยงาน/บริษัท ซึ่งไม่เหมาะกับช่วงที่ยังหางานอยู่ (อาจเปลี่ยนแปลงยากทีหลัง)
- Open-Meteo ไม่ต้องลงทะเบียนเลย เอกสารทางการระบุชัดว่าเรียก endpoint ตรงได้
  โดยไม่ต้องมี authentication สำหรับการใช้งานแบบ non-commercial

**Trade-off ที่ยอมรับ:** เปลี่ยน domain จาก "คุณภาพอากาศ" เป็น "สภาพอากาศทั่วไป"
ซึ่งความซับซ้อนของข้อมูลน้อยกว่าเล็กน้อย แต่ปลอดภัยกว่าในแง่กฎหมาย/สิทธิ์การเข้าถึง

---

## 2. เลือก PostgreSQL (Supabase) แทน MongoDB

**ตัดสินใจ:** ใช้ PostgreSQL ผ่าน Supabase

**เหตุผล:**
- ข้อมูลสภาพอากาศเป็น structured data ที่มี schema ชัดเจนตายตัว
  (temperature, humidity, wind_speed ไม่เปลี่ยนโครงสร้างบ่อย)
- ไม่มีความจำเป็นต้องใช้ flexible schema แบบที่ NoSQL ตอบโจทย์
- PostgreSQL รองรับ constraint แบบ UNIQUE ได้ตรงไปตรงมา ช่วยกันข้อมูลซ้ำ

---

## 3. แยก reading_time กับ fetched_at เป็นคนละ column

**ตัดสินใจ:** เก็บ 2 timestamp แยกกันในตาราง แทนที่จะเก็บแค่อันเดียว

**เหตุผล:**
- `reading_time` = เวลาที่ "ข้อมูล" นี้เป็นตัวแทนของช่วงเวลานั้น (มาจาก API)
- `fetched_at` = เวลาที่ pipeline ของเราไปดึงมาจริง (default เป็นเวลาปัจจุบันตอน insert)
- ถ้า pipeline ล่มแล้วมารันย้อนหลัง สองค่านี้จะไม่ตรงกัน การแยกเก็บช่วยให้
  debug ได้ว่า "ข้อมูลนี้มาช้าหรือเปล่า" ซึ่งเป็นปัญหาที่พบบ่อยจริงในงาน
  data engineering (data latency)

---

## 4. ใช้ ON CONFLICT DO NOTHING แทนการเช็คซ้ำด้วย SELECT ก่อน

**ตัดสินใจ:** ใช้ PostgreSQL UPSERT-style query แทนการ SELECT เช็คก่อน INSERT

**เหตุผล:**
- ลดจำนวน round-trip ไปยัง database จาก 2 ครั้ง (SELECT แล้ว INSERT) เหลือ 1 ครั้ง
- ป้องกัน race condition ถ้าในอนาคตมีหลาย process รันพร้อมกัน

---

## 5. แยกฟังก์ชัน fetch กับ parse ออกจากกัน

**ตัดสินใจ:** แยก `fetch_weather_for_location()` กับ `parse_weather_data()` เป็นคนละฟังก์ชัน

**เหตุผล:**
- ถ้าวันหนึ่งเปลี่ยนไปใช้ API เจ้าอื่น (field ชื่อไม่เหมือนกัน ตามที่คุยกันเรื่อง
  schema mismatch) จะแก้แค่ฟังก์ชัน parse ฟังก์ชันอื่นไม่ต้องแตะ
- ทำให้ทดสอบแต่ละส่วนแยกกันได้ง่ายขึ้น (unit testing)

---

## 6. แก้ TIMESTAMP เป็น TIMESTAMPTZ (แก้ไขภายหลัง)

ตัดสินใจ: เปลี่ยน reading_time และ fetched_at จาก TIMESTAMP เป็น TIMESTAMPTZ

เหตุผล:

TIMESTAMP ธรรมดาไม่เก็บข้อมูล timezone มาด้วย ถ้าวันหนึ่งมีคนอื่นมาต่อโปรเจกต์ แล้วอยู่คนละ timezone จะสับสนว่าเวลาที่เก็บไว้คือเวลาไหนกันแน่
TIMESTAMPTZ เก็บ timezone ไปด้วยเสมอ ปลอดภัยกว่าเมื่อระบบขยายไปหลาย timezone ในอนาคต (เช่น เพิ่มเมืองในประเทศอื่น)

---

## 7. แยก analysis_queries.sql ออกจาก schema.sql

ตัดสินใจ: แยกไฟล์ query วิเคราะห์ (SELECT) ออกจากไฟล์สร้างตาราง (CREATE TABLE)

เหตุผล:

schema.sql รันแค่ครั้งเดียวตอนตั้งค่า database ครั้งแรก
analysis_queries.sql รันบ่อยๆ ตามต้องการเวลาอยากดูข้อมูล
แยกไฟล์ทำให้จุดประสงค์ของแต่ละไฟล์ชัดเจน ไม่ต้อง scroll หา query ที่ต้องการ ท่ามกลาง DDL statement (CREATE TABLE, INDEX)

---

- requirements.txt — เปลี่ยนจาก psycopg2-binary==2.9.9 เป็น psycopg[binary] (ไม่ pin เวอร์ชัน เพราะอยากได้ตัวล่าสุดที่รองรับ Python 3.13)
- fetch_weather.py — เปลี่ยน import psycopg2 → import psycopg และเปลี่ยนทุกจุดที่เรียก psycopg2.connect(), psycopg2.OperationalError, psycopg2.Error เป็น psycopg.* แทน (ฟังก์ชันการทำงานเหมือนเดิมทุกอย่าง แค่เปลี่ยนชื่อ library)

---
<!--
เพิ่ม entry ใหม่ที่นี่ทุกครั้งที่ตัดสินใจอะไรสำคัญ เช่น:
- ทำไมเลือก error handling แบบนี้
- ทำไมเลือก scheduling ทุก X นาที
- เจอ edge case อะไรตอนทดสอบ "ทำให้พัง" แล้วแก้ยังไง
-->
