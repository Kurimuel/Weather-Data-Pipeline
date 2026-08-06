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

## 8. แก้บั๊ก: reading_time ถูกเก็บผิด timezone (เจอตอนทดสอบจริง)

ปัญหาที่เจอ: หลังรัน pipeline จริงแล้วเช็คข้อมูลใน database พบว่า fetched_at กับ reading_time ที่ควรจะใกล้เคียงกัน (เพราะดึงข้อมูล "ปัจจุบัน") กลับต่างกัน ราว 7 ชั่วโมง ทั้งที่ทั้งคู่ label เป็น UTC (+00) เหมือนกัน

สาเหตุ: ตอนเรียก Open-Meteo API ใช้ timezone=Asia/Bangkok ทำให้ API ส่ง เวลากลับมาเป็น "เวลาไทยแบบไม่มี timezone กำกับ" (เช่น "2026-08-04T14:15") เมื่อค่านี้ถูก insert ลง column TIMESTAMPTZ โดยไม่แปลงก่อน PostgreSQL จะ ตีความ string ที่ไม่มี timezone กำกับว่าเป็น UTC โดย default — ทำให้เวลาไทย 14:15 ถูกเก็บเป็น "14:15 UTC" (ซึ่งจริงๆ ควรจะเป็น "07:15 UTC" ถ้าแปลงถูกต้อง)

วิธีแก้: เปลี่ยนไปขอข้อมูลจาก API เป็น timezone=UTC แทน เพื่อให้ค่าที่ได้ มาเป็น UTC จริงตั้งแต่ต้น ไม่ต้องแปลงอะไรเพิ่ม แล้วเก็บลง TIMESTAMPTZ ตรงๆ ได้อย่างถูกต้อง

หลักการที่ยึดถือ (เก็บไว้ใช้ในโปรเจกต์อื่นด้วย): เก็บเวลาเป็น UTC เสมอใน database ไม่ว่า business logic จะต้องการแสดงผลเป็น timezone ไหนก็ตาม แล้วค่อย แปลงเป็นเวลาท้องถิ่นเฉพาะตอนแสดงผล (เช่น ใน dashboard) เท่านั้น วิธีนี้กันปัญหา ความสับสนเรื่อง timezone ได้ตั้งแต่ต้น และเป็นมาตรฐานที่ระบบ production ส่วนใหญ่ใช้

บทเรียน: นี่คือตัวอย่างจริงของเหตุผลที่ควรทดสอบ "ทำให้พัง" หรือตรวจสอบข้อมูล จริงหลัง insert เสมอ (ตามที่วางแผนไว้ใน Phase 4) เพราะบั๊กแบบนี้ไม่ทำให้โปรแกรม crash หรือ error เลย แต่ข้อมูลที่ได้กลับผิดเงียบๆ (silent data corruption) ซึ่ง อันตรายกว่า error ที่เห็นชัดเจนเสียอีก

---

## 9. ปรับปรุงเพิ่มเติม: ส่ง timezone-aware datetime object แทน string เปล่า

ปัญหาที่ยังเหลืออยู่หลังแก้ข้อ 8: แม้เปลี่ยนไปขอ timezone=UTC จาก API แล้ว แต่โค้ดยังคงส่ง string เปล่าๆ (ไม่มี timezone กำกับ) เข้า database อยู่ดี เป็นการแก้ที่ปลายเหตุ ไม่ใช่ต้นเหตุ — ถ้าวันหนึ่งลืมแล้วเปลี่ยน timezone parameter กลับไปโดยไม่รู้ตัว จะกลับไปเจอบั๊กเดิมอีก

วิธีแก้ที่ต้นเหตุจริงๆ: แปลง string ที่ได้จาก API ให้เป็น Python datetime object ที่มี tzinfo=timezone.utc กำกับชัดเจน ก่อน ส่งเข้า database ด้วย datetime.fromisoformat() + .replace(tzinfo=timezone.utc) เมื่อส่ง object ที่มี timezone กำกับแล้ว psycopg จะจัดการแปลงให้ database เก็บค่าถูกต้องเสมอ ไม่ต้องพึ่งการเดาของ Postgres อีกต่อไป

ปรับเพิ่ม: เปลี่ยน fetched_at จากที่เคยพึ่ง DEFAULT NOW() ของ database ให้ Python เป็นคนกำหนดเองด้วย datetime.now(timezone.utc) แทน เพื่อความ สอดคล้องกันทั้งสอง column (ทั้งคู่เป็น timezone-aware object ที่มาจาก Python ทั้งหมด ไม่ผสมกันระหว่างค่าที่ database สร้างเองกับค่าที่ Python ส่งมา)

หลักการทั่วไปที่ได้เรียนรู้: เวลาทำงานกับเวลา (datetime) ข้าม system (API → Python → Database) ควรทำให้ค่าเป็น "timezone-aware" ตั้งแต่จุดแรกที่รับ เข้ามาให้เร็วที่สุด ไม่ปล่อยให้เป็น "naive" (ไม่มี timezone กำกับ) ผ่านหลาย ขั้นตอน เพราะยิ่งปล่อยนานยิ่งเสี่ยงมีจุดใดจุดหนึ่ง "เดาผิด"

---

## 10. เลือก GitHub Actions แทน Cloud Composer/Airflow สำหรับ automation

ตัดสินใจ: ใช้ GitHub Actions scheduled workflow (cron) สำหรับรัน pipeline อัตโนมัติ แทนเครื่องมือ orchestration แบบเต็มรูปแบบ เช่น Airflow/Cloud Composer

เหตุผล:

Scope โปรเจกต์เล็ก มี pipeline เดียว ไม่มี dependency ระหว่างหลาย task ที่ซับซ้อนพอจะต้องใช้ DAG-based orchestration
GitHub Actions ฟรีสำหรับ public repo และตั้งค่าเร็วกว่ามาก (ไม่ต้องมี server แยกไว้รัน scheduler เอง)
อยู่ใน repo เดียวกับโค้ด ทำให้ทุกอย่างอยู่ที่เดียว ง่ายต่อการดูแล

Trade-off ที่ยอมรับ: GitHub Actions ไม่มี UI สำหรับดู dependency graph ระหว่าง task แบบ Airflow ถ้าโปรเจกต์ขยายใหญ่ขึ้นมาก (หลาย pipeline ที่ต้อง รันต่อกันเป็นลำดับ) ควรพิจารณาย้ายไป Airflow/Cloud Composer จริงจัง

ตั้งค่า schedule: เลือกรันทุก 1 ชั่วโมง (cron: "0 * * * *") เพราะข้อมูล อากาศจาก Open-Meteo อัปเดตทุก 15 นาทีอยู่แล้ว รันถี่กว่า 1 ชั่วโมงจะได้ข้อมูล ซ้ำซ้อนไม่คุ้มกับ resource ที่ใช้ (GitHub Actions มี quota จำกัดต่อเดือน)

ความปลอดภัย: เก็บ SUPABASE_DB_URL เป็น GitHub Secret ไม่ hardcode ในไฟล์ workflow เพื่อไม่ให้ password หลุดออกไปกับโค้ดที่เป็น public repo

ข้อจำกัดที่พบตอนใช้งานจริง (สำคัญ): GitHub Actions scheduled workflow ไม่รันตรงเวลาเป๊ะ เป็นพฤติกรรมที่ GitHub เอกสารไว้เองว่า schedule event อาจถูกดีเลย์ในช่วงที่มี load สูง โดยเฉพาะนาทีที่ 0 และ 30 ของทุกชั่วโมง (จุดที่คนตั้ง cron กันเยอะที่สุด) ในกรณีที่ load สูงมาก บาง run อาจถูกดร็อป ไปเลยไม่รันเลยด้วยซ้ำ ไม่มีทางแก้ให้ตรงเป๊ะ 100% แม้แต่ด้วย self-hosted runner เพราะ logic การจัดคิวอยู่ฝั่ง GitHub เอง

วิธีรับมือ: (1) เปลี่ยน cron จาก 0 * * * * เป็น 23 * * * * (นาทีสุ่ม ที่ไม่ใช่ 0/30) เพื่อลดโอกาสเจอ peak load (2) ยอมรับว่าข้อมูลจะมาไม่ตรงเวลา เป๊ะเสมอ แล้วออกแบบให้ระบบรองรับความจริงนี้แทนที่จะพยายามแก้ให้สมบูรณ์แบบ เช่น ใช้ query "เช็คจำนวน reading ต่อวัน" ใน analysis_queries.sql (Query 3) เพื่อ monitor ว่าจริงๆ แล้ว pipeline รันขาดหายไปกี่ครั้ง แทนที่จะสมมติว่ามัน รันครบเป๊ะทุกชั่วโมงเสมอ

ข้อควรระวังเพิ่มเติม: ถ้า repo ไม่มี activity (commit/PR/issue) ติดต่อกัน 60 วัน GitHub จะปิด scheduled workflow อัตโนมัติแบบเงียบๆ ต้องมี activity สม่ำเสมอถ้าอยากให้ automation ทำงานต่อเนื่องระยะยาว

---

11. Data Validation: แยกไฟล์ + คืนค่าเป็น list สะสมทุก error

ตัดสินใจ: สร้าง validate_data.py แยกจาก fetch_weather.py และออกแบบให้ validate_weather_record() คืนค่าเป็น list ของ error ทั้งหมดที่เจอ แทนที่จะ return False ทันทีที่เจอปัญหาแรก

เหตุผลที่แยกไฟล์:

ทดสอบ validation logic ได้อิสระ ไม่ต้องยิง API หรือต่อ database จริงทุกครั้ง (ดูส่วน if __name__ == "__main__" ที่มี test case จำลองในตัว)
ถ้าวันหนึ่งกฎการตรวจสอบเปลี่ยน (เช่น เพิ่มเงื่อนไขใหม่) แก้แค่ไฟล์นี้ไฟล์เดียว

เหตุผลที่คืนค่าเป็น list สะสมทุก error (ไม่ return False ทันที):

เวลา debug จริง อยากเห็นปัญหาทั้งหมดในครั้งเดียว ไม่ใช่ต้องแก้ทีละจุดแล้ว รันใหม่หลายรอบกว่าจะเจอปัญหาที่ 2, 3 ตามลำดับ
ตัวอย่างจริง: ถ้า record หนึ่งมีทั้งอุณหภูมิผิดปกติและความชื้นผิดปกติพร้อมกัน อยากรู้ทั้งคู่ทันที ไม่ใช่รู้แค่ปัญหาแรก

ตำแหน่งที่เชื่อมเข้า pipeline หลัก: validate หลังจาก parse แต่ก่อน save_to_database — ถ้าไม่ผ่าน validation จะไม่ถูกเก็บลง database เลย และ log รายละเอียดของทุกปัญหาที่เจอไว้ (ผ่าน print ตอนนี้ ในอนาคตอาจเปลี่ยนเป็น proper logging library ถ้าต้องการ persist log ไว้นานกว่านี้)

Trade-off ที่ยอมรับ: ถ้าข้อมูลไม่ผ่าน validation จะถูก "ทิ้ง" ไปเลย ไม่ได้ เก็บไว้ที่ไหนสำหรับตรวจสอบย้อนหลัง ในระบบ production จริงอาจพิจารณาเก็บลง "quarantine table" แยกต่างหากแทนการทิ้งไปเฉยๆ แต่สำหรับ scope โปรเจกต์นี้ มองว่าไม่จำเป็น เพราะข้อมูลอากาศที่ผิดปกติมักหมายถึง API มีปัญหาชั่วคราว มากกว่าจะเป็นข้อมูลที่มีค่าควรเก็บไว้วิเคราะห์

---

12. Dashboard: เลือก Streamlit + cache 5 นาที

ตัดสินใจ: ใช้ Streamlit ทำ dashboard แสดงผล พร้อม @st.cache_data(ttl=300)

เหตุผลที่เลือก Streamlit:

เขียนด้วย Python ล้วน ไม่ต้องแยกเรียนรู้ HTML/CSS/JS เพิ่ม เหมาะกับ scope โปรเจกต์ที่เน้นโชว์ทักษะ data engineering ไม่ใช่ frontend
Deploy ฟรีผ่าน Streamlit Community Cloud เชื่อม GitHub โดยตรง

เหตุผลที่ cache ไว้ 5 นาที (ไม่ query ทุกครั้งที่ผู้ใช้ interact):

ข้อมูลอัปเดตทุก 1 ชั่วโมง (ตาม automation) การ query ทุกครั้งที่มีคนเปิด dashboard จึงไม่จำเป็น เพิ่มภาระ database โดยไม่ได้ข้อมูลใหม่กว่าเดิม
ป้องกันปัญหาถ้ามีคนเข้าดู dashboard พร้อมกันหลายคน ไม่ต้องยิง query ซ้ำๆ ทุกคน

สิ่งที่ต้องทำเพิ่มตอน deploy จริง (ยังไม่ได้ทำ): Streamlit Community Cloud ไม่อ่านไฟล์ .env เหมือนตอนรันบนเครื่อง ต้องตั้งค่า SUPABASE_DB_URL ผ่าน Streamlit Secrets Manager แทน (เมนู Settings ของ app หลัง deploy)

---
<!--
เพิ่ม entry ใหม่ที่นี่ทุกครั้งที่ตัดสินใจอะไรสำคัญ เช่น:
- ทำไมเลือก error handling แบบนี้
- ทำไมเลือก scheduling ทุก X นาที
- เจอ edge case อะไรตอนทดสอบ "ทำให้พัง" แล้วแก้ยังไง
-->
