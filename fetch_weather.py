"""
fetch_weather.py

ดึงข้อมูลสภาพอากาศปัจจุบันจาก Open-Meteo API แล้วเก็บเข้า Supabase (PostgreSQL)

วิธีรัน:
    python3 fetch_weather.py
"""

import os
import time
import requests
import psycopg
from datetime import datetime, timezone
from dotenv import load_dotenv
from validate_data import validate_weather_record

load_dotenv()  # โหลดค่าจากไฟล์ .env

# --------------------------------------------------------------------------
# ตั้งค่าเมือง/พิกัดที่จะดึงข้อมูล
# ผสมทั้งเมืองในไทยและทั่วโลก เพื่อทดสอบว่า pipeline รองรับ scale ได้จริง
# --------------------------------------------------------------------------
LOCATIONS = [
    # ประเทศไทย
    {"name": "Bangkok", "country": "Thailand", "lat": 13.7563, "lon": 100.5018},
    {"name": "Chiang Mai", "country": "Thailand", "lat": 18.7883, "lon": 98.9853},
    {"name": "Phuket", "country": "Thailand", "lat": 7.8804, "lon": 98.3923},
    {"name": "Khon Kaen", "country": "Thailand", "lat": 16.4419, "lon": 102.8360},
    # เอเชีย
    {"name": "Tokyo", "country": "Japan", "lat": 35.6762, "lon": 139.6503},
    {"name": "Singapore", "country": "Singapore", "lat": 1.3521, "lon": 103.8198},
    {"name": "Seoul", "country": "South Korea", "lat": 37.5665, "lon": 126.9780},
    # ยุโรป
    {"name": "London", "country": "United Kingdom", "lat": 51.5074, "lon": -0.1278},
    {"name": "Paris", "country": "France", "lat": 48.8566, "lon": 2.3522},
    # อเมริกา
    {"name": "New York", "country": "United States", "lat": 40.7128, "lon": -74.0060},
    {"name": "Sao Paulo", "country": "Brazil", "lat": -23.5505, "lon": -46.6333},
    # โอเชียเนีย
    {"name": "Sydney", "country": "Australia", "lat": -33.8688, "lon": 151.2093},
]

API_BASE_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather_for_location(location: dict, max_retries: int = 1) -> dict | None:
    """
    ดึงข้อมูลสภาพอากาศปัจจุบันของ 1 พิกัด จาก Open-Meteo API

    คืนค่า: dict ของข้อมูลอากาศ ถ้าสำเร็จ, None ถ้าล้มเหลว

    มี retry เมื่อเจอ timeout เพราะจากการใช้งานจริงพบว่า timeout ส่วนใหญ่
    เป็นปัญหาเครือข่ายแบบสุ่ม (transient) ไม่ใช่ปัญหาถาวรของ API หรือ
    พิกัดที่ขอ (สังเกตจาก log จริง: เมืองที่ timeout เปลี่ยนไปเรื่อยๆ ทุก
    รอบ ไม่ใช่เมืองเดิมซ้ำ - ดู DECISIONS.md ข้อ 17) ลอง retry อีกครั้ง
    ก่อนจะยอมแพ้ มีโอกาสสูงที่จะสำเร็จในรอบที่ 2
    """
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "UTC",
    }

    for attempt in range(1, max_retries + 2):  # +2 = พยายามครั้งแรก + retry
        try:
            response = requests.get(API_BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            if attempt <= max_retries:
                print(f"[WARNING] Timeout ตอนดึงข้อมูลของ {location['name']} "
                      f"(ครั้งที่ {attempt}) กำลังลองใหม่...")
                time.sleep(2)
                continue
            print(f"[ERROR] Timeout ตอนดึงข้อมูลของ {location['name']} "
                  f"(รอเกิน 15 วินาที หลังลอง {attempt} ครั้ง)")
            return None
        except requests.exceptions.ConnectionError:
            print(f"[ERROR] เชื่อมต่อ internet ไม่ได้ ตอนดึงข้อมูลของ {location['name']}")
            return None
        except requests.exceptions.HTTPError as e:
            print(f"[ERROR] API ตอบกลับมาเป็น error สำหรับ {location['name']}: {e}")
            return None
        except ValueError:
            print(f"[ERROR] แปลง response เป็น JSON ไม่ได้ สำหรับ {location['name']}")
            return None

    return None


def parse_weather_data(raw_data: dict, location: dict) -> dict | None:
    """
    แปลงข้อมูลดิบจาก API ให้อยู่ในรูปแบบที่พร้อมเก็บลง database

    แยกฟังก์ชันนี้ออกจาก fetch_weather_for_location() ตั้งใจ เพราะถ้าวันหนึ่ง
    เปลี่ยนไปใช้ API เจ้าอื่น (field ชื่อไม่เหมือนกัน) จะแก้แค่ฟังก์ชันนี้
    ฟังก์ชัน fetch และฟังก์ชันที่ insert เข้า database ไม่ต้องแก้เลย

    รับ location เป็น dict เต็ม (ไม่ใช่แค่ location_name) เพราะหลัง
    normalize schema (ดู DECISIONS.md ข้อ 15) ต้องใช้ country ด้วยตอน
    get-or-create record ในตาราง locations
    """
    try:
        current = raw_data["current"]

        # API ขอเป็น UTC แล้ว (ดู params ด้านบน) แต่ค่าที่ได้มายังเป็น
        # string เปล่าๆ ไม่มี timezone กำกับ เช่น "2026-08-04T14:15"
        # ต้องแปลงเป็น datetime object ที่มี tzinfo=UTC ชัดเจนก่อน
        # ไม่งั้น Postgres จะเดา timezone เอง (เคยเกิดบั๊กจากจุดนี้มาแล้ว
        # ดู DECISIONS.md ข้อ 8)
        reading_time_naive = datetime.fromisoformat(current["time"])
        reading_time_utc = reading_time_naive.replace(tzinfo=timezone.utc)

        return {
            "location_name": location["name"],
            "country": location.get("country"),
            "latitude": raw_data["latitude"],
            "longitude": raw_data["longitude"],
            "reading_time": reading_time_utc,
            "temperature_c": current["temperature_2m"],
            "humidity_percent": current["relative_humidity_2m"],
            "wind_speed_kmh": current["wind_speed_10m"],
        }
    except KeyError as e:
        # ถ้า API เปลี่ยนโครงสร้าง (เช่น เปลี่ยนชื่อ field) จะเจอ error ตรงนี้
        # แทนที่โปรแกรมจะพังแบบไม่รู้สาเหตุ เราจะเห็น field ไหนหายไปชัดเจน
        print(f"[ERROR] ข้อมูลที่ได้มาขาด field ที่คาดไว้: {e}")
        return None


def get_or_create_location_id(cursor, weather_record: dict) -> int:
    """
    หา id ของเมืองนี้ในตาราง locations ถ้ายังไม่มีให้สร้างใหม่

    เช็คด้วย SELECT ก่อนเสมอ แทนที่จะพยายาม INSERT ทุกครั้งแล้วพึ่ง
    ON CONFLICT เพราะ PostgreSQL จะ "เผา" เลข SERIAL ทิ้งทุกครั้งที่มีการ
    พยายาม INSERT แม้จะไปชน ON CONFLICT แล้วเปลี่ยนเป็น UPDATE ก็ตาม
    (ดู DECISIONS.md ข้อ 17) ถ้าเรียกฟังก์ชันนี้ทุกชั่วโมงสำหรับเมืองที่
    มีอยู่แล้ว จะทำให้เลข id กระโดดขึ้นเรื่อยๆ อย่างไม่จำเป็น การ SELECT
    ก่อนช่วยให้ INSERT เกิดขึ้นเฉพาะตอนที่เป็นเมืองใหม่จริงๆ เท่านั้น
    """
    select_query = "SELECT id FROM locations WHERE name = %s;"
    cursor.execute(select_query, (weather_record["location_name"],))
    row = cursor.fetchone()
    if row is not None:
        return row[0]

    insert_query = """
        INSERT INTO locations (name, country, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id;
    """
    cursor.execute(insert_query, (
        weather_record["location_name"],
        weather_record["country"],
        weather_record["latitude"],
        weather_record["longitude"],
    ))
    return cursor.fetchone()[0]


def save_to_database(weather_record: dict, db_url: str) -> bool:
    """
    เก็บข้อมูล 1 record เข้า Supabase

    ทำ 2 ขั้นตอนในการเชื่อมต่อเดียวกัน (transaction เดียวกัน):
    1. get-or-create location_id จากตาราง locations
    2. insert ข้อมูลอากาศเข้า weather_readings โดยอ้างอิง location_id

    ใช้ ON CONFLICT DO NOTHING ตอน insert weather_readings เพื่อป้องกัน
    ข้อมูลซ้ำ ถ้ารัน pipeline ซ้ำในช่วงเวลาเดียวกัน (ตรงกับ UNIQUE
    constraint ที่ตั้งไว้ใน schema.sql)
    """
    insert_query = """
        INSERT INTO weather_readings
            (location_id, reading_time, temperature_c,
             humidity_percent, wind_speed_kmh, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (location_id, reading_time) DO NOTHING;
    """

    fetched_at_utc = datetime.now(timezone.utc)

    conn = None
    try:
        conn = psycopg.connect(db_url)
        cursor = conn.cursor()

        location_id = get_or_create_location_id(cursor, weather_record)

        cursor.execute(insert_query, (
            location_id,
            weather_record["reading_time"],
            weather_record["temperature_c"],
            weather_record["humidity_percent"],
            weather_record["wind_speed_kmh"],
            fetched_at_utc,
        ))
        conn.commit()
        cursor.close()
        return True

    except psycopg.OperationalError as e:
        print(f"[ERROR] เชื่อมต่อ database ไม่ได้: {e}")
        return False
    except psycopg.Error as e:
        print(f"[ERROR] เขียนข้อมูลลง database ไม่สำเร็จ: {e}")
        return False
    finally:
        if conn is not None:
            conn.close()


def main():
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        print("[ERROR] ไม่พบ SUPABASE_DB_URL ในไฟล์ .env กรุณาตั้งค่าก่อนรัน")
        return

    print(f"=== เริ่มดึงข้อมูล ({datetime.now().isoformat()}) ===")

    for location in LOCATIONS:
        print(f"\nกำลังดึงข้อมูลของ {location['name']}...")

        raw_data = fetch_weather_for_location(location)
        if raw_data is None:
            continue  # ข้ามไปเมืองถัดไป ไม่ทำให้ทั้ง pipeline ล้ม

        weather_record = parse_weather_data(raw_data, location)
        if weather_record is None:
            continue

        validation_errors = validate_weather_record(weather_record)
        if validation_errors:
            print(f"  [VALIDATION FAILED] ข้อมูลของ {location['name']} ไม่ผ่านการตรวจสอบ:")
            for err in validation_errors:
                print(f"    - {err}")
            continue  # ไม่เก็บข้อมูลที่ไม่ผ่าน validation ลง database

        success = save_to_database(weather_record, db_url)
        if success:
            print(f"  บันทึกสำเร็จ: {weather_record['temperature_c']}°C, "
                  f"ความชื้น {weather_record['humidity_percent']}%")
        else:
            print(f"  บันทึกไม่สำเร็จสำหรับ {location['name']}")

        time.sleep(1)  # หน่วงเวลาก่อนไปเมืองถัดไป ลดโอกาสโดน rate limit/timeout
        # จากการยิง request รัวติดกันเร็วเกินไป (ดู DECISIONS.md ข้อ 17)

    print(f"\n=== เสร็จสิ้น ({datetime.now().isoformat()}) ===")


if __name__ == "__main__":
    main()
