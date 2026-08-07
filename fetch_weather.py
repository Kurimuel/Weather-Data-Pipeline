"""
fetch_weather.py

ดึงข้อมูลสภาพอากาศปัจจุบันจาก Open-Meteo API แล้วเก็บเข้า Supabase (PostgreSQL)

วิธีรัน:
    python3 fetch_weather.py
"""

import os
import requests
import psycopg
from datetime import datetime, timezone
from dotenv import load_dotenv
from validate_data import validate_weather_record

load_dotenv()  # โหลดค่าจากไฟล์ .env

# --------------------------------------------------------------------------
# ตั้งค่าเมือง/พิกัดที่จะดึงข้อมูล
# เก็บเป็น list ตั้งแต่ต้น เผื่ออนาคตอยากเพิ่มเมืองอื่น ไม่ต้องแก้โครงสร้างโค้ด
# --------------------------------------------------------------------------
LOCATIONS = [
    {"name": "Bangkok", "lat": 13.7563, "lon": 100.5018},
    # เพิ่มเมืองอื่นได้ที่นี่ เช่น:
    # {"name": "Chiang Mai", "lat": 18.7883, "lon": 98.9853},
]

API_BASE_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather_for_location(location: dict) -> dict | None:
    """
    ดึงข้อมูลสภาพอากาศปัจจุบันของ 1 พิกัด จาก Open-Meteo API

    คืนค่า: dict ของข้อมูลอากาศ ถ้าสำเร็จ, None ถ้าล้มเหลว
    """
    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "UTC",
    }

    try:
        response = requests.get(API_BASE_URL, params=params, timeout=10)
        response.raise_for_status()  # จะ raise error ถ้า status code ไม่ใช่ 200
        data = response.json()
        return data

    except requests.exceptions.Timeout:
        print(f"[ERROR] Timeout ตอนดึงข้อมูลของ {location['name']} (รอเกิน 10 วินาที)")
        return None
    except requests.exceptions.ConnectionError:
        print(f"[ERROR] เชื่อมต่อ internet ไม่ได้ ตอนดึงข้อมูลของ {location['name']}")
        return None
    except requests.exceptions.HTTPError as e:
        print(f"[ERROR] API ตอบกลับมาเป็น error สำหรับ {location['name']}: {e}")
        return None
    except ValueError:
        # กรณี response ไม่ใช่ JSON ที่ถูกต้อง (parse ไม่ได้)
        print(f"[ERROR] แปลง response เป็น JSON ไม่ได้ สำหรับ {location['name']}")
        return None


def parse_weather_data(raw_data: dict, location_name: str) -> dict | None:
    """
    แปลงข้อมูลดิบจาก API ให้อยู่ในรูปแบบที่พร้อมเก็บลง database

    แยกฟังก์ชันนี้ออกจาก fetch_weather_for_location() ตั้งใจ เพราะถ้าวันหนึ่ง
    เปลี่ยนไปใช้ API เจ้าอื่น (field ชื่อไม่เหมือนกัน) จะแก้แค่ฟังก์ชันนี้
    ฟังก์ชัน fetch และฟังก์ชันที่ insert เข้า database ไม่ต้องแก้เลย
    """
    try:
        current = raw_data["current"]

        reading_time_naive = datetime.fromisoformat(current["time"])
        reading_time_utc = reading_time_naive.replace(tzinfo=timezone.utc)
        
        return {
            "location_name": location_name,
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


def save_to_database(weather_record: dict, db_url: str) -> bool:
    """
    เก็บข้อมูล 1 record เข้า Supabase

    ใช้ ON CONFLICT DO NOTHING เพื่อป้องกันข้อมูลซ้ำ ถ้ารัน pipeline ซ้ำ
    ในช่วงเวลาเดียวกัน (ตรงกับ UNIQUE constraint ที่ตั้งไว้ใน schema.sql)
    """
    insert_query = """
        INSERT INTO weather_readings
            (location_name, latitude, longitude, reading_time,
             temperature_c, humidity_percent, wind_speed_kmh)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (location_name, reading_time) DO NOTHING;
    """

    conn = None
    try:
        conn = psycopg.connect(db_url)
        cursor = conn.cursor()
        cursor.execute(insert_query, (
            weather_record["location_name"],
            weather_record["latitude"],
            weather_record["longitude"],
            weather_record["reading_time"],
            weather_record["temperature_c"],
            weather_record["humidity_percent"],
            weather_record["wind_speed_kmh"],
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

        weather_record = parse_weather_data(raw_data, location["name"])
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

    print(f"\n=== เสร็จสิ้น ({datetime.now().isoformat()}) ===")


if __name__ == "__main__":
    main()
