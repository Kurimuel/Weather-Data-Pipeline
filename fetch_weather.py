"""
fetch_weather.py

ดึงข้อมูลสภาพอากาศปัจจุบันจาก Open-Meteo API แล้วเก็บเข้า Supabase (PostgreSQL)

วิธีรัน:
    python3 fetch_weather.py

โหมดดึงข้อมูล (ตั้งผ่าน environment variable FETCH_MODE):
    batch     (ค่าเริ่มต้น) ยิง API "ครั้งเดียว" ขอข้อมูลทุกเมืองพร้อมกัน แล้วจับคู่ผลกับเมืองตามลำดับ
              ตรวจพิกัดที่ API ตอบกลับทุกเมืองว่าตรงกับที่ขอ (กันลำดับสลับ) ถ้าล้มจะรอแล้วลองใหม่หลายครั้ง
              เหตุผลที่เปลี่ยน: บน GitHub runner คำขอเดี่ยว 12 ครั้งต่อรอบล้ม ~29% (ดู DECISIONS.md ข้อ 22)
    per_city  พฤติกรรมเดิม ยิงทีละเมือง (ใช้ย้อนกลับถ้าโหมดรวมมีปัญหา ไม่ต้องแก้โค้ด)

Exit code (สำคัญสำหรับ GitHub Actions เพราะใช้ตัดสินว่า job เป็นสีเขียวหรือแดง):
    0 = รอบนี้ใช้ได้ (บันทึกสำเร็จอย่างน้อย 1 เมือง และล้มเหลวไม่เกิน MAX_FAILURE_RATIO)
    1 = รอบนี้ล้มเหลว (ตั้งค่าไม่ครบ / ต่อ database ไม่ได้ / ไม่มีข้อมูลถูกบันทึกเลย /
        ล้มเหลวเกินสัดส่วนที่กำหนด)
"""

import os
import sys
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

# --------------------------------------------------------------------------
# ตั้งค่า retry และเกณฑ์ตัดสินว่ารอบนี้ "ล้มเหลว"
# --------------------------------------------------------------------------
MAX_RETRIES = 2                # ลองใหม่สูงสุดกี่ครั้ง (รวมครั้งแรกเป็น 3 ครั้ง)
BACKOFF_BASE_SECONDS = 2       # รอ 2 วินาที, 4 วินาที, ... (เพิ่มเป็น 2 เท่าทุกครั้งที่ลองใหม่)
MAX_RETRY_AFTER_SECONDS = 30   # เพดานเวลารอ เมื่อ API ส่ง header Retry-After มา
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}  # ปัญหาชั่วคราวฝั่ง API ที่ควรลองใหม่
MAX_FAILURE_RATIO = 0.5        # ล้มเหลวเกินสัดส่วนนี้ของจำนวนเมือง -> job ต้องขึ้นสีแดง

# --------------------------------------------------------------------------
# ตั้งค่าโหมดรวม (batch): ยิงครั้งเดียวสำหรับทุกเมือง
# ค่าตัวเลขมาจากการวัดบน GitHub runner (ดู DECISIONS.md ข้อ 22) ปรับได้ตรงนี้
# --------------------------------------------------------------------------
DEFAULT_FETCH_MODE = "batch"
FETCH_MODES = ("batch", "per_city")
BATCH_TIMEOUT = 8                     # วินาทีต่อครั้ง (คำขอที่สำเร็จใช้ไม่ถึง 1.3 วินาทีในทุกการวัด)
BATCH_RETRY_WAITS = (10, 30, 60)      # รอกี่วินาทีก่อนลองครั้งที่ 2, 3, 4 (รวมลองได้ 4 ครั้ง)
BATCH_MAX_RETRY_AFTER_SECONDS = 120   # เพดานเวลารอเมื่อ API ส่ง Retry-After มา
COORDINATE_TOLERANCE_DEGREES = 1.0    # พิกัดที่ API ตอบต้องห่างจากที่ขอไม่เกินนี้ (ดูเหตุผลที่ verify_batch_item)


def _backoff_seconds(attempt: int, response=None) -> float:
    """
    คำนวณเวลารอก่อนลองใหม่ (exponential backoff: 2, 4, 8, ... วินาที)

    ถ้า API ส่ง header Retry-After (เป็นตัวเลขวินาที) มาด้วย เช่นตอนโดน 429
    จะเชื่อค่านั้นก่อน แต่จำกัดเพดานไว้ที่ MAX_RETRY_AFTER_SECONDS ไม่ให้ job ค้างนาน
    หมายเหตุ: ต้องเช็ค `response is not None` ห้ามเช็ค `if response:` เพราะ
    requests.Response ที่เป็น error (4xx/5xx) จะมีค่าความจริงเป็น False
    """
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            return min(int(retry_after), MAX_RETRY_AFTER_SECONDS)
    return BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))


def fetch_weather_for_location(location: dict, max_retries: int = MAX_RETRIES) -> dict | None:
    """
    ดึงข้อมูลสภาพอากาศปัจจุบันของ 1 พิกัด จาก Open-Meteo API

    คืนค่า: dict ของข้อมูลอากาศ ถ้าสำเร็จ, None ถ้าล้มเหลว

    ลองใหม่ (retry) เฉพาะปัญหาที่น่าจะเป็นแค่ชั่วคราว:
      - Timeout / เชื่อมต่อไม่ได้ (ปัญหาเครือข่ายแบบสุ่ม ดู DECISIONS.md ข้อ 17)
      - HTTP 429 (โดนจำกัดอัตรา) และ 5xx (API ฝั่งเซิร์ฟเวอร์มีปัญหาชั่วคราว)
    ไม่ลองใหม่เมื่อเจอ 4xx อื่นๆ (เช่น 400 พิกัดผิด) เพราะลองกี่ครั้งผลก็เหมือนเดิม
    รอแบบ exponential backoff ระหว่างครั้ง (ดู _backoff_seconds)
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

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in RETRYABLE_STATUS_CODES and attempt <= max_retries:
                wait = _backoff_seconds(attempt, e.response)
                print(f"[WARNING] API ตอบ HTTP {status} สำหรับ {location['name']} "
                      f"(ครั้งที่ {attempt}) รอ {wait} วินาทีแล้วลองใหม่...")
                time.sleep(wait)
                continue
            print(f"[ERROR] API ตอบกลับมาเป็น error สำหรับ {location['name']}: {e}")
            return None

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if isinstance(e, requests.exceptions.Timeout):
                reason = "Timeout (รอเกิน 15 วินาที)"
            else:
                reason = "เชื่อมต่อ internet ไม่ได้"
            if attempt <= max_retries:
                wait = _backoff_seconds(attempt)
                print(f"[WARNING] {reason} ตอนดึงข้อมูลของ {location['name']} "
                      f"(ครั้งที่ {attempt}) รอ {wait} วินาทีแล้วลองใหม่...")
                time.sleep(wait)
                continue
            print(f"[ERROR] {reason} ตอนดึงข้อมูลของ {location['name']} "
                  f"(หลังลอง {attempt} ครั้ง)")
            return None

        except ValueError:
            print(f"[ERROR] แปลง response เป็น JSON ไม่ได้ สำหรับ {location['name']}")
            return None
        except requests.exceptions.RequestException as e:
            # ปัญหาอื่นของ requests ที่ไม่ได้ดักไว้ข้างบน (เช่น redirect วน)
            print(f"[ERROR] ดึงข้อมูลของ {location['name']} ไม่สำเร็จ: {e}")
            return None

    return None


def get_fetch_mode() -> str:
    """อ่านโหมดดึงข้อมูลจาก FETCH_MODE (ว่าง = batch)"""
    raw = os.getenv("FETCH_MODE", "").strip().lower()
    if not raw:
        return DEFAULT_FETCH_MODE
    if raw not in FETCH_MODES:
        raise ValueError(f"FETCH_MODE ต้องเป็นหนึ่งใน {', '.join(FETCH_MODES)} แต่ได้ {raw!r}")
    return raw


def _batch_params(locations: list) -> dict:
    """พารามิเตอร์ของคำขอรวม: latitude/longitude เป็นรายการคั่นด้วยจุลภาค เรียงตามลำดับของ locations"""
    return {
        "latitude": ",".join(str(loc["lat"]) for loc in locations),
        "longitude": ",".join(str(loc["lon"]) for loc in locations),
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "UTC",
    }


def _batch_wait(attempt: int, response, waits=BATCH_RETRY_WAITS) -> float:
    """เวลารอก่อนลองครั้งถัดไป ถ้า API ส่ง Retry-After (ตัวเลข) มาจะเชื่อค่านั้น แต่ไม่เกินเพดาน"""
    if response is not None:  # ห้ามเขียน `if response:` เพราะ Response ที่เป็น error มีค่าความจริงเป็น False
        retry_after = response.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            return min(int(retry_after), BATCH_MAX_RETRY_AFTER_SECONDS)
    return waits[attempt - 1]


def _normalize_batch(data, expected: int):
    """
    ตรวจรูปร่างของผลลัพธ์คำขอรวม คืน (รายการผลต่อเมือง, None) ถ้าถูกต้อง หรือ (None, ข้อความปัญหา)

    ผลลัพธ์ที่ถูกต้องคือรายการ (list) ที่มีจำนวนเท่ากับจำนวนเมืองที่ขอ เรียงตามลำดับที่ขอ
    (ถ้าขอเมืองเดียว API ตอบเป็น object ตัวเดียว ไม่ใช่ list จึงรองรับกรณีนั้นด้วย)
    """
    if isinstance(data, dict) and expected == 1:
        data = [data]
    if not isinstance(data, list):
        detail = ""
        if isinstance(data, dict) and data.get("reason"):
            detail = f": {str(data['reason'])[:150]}"
        return None, f"ผลลัพธ์ไม่ใช่รายการของเมืองต่างๆ (ได้ {type(data).__name__}{detail})"
    if len(data) != expected:
        return None, f"ได้ข้อมูล {len(data)} เมือง แต่ขอ {expected} เมือง"
    return data, None


def fetch_weather_batch(locations: list, timeout: float = BATCH_TIMEOUT, waits=BATCH_RETRY_WAITS) -> list | None:
    """
    ยิง Open-Meteo "ครั้งเดียว" ขอข้อมูลทุกเมืองพร้อมกัน คืนรายการผลต่อเมืองเรียงตามลำดับของ locations
    หรือ None ถ้าล้มเหลวครบทุกครั้งที่ลอง

    ลองใหม่ได้ len(waits)+1 ครั้ง โดยรอตาม waits (10, 30, 60 วินาที) ระหว่างครั้ง เพราะจากการวัดบน
    GitHub runner ความล้มเหลวเป็นช่วงๆ ที่หายเองใน 10 ถึง ~70 วินาที การลองซ้ำทันทีภายในไม่กี่วินาทีไม่ช่วย
    ลองใหม่เมื่อ: Timeout / เชื่อมต่อไม่ได้ / HTTP 429 และ 5xx / อ่าน JSON ไม่ได้ / ผลลัพธ์รูปร่างผิด
    ไม่ลองใหม่เมื่อ: HTTP 4xx อื่นๆ (ลองกี่ครั้งผลก็เหมือนเดิม)
    ทุกครั้งที่ล้มจะพิมพ์ชนิดของความผิดพลาด (เช่น ReadTimeout) และเวลาที่ใช้ เพื่อใช้วินิจฉัย
    """
    params = _batch_params(locations)
    expected = len(locations)
    max_attempts = len(waits) + 1

    for attempt in range(1, max_attempts + 1):
        start = time.monotonic()
        reason = None
        retry_response = None
        try:
            response = requests.get(API_BASE_URL, params=params, timeout=timeout)
            response.raise_for_status()
            items, problem = _normalize_batch(response.json(), expected)
            if problem is None:
                print(f"ได้ข้อมูลของ {expected} เมืองจากคำขอเดียวใน {time.monotonic() - start:.1f} วินาที "
                      f"(ครั้งที่ {attempt}/{max_attempts})")
                return items
            reason = problem

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status not in RETRYABLE_STATUS_CODES:
                print(f"[ERROR] API ตอบ HTTP {status} สำหรับคำขอรวม (ไม่ลองใหม่ เพราะไม่ใช่ปัญหาชั่วคราว): {e}")
                return None
            reason = f"HTTP {status}"
            retry_response = e.response

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            reason = type(e).__name__   # เช่น ReadTimeout, ConnectTimeout, ConnectionError

        except ValueError:
            reason = "แปลง response เป็น JSON ไม่ได้"

        except requests.exceptions.RequestException as e:
            print(f"[ERROR] คำขอรวมล้มเหลวโดยไม่ลองใหม่ ({type(e).__name__}): {e}")
            return None

        elapsed = time.monotonic() - start
        if attempt < max_attempts:
            wait = _batch_wait(attempt, retry_response, waits)
            print(f"[WARNING] คำขอรวม {expected} เมืองล้มเหลว (ครั้งที่ {attempt}/{max_attempts}): "
                  f"{reason} หลัง {elapsed:.1f} วินาที รอ {wait} วินาทีแล้วลองใหม่...")
            time.sleep(wait)
        else:
            print(f"[ERROR] คำขอรวม {expected} เมืองล้มเหลวครบ {max_attempts} ครั้ง "
                  f"(ครั้งสุดท้าย: {reason} หลัง {elapsed:.1f} วินาที)")
    return None


def _degrees_apart(a: float, b: float) -> float:
    """ระยะห่างของมุมสองค่าเป็นองศา โดยคิดการวนรอบที่ ±180 (ใช้กับ longitude)"""
    return abs((a - b + 180) % 360 - 180)


def verify_batch_item(location: dict, item) -> str | None:
    """
    ตรวจว่าผลของรายการนี้เป็นของเมืองที่เราคิดจริง คืนข้อความปัญหา หรือ None ถ้าถูกต้อง

    ทำไมต้องตรวจ: โหมดรวมจับคู่ผลกับเมืองด้วย "ลำดับในรายการ" เท่านั้น ถ้าลำดับสลับหรือเลื่อนไปหนึ่งตำแหน่ง
    ข้อมูลของเมืองหนึ่งจะถูกบันทึกเป็นของอีกเมืองหนึ่งโดยไม่มี error ใดๆ ซึ่งอันตรายที่สุดของโหมดนี้
    API ตอบพิกัดของจุดกริดที่ใกล้ที่สุด (ต่างจากที่ขอเล็กน้อย) จึงยอมให้คลาดเคลื่อนได้ตาม
    COORDINATE_TOLERANCE_DEGREES ซึ่งตั้ง 1 องศา: ใหญ่พอสำหรับการปัดจุดกริด แต่เล็กกว่าระยะห่างระหว่างเมืองใดๆ
    ในรายการ (คู่ที่ใกล้สุดคือ London กับ Paris ห่างกัน 2.65 องศาในแกนที่ห่างกว่า รองลงมาคือ Bangkok กับ Khon Kaen
    2.69 องศา เพราะโค้ดปฏิเสธเมื่อแกนใดแกนหนึ่งเกินเกณฑ์) เทสต์ยืนยันว่าทุกคู่แยกกันได้
    """
    if not isinstance(item, dict):
        return "ผลลัพธ์ของเมืองนี้ไม่ใช่ข้อมูลแบบ object"
    lat, lon = item.get("latitude"), item.get("longitude")
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return "ผลลัพธ์ไม่มี latitude/longitude จึงตรวจลำดับของเมืองไม่ได้"
    if (abs(lat - location["lat"]) > COORDINATE_TOLERANCE_DEGREES
            or _degrees_apart(lon, location["lon"]) > COORDINATE_TOLERANCE_DEGREES):
        return (f"พิกัดที่ API ตอบ ({lat}, {lon}) ห่างจากที่ขอ ({location['lat']}, {location['lon']}) "
                f"เกิน {COORDINATE_TOLERANCE_DEGREES} องศา อาจเป็นลำดับสลับ จึงไม่ใช้ข้อมูลนี้")
    return None


def _github_annotation(level: str, message: str) -> None:
    """
    พิมพ์คำสั่งของ GitHub Actions (::warning:: หรือ ::error::) เพื่อให้ขึ้นบนหน้าสรุปของ run
    ทำเฉพาะเมื่อรันบน GitHub (GITHUB_ACTIONS=true) นอกนั้นไม่พิมพ์เพื่อไม่ให้รก
    """
    if os.getenv("GITHUB_ACTIONS") != "true":
        return
    message = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::{level} title=Fetch Weather::{message}")


def _write_step_summary(lines: list) -> None:
    """เขียนสรุปลงหน้า Summary ของ run บน GitHub (ถ้ามี GITHUB_STEP_SUMMARY)"""
    path = os.getenv("GITHUB_STEP_SUMMARY")
    if not path:
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


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


def open_connection(db_url: str):
    """
    เปิด connection ไปยัง database คืนค่า connection ถ้าสำเร็จ, None ถ้าล้มเหลว

    เปิดครั้งเดียวต่อการรัน 1 รอบ แล้วใช้ซ้ำกับทุกเมือง (เดิมเปิดใหม่ทุกเมือง
    = 12 ครั้งต่อรอบ ซึ่งช้ากว่าและเปลืองจำนวน connection ของ Supabase)
    """
    try:
        # prepare_threshold=None = ปิดการ "prepare" คำสั่ง SQL อัตโนมัติของ psycopg 3
        # (ค่าเริ่มต้นจะ prepare เมื่อคำสั่งเดิมถูกรันซ้ำเกิน 5 ครั้งบน connection เดียวกัน
        # ซึ่งตอนนี้เกิดขึ้นแน่ เพราะ insert 12 เมืองบน connection เดียว) ถ้า SUPABASE_DB_URL
        # เป็น pooler แบบ transaction mode (พอร์ต 6543) prepared statement จะพังกับเมืองที่ 6 เป็นต้นไป
        return psycopg.connect(db_url, prepare_threshold=None)
    except psycopg.Error as e:
        print(f"[ERROR] เชื่อมต่อ database ไม่ได้: {e}")
        return None


def save_to_database(weather_record: dict, conn) -> bool:
    """
    เก็บข้อมูล 1 record เข้า Supabase โดยใช้ connection ที่เปิดไว้แล้ว

    ทำ 2 ขั้นตอนใน transaction เดียวกัน:
    1. get-or-create location_id จากตาราง locations
    2. insert ข้อมูลอากาศเข้า weather_readings โดยอ้างอิง location_id

    ใช้ `with conn.transaction()` ซึ่งจะ commit ให้เมื่อสำเร็จ และ rollback ให้อัตโนมัติ
    เมื่อเกิด error ทำให้เมืองที่ล้มเหลวไม่ทำให้ connection เสียสภาพ
    เมืองถัดไปใช้ connection เดิมต่อได้ (ยกเว้นกรณีสายหลุดจริง ซึ่ง main() จะเปิดใหม่ให้)

    ใช้ ON CONFLICT DO NOTHING ตอน insert weather_readings เพื่อป้องกัน
    ข้อมูลซ้ำ ถ้ารัน pipeline ซ้ำในช่วงเวลาเดียวกัน (ตรงกับ UNIQUE
    constraint ที่ตั้งไว้ใน schema.sql) กรณีข้อมูลซ้ำนับเป็น "สำเร็จ"
    เพราะข้อมูลนั้นมีอยู่ใน database แล้วอย่างถูกต้อง
    """
    insert_query = """
        INSERT INTO weather_readings
            (location_id, reading_time, temperature_c,
             humidity_percent, wind_speed_kmh, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (location_id, reading_time) DO NOTHING;
    """

    fetched_at_utc = datetime.now(timezone.utc)

    try:
        with conn.transaction():
            with conn.cursor() as cursor:
                location_id = get_or_create_location_id(cursor, weather_record)

                cursor.execute(insert_query, (
                    location_id,
                    weather_record["reading_time"],
                    weather_record["temperature_c"],
                    weather_record["humidity_percent"],
                    weather_record["wind_speed_kmh"],
                    fetched_at_utc,
                ))
        return True

    except psycopg.OperationalError as e:
        print(f"[ERROR] เชื่อมต่อ database ไม่ได้: {e}")
        return False
    except psycopg.Error as e:
        print(f"[ERROR] เขียนข้อมูลลง database ไม่สำเร็จ: {e}")
        return False


def main() -> int:
    """
    รัน pipeline 1 รอบ คืนค่า exit code (0 = ใช้ได้, 1 = ล้มเหลว)

    แยกผลของแต่ละเมืองเป็น 3 กลุ่ม:
      saved    = บันทึกสำเร็จ
      rejected = ข้อมูลไม่ผ่าน validation (ระบบทำงานถูกต้อง แค่ไม่ยอมเก็บข้อมูลแปลกๆ
                 จึงไม่นับเป็นความล้มเหลวของ pipeline)
      failed   = ดึงไม่ได้ / พิกัดไม่ตรง / แปลงข้อมูลไม่ได้ / บันทึกลง database ไม่ได้

    ตัดสินว่ารอบนี้ล้มเหลว (exit 1) เมื่อ:
      - ไม่มีข้อมูลถูกบันทึกเลย (saved == 0) หรือ
      - failed เกิน MAX_FAILURE_RATIO ของจำนวนเมืองทั้งหมด
    ในโหมด batch ถ้าคำขอรวมล้มเหลวครบทุกครั้งที่ลอง ทุกเมืองถือว่าล้มเหลว จึง exit 1 ทันที (ชั่วโมงนั้นไม่มีข้อมูลเลย)
    ส่วนในโหมด per_city เมืองเดียว timeout ถือเป็นเรื่องปกติ (ไม่ทำให้ job แดง)
    ทุกกรณีที่มีเมืองล้มเหลวหรือไม่ผ่าน validation จะพิมพ์ชื่อเมือง และขึ้น ::warning:: บน GitHub
    """
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        print("[ERROR] ไม่พบ SUPABASE_DB_URL ในไฟล์ .env กรุณาตั้งค่าก่อนรัน")
        return 1
    try:
        mode = get_fetch_mode()
    except ValueError as e:
        print(f"[ERROR] {e}")
        return 1

    print(f"=== เริ่มดึงข้อมูล ({datetime.now().isoformat()}) | โหมด: {mode} ===")

    # เช็ค database ก่อนเริ่ม: ถ้าต่อไม่ได้ ไม่มีประโยชน์ที่จะยิง API ไปทิ้งเปล่าๆ
    conn = open_connection(db_url)
    if conn is None:
        print("[FAILED] ต่อ database ไม่ได้ ยกเลิกรอบนี้")
        _github_annotation("error", "ต่อ database ไม่ได้ ยกเลิกรอบนี้")
        return 1

    total = len(LOCATIONS)
    saved = 0
    rejected = 0
    failed = 0
    failed_names = []
    rejected_names = []

    try:
        batch_items = None
        if mode == "batch":
            print(f"\nกำลังดึงข้อมูลของ {total} เมืองด้วยคำขอเดียว...")
            batch_items = fetch_weather_batch(LOCATIONS)
            if batch_items is None:
                # คำขอรวมล้มเหลวครบทุกครั้ง: ไม่มีข้อมูลของเมืองใดเลย ไม่ต้องวนต่อ
                failed = total
                failed_names = [loc["name"] for loc in LOCATIONS]

        for index, location in enumerate(LOCATIONS):
            if mode == "batch" and batch_items is None:
                break
            name = location["name"]
            print(f"\nกำลังดึงข้อมูลของ {name}..." if mode == "per_city"
                  else f"\nกำลังประมวลผลข้อมูลของ {name}...")

            if mode == "batch":
                raw_data = batch_items[index]
                problem = verify_batch_item(location, raw_data)
                if problem:
                    print(f"[ERROR] {name}: {problem}")
                    raw_data = None
            else:
                raw_data = fetch_weather_for_location(location)

            if raw_data is None:
                failed += 1
                failed_names.append(name)
                continue  # ข้ามไปเมืองถัดไป ไม่ทำให้ทั้ง pipeline ล้ม

            weather_record = parse_weather_data(raw_data, location)
            if weather_record is None:
                failed += 1
                failed_names.append(name)
                continue

            validation_errors = validate_weather_record(weather_record)
            if validation_errors:
                print(f"  [VALIDATION FAILED] ข้อมูลของ {name} ไม่ผ่านการตรวจสอบ:")
                for err in validation_errors:
                    print(f"    - {err}")
                rejected += 1
                rejected_names.append(name)
                continue  # ไม่เก็บข้อมูลที่ไม่ผ่าน validation ลง database

            # ถ้า connection หลุดระหว่างรอบ ลองเปิดใหม่ 1 ครั้งก่อนบันทึก
            if conn is None or conn.closed:
                conn = open_connection(db_url)
            if conn is None:
                failed += 1
                failed_names.append(name)
                continue

            success = save_to_database(weather_record, conn)
            if not success and conn.closed:
                # สายหลุดตอนบันทึก: เปิดใหม่แล้วลองบันทึกเมืองนี้ซ้ำอีก 1 ครั้ง
                # (ข้อมูลของ API เป็นค่า "ปัจจุบัน" ดึงย้อนหลังไม่ได้ ถ้าทิ้งไปจะเป็นช่องว่างถาวร)
                conn = open_connection(db_url)
                if conn is not None:
                    success = save_to_database(weather_record, conn)
            if success:
                saved += 1
                print(f"  บันทึกสำเร็จ: {weather_record['temperature_c']}°C, "
                      f"ความชื้น {weather_record['humidity_percent']}%")
            else:
                failed += 1
                failed_names.append(name)
                print(f"  บันทึกไม่สำเร็จสำหรับ {name}")

            if mode == "per_city":
                time.sleep(1)  # หน่วงเวลาก่อนไปเมืองถัดไป ลดโอกาสโดน rate limit/timeout
                # จากการยิง request รัวติดกันเร็วเกินไป (ดู DECISIONS.md ข้อ 17)
    finally:
        if conn is not None and not conn.closed:
            conn.close()

    print(f"\n=== เสร็จสิ้น ({datetime.now().isoformat()}) ===")
    print(f"สรุป: บันทึกสำเร็จ {saved}/{total} | ไม่ผ่าน validation {rejected} | ล้มเหลว {failed}")
    if failed_names:
        print("เมืองที่ล้มเหลว: " + ", ".join(failed_names))
    if rejected_names:
        print("เมืองที่ไม่ผ่าน validation: " + ", ".join(rejected_names))

    summary_lines = [f"### Fetch Weather Data (โหมด {mode})", f"- บันทึกสำเร็จ: {saved}/{total}"]
    if failed_names:
        summary_lines.append(f"- เมืองที่ล้มเหลว ({failed}): {', '.join(failed_names)}")
    if rejected_names:
        summary_lines.append(f"- ไม่ผ่าน validation ({rejected}): {', '.join(rejected_names)}")
    _write_step_summary(summary_lines)

    if failed_names or rejected_names:
        detail = f"บันทึกสำเร็จ {saved}/{total}"
        if failed_names:
            detail += f" | ล้มเหลว: {', '.join(failed_names)}"
        if rejected_names:
            detail += f" | ไม่ผ่าน validation: {', '.join(rejected_names)}"
        _github_annotation("warning", detail)

    if saved == 0:
        print("[FAILED] ไม่มีข้อมูลถูกบันทึกเลยในรอบนี้")
        _github_annotation("error", "ไม่มีข้อมูลถูกบันทึกเลยในรอบนี้")
        return 1
    if failed > total * MAX_FAILURE_RATIO:
        print(f"[FAILED] ล้มเหลว {failed}/{total} เมือง เกินเกณฑ์ที่ยอมรับได้ "
              f"({MAX_FAILURE_RATIO:.0%})")
        _github_annotation("error", f"ล้มเหลว {failed}/{total} เมือง เกินเกณฑ์ที่ยอมรับได้")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
