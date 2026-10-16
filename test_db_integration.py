"""
เทสต์ที่ใช้ PostgreSQL จริง (ตรวจ schema.sql และ SQL ใน fetch_weather.py)

จะถูกข้ามอัตโนมัติ ถ้าไม่ได้ตั้ง environment variable TEST_DATABASE_URL

คำแนะนำด้านความปลอดภัย:
  - ใช้ฐานข้อมูลสำหรับ "ทดสอบ" เท่านั้น (เช่น PostgreSQL ในเครื่อง หรือ Supabase project แยก)
    อย่าชี้ไปที่ฐานข้อมูลที่ pipeline ใช้งานจริง
  - แต่ละเทสต์สร้าง schema ชั่วคราวชื่อ pytest_xxxxxxxxxx ของตัวเอง (search_path ชี้ไปที่ schema นั้น
    เท่านั้น) แล้วลบทิ้งด้วย DROP SCHEMA ... CASCADE ตอนจบ จึงไม่แตะตาราง public ของจริง
  - ใช้ connection แบบ direct หรือ session pooler (ไม่ใช่ transaction pooler พอร์ต 6543)
    เพราะต้องกำหนด search_path ผ่าน options

ตัวอย่าง (PostgreSQL ในเครื่อง):
    export TEST_DATABASE_URL="postgresql://postgres:password@localhost:5432/postgres"
    pytest tests/test_db_integration.py -v
"""

import os
import pathlib
import re
import uuid
from datetime import datetime, timezone

import psycopg
import pytest

import fetch_weather as fw

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="ตั้ง TEST_DATABASE_URL (ฐานข้อมูลสำหรับทดสอบเท่านั้น) เพื่อรันเทสต์กลุ่มนี้",
)

SCHEMA_SQL = pathlib.Path(__file__).resolve().parent.parent / "schema.sql"


def read_schema_sql() -> str:
    """
    อ่าน schema.sql แล้วตัดคอมเมนต์ (--) ออกก่อนส่งให้ฐานข้อมูล
    เพราะคอมเมนต์ในไฟล์เป็นภาษาไทย ถ้าฐานข้อมูลทดสอบไม่ได้ใช้ encoding UTF-8
    (เช่น SQL_ASCII หรือ WIN1252 บน Windows) การส่งข้อความไทยจะ error โดยไม่เกี่ยวกับ schema เลย
    """
    text = SCHEMA_SQL.read_text(encoding="utf-8")
    return re.sub(r"--[^\n]*", "", text)

LOCATION = {"name": "Bangkok", "country": "Thailand", "lat": 13.7563, "lon": 100.5018}


def make_record(**overrides) -> dict:
    record = {
        "location_name": "Bangkok",
        "country": "Thailand",
        "latitude": 13.7563,
        "longitude": 100.5018,
        "reading_time": datetime(2026, 9, 1, 0, 15, tzinfo=timezone.utc),
        "temperature_c": 30.1,
        "humidity_percent": 70,
        "wind_speed_kmh": 5.0,
    }
    record.update(overrides)
    return record


@pytest.fixture
def conn():
    """connection ที่ผูกกับ schema ชั่วคราวที่สร้างจาก schema.sql จริง แล้วลบทิ้งเมื่อจบเทสต์"""
    schema = f"pytest_{uuid.uuid4().hex[:10]}"
    with psycopg.connect(TEST_DATABASE_URL, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')

    c = psycopg.connect(
        TEST_DATABASE_URL,
        options=f"-c search_path={schema}",
        client_encoding="UTF8",   # บังคับให้ผลลัพธ์เป็น str เสมอ ไม่ว่า encoding ของฐานข้อมูลทดสอบจะเป็นอะไร
        prepare_threshold=None,
    )
    try:
        c.execute(read_schema_sql())
        c.commit()
        yield c
    finally:
        c.close()
        with psycopg.connect(TEST_DATABASE_URL, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')


def scalar(conn, sql, params=None):
    return conn.execute(sql, params).fetchone()[0]


# ---------------------------------------------------------------------------
# schema.sql
# ---------------------------------------------------------------------------

def test_schema_creates_expected_columns(conn):
    rows = conn.execute("""
        SELECT table_name, column_name FROM information_schema.columns
        WHERE table_schema = current_schema()
    """).fetchall()
    columns = {}
    for table, column in rows:
        columns.setdefault(table, set()).add(column)

    assert columns["locations"] == {"id", "name", "country", "latitude", "longitude"}
    assert columns["weather_readings"] == {
        "id", "location_id", "reading_time", "temperature_c",
        "humidity_percent", "wind_speed_kmh", "fetched_at",
    }


def test_schema_has_expected_constraints_and_indexes(conn):
    constraints = {r[0] for r in conn.execute("""
        SELECT conname FROM pg_constraint WHERE connamespace = current_schema()::regnamespace
    """).fetchall()}
    assert {
        "locations_pkey", "locations_name_key", "weather_readings_pkey",
        "fk_weather_location", "weather_readings_location_id_reading_time_key",
    } <= constraints

    indexes = {r[0] for r in conn.execute(
        "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()"
    ).fetchall()}
    assert {"idx_weather_reading_time", "idx_weather_location_id"} <= indexes


def test_schema_can_be_run_twice(conn):
    """schema.sql ใช้ IF NOT EXISTS ต้องรันซ้ำได้โดยไม่ error"""
    conn.execute(read_schema_sql())
    conn.commit()


def test_foreign_key_rejects_unknown_location(conn):
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        conn.execute(
            "INSERT INTO weather_readings (location_id, reading_time) VALUES (999, now())"
        )
    conn.rollback()


# ---------------------------------------------------------------------------
# get_or_create_location_id
# ---------------------------------------------------------------------------

def test_get_or_create_returns_same_id_for_same_city(conn):
    with conn.cursor() as cur:
        first = fw.get_or_create_location_id(cur, make_record())
        second = fw.get_or_create_location_id(cur, make_record())
    assert first == second
    assert scalar(conn, "SELECT count(*) FROM locations") == 1


def test_get_or_create_gives_different_ids_for_different_cities(conn):
    with conn.cursor() as cur:
        a = fw.get_or_create_location_id(cur, make_record(location_name="Bangkok"))
        b = fw.get_or_create_location_id(cur, make_record(location_name="Tokyo"))
    assert a != b


def test_get_or_create_does_not_burn_serial_ids_on_repeat_calls(conn):
    """
    SELECT ก่อน INSERT ตั้งใจเพื่อไม่ให้เลข SERIAL กระโดดทุกชั่วโมง (DECISIONS.md)
    เรียกซ้ำ 10 ครั้งกับเมืองเดิม แล้วเพิ่มเมืองใหม่ -> ต้องได้ id ถัดไปแบบไม่ข้าม (2)
    """
    with conn.cursor() as cur:
        for _ in range(10):
            fw.get_or_create_location_id(cur, make_record(location_name="Bangkok"))
        new_id = fw.get_or_create_location_id(cur, make_record(location_name="Tokyo"))
    assert new_id == 2


# ---------------------------------------------------------------------------
# save_to_database
# ---------------------------------------------------------------------------

def test_save_inserts_one_row_with_correct_values(conn):
    assert fw.save_to_database(make_record(), conn) is True

    row = conn.execute("""
        SELECT l.name, l.country, wr.reading_time, wr.temperature_c,
               wr.humidity_percent, wr.wind_speed_kmh, wr.fetched_at
        FROM weather_readings wr JOIN locations l ON l.id = wr.location_id
    """).fetchone()
    name, country, reading_time, temp, humidity, wind, fetched_at = row
    assert (name, country) == ("Bangkok", "Thailand")
    assert float(temp) == 30.1
    assert humidity == 70
    assert float(wind) == 5.0
    assert fetched_at is not None


def test_reading_time_roundtrips_as_the_same_utc_instant(conn):
    """กันบั๊ก timezone เงียบๆ (DECISIONS.md ข้อ 8): เวลาที่อ่านกลับมาต้องเป็นวินาทีเดียวกับที่เขียน"""
    record = make_record(reading_time=datetime(2026, 8, 4, 14, 15, tzinfo=timezone.utc))
    fw.save_to_database(record, conn)
    stored = scalar(conn, "SELECT reading_time FROM weather_readings")
    assert stored == record["reading_time"]


def test_saving_same_reading_twice_keeps_one_row_and_reports_success(conn):
    assert fw.save_to_database(make_record(), conn) is True
    assert fw.save_to_database(make_record(), conn) is True
    assert scalar(conn, "SELECT count(*) FROM weather_readings") == 1


def test_different_times_for_same_city_are_both_kept(conn):
    fw.save_to_database(make_record(reading_time=datetime(2026, 9, 1, 0, 15, tzinfo=timezone.utc)), conn)
    fw.save_to_database(make_record(reading_time=datetime(2026, 9, 1, 1, 15, tzinfo=timezone.utc)), conn)
    assert scalar(conn, "SELECT count(*) FROM weather_readings") == 2
    assert scalar(conn, "SELECT count(*) FROM locations") == 1


def test_failed_save_rolls_back_and_connection_stays_usable(conn):
    """
    เมืองที่ชื่อยาวเกิน VARCHAR(100) ต้องล้มเหลว (คืน False) โดยไม่ทิ้งข้อมูลครึ่งๆ กลางๆ
    และ connection เดิมต้องบันทึกเมืองถัดไปได้ (นี่คือหัวใจของการใช้ connection เดียวทั้งรอบ)
    """
    too_long = make_record(location_name="X" * 150)
    assert fw.save_to_database(too_long, conn) is False
    assert scalar(conn, "SELECT count(*) FROM locations") == 0
    assert scalar(conn, "SELECT count(*) FROM weather_readings") == 0

    assert fw.save_to_database(make_record(), conn) is True
    assert scalar(conn, "SELECT count(*) FROM weather_readings") == 1


def test_save_all_real_locations_on_one_connection(conn):
    """จำลอง 1 รอบจริง: 12 เมืองบน connection เดียว ต้องได้ 12 เมือง 12 แถว"""
    for loc in fw.LOCATIONS:
        record = make_record(
            location_name=loc["name"], country=loc["country"],
            latitude=loc["lat"], longitude=loc["lon"],
        )
        assert fw.save_to_database(record, conn) is True
    assert scalar(conn, "SELECT count(*) FROM locations") == len(fw.LOCATIONS)
    assert scalar(conn, "SELECT count(*) FROM weather_readings") == len(fw.LOCATIONS)


def test_connection_from_open_connection_never_creates_prepared_statements():
    """
    ใช้ query ที่ไม่แตะตารางใดๆ (ปลอดภัยต่อฐานข้อมูลที่ใช้ทดสอบ) รัน 12 ครั้งเหมือน 12 เมือง
    บน connection ที่ได้จาก open_connection() แล้วตรวจว่าไม่มี prepared statement เกิดขึ้น
    """
    c = fw.open_connection(TEST_DATABASE_URL)
    assert c is not None
    try:
        for i in range(12):
            c.execute("SELECT %s::int + 1", (i,))
        assert scalar(c, "SELECT count(*) FROM pg_prepared_statements") == 0
    finally:
        c.close()
