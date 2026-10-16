"""
เทสต์ของ validate_data.py

ไม่ต้องใช้ database หรืออินเทอร์เน็ต รันเร็ว (ไม่ถึงวินาที)
รันด้วย:  pytest tests/test_validate_data.py -v
"""

from datetime import datetime, timedelta, timezone

import pytest

from validate_data import (
    VALID_HUMIDITY_RANGE,
    VALID_TEMP_RANGE,
    VALID_WIND_SPEED_RANGE,
    ValidationError,
    is_valid,
    validate_weather_record,
)


def make_record(**overrides) -> dict:
    """สร้าง record ที่ 'ผ่านทุกข้อ' เป็นฐาน แล้วให้แต่ละเทสต์แก้เฉพาะช่องที่สนใจ"""
    record = {
        "location_name": "Bangkok",
        "temperature_c": 31.0,
        "humidity_percent": 71,
        "wind_speed_kmh": 9.2,
        "reading_time": datetime.now(timezone.utc) - timedelta(minutes=5),
    }
    record.update(overrides)
    return record


def failed_fields(errors) -> set:
    return {e.field for e in errors}


# ---------------------------------------------------------------------------
# กรณีปกติ
# ---------------------------------------------------------------------------

def test_valid_record_passes():
    assert validate_weather_record(make_record()) == []


def test_is_valid_true_for_good_record():
    assert is_valid(make_record()) is True


def test_is_valid_false_for_bad_record():
    assert is_valid(make_record(temperature_c=999.0)) is False


# ---------------------------------------------------------------------------
# ค่าขอบของช่วงที่ยอมรับ (boundary) — จุดที่บั๊กแบบ < กับ <= ชอบซ่อนอยู่
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("temp, ok", [
    (-60.0, True),      # ขอบล่างพอดี ต้องผ่าน
    (60.0, True),       # ขอบบนพอดี ต้องผ่าน
    (0, True),
    (-60.1, False),
    (60.1, False),
    (999.0, False),
    (-273.15, False),   # ต่ำกว่าศูนย์สัมบูรณ์
])
def test_temperature_boundaries(temp, ok):
    errors = validate_weather_record(make_record(temperature_c=temp))
    assert (errors == []) is ok
    if not ok:
        assert failed_fields(errors) == {"temperature_c"}


@pytest.mark.parametrize("humidity, ok", [
    (0, True),
    (100, True),
    (55.5, True),
    (-1, False),
    (101, False),
    (150, False),
])
def test_humidity_boundaries(humidity, ok):
    errors = validate_weather_record(make_record(humidity_percent=humidity))
    assert (errors == []) is ok
    if not ok:
        assert failed_fields(errors) == {"humidity_percent"}


@pytest.mark.parametrize("wind, ok", [
    (0, True),
    (500, True),
    (500.1, False),
    (-0.1, False),
    (-5, False),
])
def test_wind_speed_boundaries(wind, ok):
    errors = validate_weather_record(make_record(wind_speed_kmh=wind))
    assert (errors == []) is ok
    if not ok:
        assert failed_fields(errors) == {"wind_speed_kmh"}


def test_wind_negative_and_too_high_give_different_reasons():
    negative = validate_weather_record(make_record(wind_speed_kmh=-5))[0]
    too_high = validate_weather_record(make_record(wind_speed_kmh=900))[0]
    assert "ติดลบ" in negative.reason
    assert "เกิน" in too_high.reason
    assert negative.reason != too_high.reason


@pytest.mark.parametrize("field", ["temperature_c", "humidity_percent", "wind_speed_kmh"])
def test_nan_is_rejected(field):
    """NaN เทียบแล้วเป็น False เสมอ ต้องไม่หลุดผ่านไปเป็นข้อมูลที่ 'ดูเหมือนปกติ'"""
    errors = validate_weather_record(make_record(**{field: float("nan")}))
    assert failed_fields(errors) == {field}


@pytest.mark.parametrize("field", ["temperature_c", "humidity_percent", "wind_speed_kmh"])
@pytest.mark.parametrize("value", [float("inf"), float("-inf")], ids=["+inf", "-inf"])
def test_infinity_is_rejected(field, value):
    errors = validate_weather_record(make_record(**{field: value}))
    assert failed_fields(errors) == {field}


# ---------------------------------------------------------------------------
# ค่า None / ไม่มี key: ตอนนี้ถูก "ข้าม" ไม่ถือเป็นข้อผิดพลาด
# (ตรงกับ schema ที่ column พวกนี้เป็น NULL ได้) ถ้าวันหนึ่งอยากให้ค่าหายนับเป็นข้อผิดพลาด
# เทสต์กลุ่มนี้จะ fail และเตือนให้ตัดสินใจอีกครั้ง
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("field", [
    "temperature_c", "humidity_percent", "wind_speed_kmh", "reading_time",
])
def test_none_values_are_skipped_not_rejected(field):
    assert validate_weather_record(make_record(**{field: None})) == []


def test_record_with_only_location_name_passes():
    assert validate_weather_record({"location_name": "Bangkok"}) == []


# ---------------------------------------------------------------------------
# reading_time
# ---------------------------------------------------------------------------

def test_past_reading_time_passes():
    old = datetime.now(timezone.utc) - timedelta(days=30)
    assert validate_weather_record(make_record(reading_time=old)) == []


@pytest.mark.parametrize("delta", [timedelta(hours=1), timedelta(days=1), timedelta(days=365)])
def test_future_reading_time_is_rejected(delta):
    future = datetime.now(timezone.utc) + delta
    errors = validate_weather_record(make_record(reading_time=future))
    assert failed_fields(errors) == {"reading_time"}


@pytest.mark.xfail(
    strict=True,
    raises=TypeError,
    reason="ช่องโหว่ที่ทราบ: reading_time แบบไม่มี timezone (naive) ทำให้ validate พังด้วย "
           "TypeError แทนที่จะรายงานเป็น ValidationError ปัจจุบันปลอดภัยเพราะ "
           "parse_weather_data() ใส่ UTC ให้เสมอ ถ้าแก้ validator แล้วเทสต์นี้จะ XPASS "
           "(strict) ให้ลบเครื่องหมาย xfail ออก",
)
def test_naive_reading_time_should_be_reported_not_crash():
    naive = datetime(2026, 1, 1, 12, 0, 0)  # ไม่มี tzinfo
    errors = validate_weather_record(make_record(reading_time=naive))
    assert isinstance(errors, list)


# ---------------------------------------------------------------------------
# location_name
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", [None, "", "   ", "\t\n"])
def test_blank_location_name_is_rejected(name):
    errors = validate_weather_record(make_record(location_name=name))
    assert failed_fields(errors) == {"location_name"}


def test_missing_location_name_key_is_rejected():
    record = make_record()
    del record["location_name"]
    assert failed_fields(validate_weather_record(record)) == {"location_name"}


@pytest.mark.parametrize("name", ["Bangkok", " Bangkok ", "Sao Paulo"])
def test_normal_location_names_pass(name):
    assert validate_weather_record(make_record(location_name=name)) == []


# ---------------------------------------------------------------------------
# พฤติกรรมโดยรวม
# ---------------------------------------------------------------------------

def test_all_problems_are_accumulated_in_one_pass():
    """ออกแบบไว้ให้ 'เห็นปัญหาทั้งหมดในครั้งเดียว' ไม่ใช่หยุดที่ปัญหาแรก"""
    bad = {
        "location_name": "",
        "temperature_c": 999.0,
        "humidity_percent": 150,
        "wind_speed_kmh": -5,
        "reading_time": datetime.now(timezone.utc) + timedelta(days=1),
    }
    errors = validate_weather_record(bad)
    assert len(errors) == 5
    assert failed_fields(errors) == {
        "location_name", "temperature_c", "humidity_percent",
        "wind_speed_kmh", "reading_time",
    }


def test_validation_does_not_mutate_input():
    record = make_record(temperature_c=999.0)
    snapshot = dict(record)
    validate_weather_record(record)
    assert record == snapshot


def test_validation_error_str_contains_field_value_and_reason():
    err = ValidationError("temperature_c", 999.0, "นอกช่วง")
    text = str(err)
    assert "temperature_c" in text
    assert "999.0" in text
    assert "นอกช่วง" in text


def test_error_carries_the_offending_value():
    errors = validate_weather_record(make_record(humidity_percent=150))
    assert errors[0].value == 150


# ---------------------------------------------------------------------------
# ความสอดคล้องกับ schema: ช่วงที่ validator ยอมรับต้อง "ใส่ลง column ได้จริง"
# schema.sql: temperature_c DECIMAL(4,1), wind_speed_kmh DECIMAL(4,1) -> สูงสุด 999.9
# ถ้าใครขยายช่วงใน validator เกินนี้ ข้อมูลที่ผ่าน validation จะไปพังตอน INSERT
# ---------------------------------------------------------------------------

DECIMAL_4_1_MAX = 999.9


def test_validator_ranges_fit_the_database_columns():
    assert max(abs(v) for v in VALID_TEMP_RANGE) <= DECIMAL_4_1_MAX
    assert max(abs(v) for v in VALID_WIND_SPEED_RANGE) <= DECIMAL_4_1_MAX
    # ความชื้นเป็น INTEGER ช่วง 0-100 ปลอดภัยอยู่แล้ว เช็คแค่ว่าไม่ติดลบ
    assert VALID_HUMIDITY_RANGE[0] >= 0


def test_range_tuples_are_ordered_low_to_high():
    for low, high in (VALID_TEMP_RANGE, VALID_HUMIDITY_RANGE, VALID_WIND_SPEED_RANGE):
        assert low < high
