"""
validate_data.py

ตรวจสอบความถูกต้องของข้อมูลอากาศก่อนเก็บลง database
แยกออกจาก fetch_weather.py ตั้งใจ เพื่อให้ทดสอบ validation logic ได้อิสระ
โดยไม่ต้องยิง API หรือต่อ database จริงทุกครั้ง
"""

from datetime import datetime, timezone

# ช่วงค่าที่ยอมรับได้ - อ้างอิงจากสภาพอากาศโลกจริง (ไม่ใช่แค่ของไทย
# เผื่อขยายไปเก็บเมืองอื่นในอนาคตตามที่ออกแบบไว้ใน LOCATIONS list)
VALID_TEMP_RANGE = (-60.0, 60.0)      # °C, ครอบคลุมขั้วโลกถึงทะเลทราย
VALID_HUMIDITY_RANGE = (0, 100)        # %
VALID_WIND_SPEED_RANGE = (0, 500)      # km/h, กันไว้ครอบคลุมพายุรุนแรงสุด


class ValidationError:
    """เก็บรายละเอียดของแต่ละปัญหาที่เจอ เพื่อ log ให้ตรงจุด"""

    def __init__(self, field: str, value, reason: str):
        self.field = field
        self.value = value
        self.reason = reason

    def __str__(self):
        return f"field='{self.field}' value={self.value} reason='{self.reason}'"


def validate_weather_record(record: dict) -> list[ValidationError]:
    """
    ตรวจสอบ 1 record ว่าข้อมูลสมเหตุสมผลไหม

    คืนค่า: list ของ ValidationError (list ว่างหมายถึงผ่านการตรวจสอบทั้งหมด)

    ตั้งใจให้คืนค่าเป็น list สะสมทุกปัญหาที่เจอ แทนที่จะ return False ทันที
    ที่เจอปัญหาแรก เพราะเวลา debug จริงอยากเห็นปัญหาทั้งหมดในครั้งเดียว
    ไม่ใช่ต้องแก้ทีละจุดแล้วรันใหม่หลายรอบ
    """
    errors = []

    temp = record.get("temperature_c")
    if temp is not None:
        if not (VALID_TEMP_RANGE[0] <= temp <= VALID_TEMP_RANGE[1]):
            errors.append(ValidationError(
                "temperature_c", temp,
                f"ค่าอยู่นอกช่วงที่สมเหตุสมผล ({VALID_TEMP_RANGE[0]} ถึง {VALID_TEMP_RANGE[1]} °C)"
            ))

    humidity = record.get("humidity_percent")
    if humidity is not None:
        if not (VALID_HUMIDITY_RANGE[0] <= humidity <= VALID_HUMIDITY_RANGE[1]):
            errors.append(ValidationError(
                "humidity_percent", humidity,
                f"ความชื้นต้องอยู่ระหว่าง {VALID_HUMIDITY_RANGE[0]}-{VALID_HUMIDITY_RANGE[1]}%"
            ))

    wind = record.get("wind_speed_kmh")
    if wind is not None:
        if wind < VALID_WIND_SPEED_RANGE[0]:
            errors.append(ValidationError(
                "wind_speed_kmh", wind,
                "ความเร็วลมติดลบไม่ได้"
            ))
        elif wind > VALID_WIND_SPEED_RANGE[1]:
            errors.append(ValidationError(
                "wind_speed_kmh", wind,
                f"ค่าสูงผิดปกติ (เกิน {VALID_WIND_SPEED_RANGE[1]} km/h)"
            ))

    reading_time = record.get("reading_time")
    if reading_time is not None:
        now = datetime.now(timezone.utc)
        if reading_time > now:
            errors.append(ValidationError(
                "reading_time", reading_time,
                "เวลาของข้อมูลอยู่ในอนาคต (เป็นไปไม่ได้ที่จะมีข้อมูลจากอนาคต)"
            ))

    location_name = record.get("location_name")
    if not location_name or not str(location_name).strip():
        errors.append(ValidationError(
            "location_name", location_name,
            "ชื่อสถานที่ว่างเปล่า"
        ))

    return errors


def is_valid(record: dict) -> bool:
    """เช็คแบบง่าย ใช้ตอนไม่ต้องการรายละเอียด แค่อยากรู้ผ่าน/ไม่ผ่าน"""
    return len(validate_weather_record(record)) == 0


if __name__ == "__main__":
    # ตัวอย่างการทดสอบ validation ด้วยข้อมูลจำลอง (ไม่ต้องยิง API จริง)
    # รันไฟล์นี้ตรงๆ ด้วย `python validate_data.py` เพื่อดูว่า validation ทำงานถูกไหม

    test_cases = [
        {
            "location_name": "Bangkok",
            "temperature_c": 31.0,
            "humidity_percent": 71,
            "wind_speed_kmh": 9.2,
            "reading_time": datetime.now(timezone.utc),
        },  # ควรผ่าน
        {
            "location_name": "Bangkok",
            "temperature_c": 999.0,  # ผิดปกติ
            "humidity_percent": 150,  # ผิดปกติ
            "wind_speed_kmh": -5,     # ผิดปกติ
            "reading_time": datetime.now(timezone.utc),
        },  # ควรไม่ผ่าน 3 จุด
    ]

    for i, case in enumerate(test_cases, start=1):
        errors = validate_weather_record(case)
        print(f"\nTest case {i}: {'PASS' if not errors else 'FAIL'}")
        for err in errors:
            print(f"  - {err}")
