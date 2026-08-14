"""
weather_embeddings.py

สร้าง text description + vector embedding จากข้อมูลอากาศเชิงตัวเลข
แล้วใช้ pgvector ทำ similarity search ("หา reading ที่สภาพอากาศ
คล้ายกันที่สุด") — เป็น demo พื้นฐานที่แสดงหลักการเดียวกับที่ใช้ใน
RAG (Retrieval-Augmented Generation): แปลงข้อมูลเป็น embedding แล้ว
ค้นหาด้วยความใกล้เคียงเชิงความหมาย แทนการค้นหาด้วยคำที่ตรงกันเป๊ะ

วิธีรัน:
    1. สร้าง embedding ให้ข้อมูลทั้งหมดที่ยังไม่มี:
       python3 weather_embeddings.py --generate
    2. ค้นหา reading ที่คล้ายกับ reading ล่าสุดของเมืองหนึ่ง:
       python3 weather_embeddings.py --search "Bangkok"
"""

import os
import sys
import psycopg
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from pgvector.psycopg import register_vector

load_dotenv()

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")

# Model ขนาดเล็ก (ประมาณ 80MB) เหมาะกับงาน demo ให้ vector ขนาด 384 มิติ
# (ต้องตรงกับ vector(384) ที่กำหนดไว้ใน migration_002_add_pgvector.sql)
MODEL_NAME = "all-MiniLM-L6-v2"


def describe_weather(temperature_c: float, humidity_percent: int, wind_speed_kmh: float) -> str:
    """
    แปลงตัวเลขอากาศเป็นประโยคภาษาอังกฤษง่ายๆ

    ใช้กฎง่ายๆ (rule-based) ไม่ซับซ้อน เพราะจุดประสงค์ของ demo นี้คือ
    แสดงหลักการของ "แปลงข้อมูลเป็น text แล้วทำ embedding" ไม่ใช่การทำ
    natural language generation ที่ซับซ้อน
    """
    if temperature_c >= 35:
        temp_desc = "very hot"
    elif temperature_c >= 28:
        temp_desc = "hot"
    elif temperature_c >= 20:
        temp_desc = "mild"
    elif temperature_c >= 10:
        temp_desc = "cool"
    else:
        temp_desc = "cold"

    if humidity_percent >= 80:
        humidity_desc = "very humid"
    elif humidity_percent >= 60:
        humidity_desc = "humid"
    elif humidity_percent >= 40:
        humidity_desc = "moderate humidity"
    else:
        humidity_desc = "dry"

    if wind_speed_kmh >= 40:
        wind_desc = "strong winds"
    elif wind_speed_kmh >= 20:
        wind_desc = "moderate winds"
    elif wind_speed_kmh >= 5:
        wind_desc = "light winds"
    else:
        wind_desc = "calm winds"

    return f"{temp_desc} weather, {humidity_desc}, with {wind_desc}"


def generate_embeddings():
    """สร้าง text description + embedding ให้ทุก record ที่ยังไม่มี"""
    model = SentenceTransformer(MODEL_NAME)

    with psycopg.connect(SUPABASE_DB_URL) as conn:
        register_vector(conn)  # ให้ psycopg รู้จักชนิดข้อมูล vector ของ pgvector
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, temperature_c, humidity_percent, wind_speed_kmh
            FROM weather_readings
            WHERE embedding IS NULL
              AND temperature_c IS NOT NULL
              AND humidity_percent IS NOT NULL
              AND wind_speed_kmh IS NOT NULL;
        """)
        rows = cursor.fetchall()

        if not rows:
            print("ทุก record มี embedding ครบแล้ว ไม่มีอะไรต้องทำเพิ่ม")
            return

        print(f"พบ {len(rows)} record ที่ยังไม่มี embedding กำลังสร้าง...")

        for row_id, temp, humidity, wind in rows:
            description = describe_weather(float(temp), int(humidity), float(wind))
            embedding = model.encode(description)  # คืนค่าเป็น numpy array 384 มิติ

            cursor.execute("""
                UPDATE weather_readings
                SET weather_description = %s, embedding = %s
                WHERE id = %s;
            """, (description, embedding, row_id))

        conn.commit()
        print(f"สร้าง embedding สำเร็จ {len(rows)} record")


def find_similar_weather(location_name: str, limit: int = 5):
    """
    หา reading ที่สภาพอากาศ "คล้าย" กับ reading ล่าสุดของเมืองที่ระบุมากที่สุด

    ใช้ cosine distance operator (<=>) ของ pgvector วัดความใกล้เคียง
    ระหว่าง embedding สองตัว ยิ่งค่าน้อยยิ่งใกล้เคียงกันมาก (0 = เหมือนกันเป๊ะ)
    """
    with psycopg.connect(SUPABASE_DB_URL) as conn:
        register_vector(conn)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT wr.id, wr.embedding, wr.weather_description, wr.reading_time
            FROM weather_readings wr
            JOIN locations l ON wr.location_id = l.id
            WHERE l.name = %s AND wr.embedding IS NOT NULL
            ORDER BY wr.reading_time DESC
            LIMIT 1;
        """, (location_name,))
        reference = cursor.fetchone()

        if reference is None:
            print(f"ไม่พบข้อมูลที่มี embedding สำหรับเมือง '{location_name}' "
                  f"ลองรัน --generate ก่อน")
            return

        ref_id, ref_embedding, ref_description, ref_time = reference
        print(f"อ้างอิงจาก: {location_name} เมื่อ {ref_time}")
        print(f"สภาพอากาศ: {ref_description}\n")

        cursor.execute("""
            SELECT l.name, wr.reading_time, wr.weather_description,
                   wr.embedding <=> %s AS distance
            FROM weather_readings wr
            JOIN locations l ON wr.location_id = l.id
            WHERE wr.id != %s AND wr.embedding IS NOT NULL
            ORDER BY distance ASC
            LIMIT %s;
        """, (ref_embedding, ref_id, limit))
        results = cursor.fetchall()

        print(f"สภาพอากาศที่คล้ายกันที่สุด {limit} อันดับแรก:")
        for name, reading_time, description, distance in results:
            print(f"  [{distance:.4f}] {name} ({reading_time}): {description}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("วิธีใช้: python3 weather_embeddings.py --generate")
        print("     หรือ: python3 weather_embeddings.py --search <ชื่อเมือง>")
        sys.exit(1)

    if sys.argv[1] == "--generate":
        generate_embeddings()
    elif sys.argv[1] == "--search" and len(sys.argv) >= 3:
        find_similar_weather(sys.argv[2])
    else:
        print("คำสั่งไม่ถูกต้อง ใช้ --generate หรือ --search <ชื่อเมือง>")
