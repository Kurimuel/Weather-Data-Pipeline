"""
bigquery_sync.py

Sync ข้อมูลจาก Supabase (PostgreSQL, OLTP) ไปยัง BigQuery (OLAP)
ออกแบบให้เป็น incremental sync (sync เฉพาะข้อมูลใหม่) ไม่ใช่ full reload
ทุกครั้ง เพื่อประหยัด BigQuery quota และเร็วกว่าเมื่อข้อมูลเยอะขึ้น

วิธีรัน:
    python3 bigquery_sync.py

Setup ที่ต้องทำก่อนรัน (ดูรายละเอียดใน README.md):
    1. สร้าง GCP project + เปิดใช้ BigQuery API
    2. สร้าง Service Account ให้สิทธิ์ "BigQuery Data Editor"
    3. ดาวน์โหลด JSON key ของ Service Account มาเก็บไว้ในเครื่อง (ห้าม commit)
    4. ตั้งค่าใน .env: GOOGLE_APPLICATION_CREDENTIALS, BQ_PROJECT_ID,
       BQ_DATASET, BQ_TABLE
"""

import os
import psycopg
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

load_dotenv()

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET", "weather_pipeline")
BQ_TABLE = os.getenv("BQ_TABLE", "weather_readings")


def get_last_synced_time(bq_client: bigquery.Client, table_ref: str) -> datetime | None:
    """
    หาว่า reading_time ล่าสุดที่เคย sync ไป BigQuery แล้วคือเมื่อไหร่
    เพื่อใช้เป็นจุดเริ่มต้นของการดึงข้อมูลใหม่รอบนี้ (incremental sync)

    คืนค่า None ถ้าตาราง BigQuery ยังไม่มีข้อมูลเลย (sync ครั้งแรก)
    """
    query = f"SELECT MAX(reading_time) AS last_time FROM `{table_ref}`"
    try:
        result = bq_client.query(query).result()
        row = list(result)[0]
        return row.last_time  # None ถ้าตารางว่างเปล่า
    except NotFound:
        # ตารางยังไม่เคยถูกสร้าง แปลว่านี่คือ sync ครั้งแรก
        return None


def fetch_new_records_from_supabase(since: datetime | None) -> pd.DataFrame:
    """
    ดึงข้อมูลจาก Supabase เฉพาะที่ใหม่กว่า `since`
    ถ้า since เป็น None (sync ครั้งแรก) จะดึงข้อมูลทั้งหมด

    ตั้งใจ JOIN กับ locations แล้ว "flatten" เป็นตารางเดียวก่อนส่งเข้า
    BigQuery (denormalize) ต่างจาก Supabase ที่ normalize ไว้ เพราะ
    BigQuery เป็น OLAP ที่เหมาะกับตารางแบบ flat มากกว่า การ JOIN ซ้ำ
    ทุกครั้งตอน query วิเคราะห์ (ดู DECISIONS.md ข้อ 15)
    """
    base_query = """
        SELECT
            l.name AS location_name,
            l.country,
            l.latitude,
            l.longitude,
            wr.reading_time,
            wr.temperature_c,
            wr.humidity_percent,
            wr.wind_speed_kmh,
            wr.fetched_at
        FROM weather_readings wr
        JOIN locations l ON wr.location_id = l.id
    """

    if since is None:
        query = base_query + " ORDER BY wr.reading_time ASC;"
        params = None
    else:
        query = base_query + " WHERE wr.reading_time > %s ORDER BY wr.reading_time ASC;"
        params = (since,)

    with psycopg.connect(SUPABASE_DB_URL) as conn:
        df = pd.read_sql(query, conn, params=params)

    return df


def sync_to_bigquery(df: pd.DataFrame, bq_client: bigquery.Client, table_ref: str):
    """
    เขียนข้อมูลเข้า BigQuery แบบ append (ไม่เขียนทับของเดิม)

    กำหนด schema เองชัดเจนแทนที่จะใช้ autodetect=True เพราะเคยเจอปัญหา
    จริง: autodetect เดา schema ผิดตอน sync ครั้งแรก (เดา column country
    เป็น int64 แทนที่จะเป็น string) ทำให้ sync ครั้งถัดๆ มาพังเพราะข้อมูล
    จริงเป็น string แต่ table ถูกล็อก schema แบบผิดไปแล้ว (ดู DECISIONS.md
    ข้อ 18) การกำหนด schema เองตั้งแต่ต้นทำให้ type ถูกต้องเสมอ ไม่ขึ้นกับ
    การเดาจากข้อมูลตัวอย่างที่อาจกำกวม
    """
    schema = [
        bigquery.SchemaField("location_name", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("reading_time", "TIMESTAMP"),
        bigquery.SchemaField("temperature_c", "FLOAT64"),
        bigquery.SchemaField("humidity_percent", "INTEGER"),
        bigquery.SchemaField("wind_speed_kmh", "FLOAT64"),
        bigquery.SchemaField("fetched_at", "TIMESTAMP"),
    ]

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=schema,
    )
    load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()  # รอจนกว่า job จะเสร็จ


def main():
    if not SUPABASE_DB_URL:
        print("[ERROR] ไม่พบ SUPABASE_DB_URL ใน .env")
        return
    if not BQ_PROJECT_ID:
        print("[ERROR] ไม่พบ BQ_PROJECT_ID ใน .env")
        return

    print(f"=== เริ่ม sync ไป BigQuery ({datetime.now(timezone.utc).isoformat()}) ===")

    bq_client = bigquery.Client(project=BQ_PROJECT_ID)
    table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    last_synced = get_last_synced_time(bq_client, table_ref)
    if last_synced:
        print(f"Sync ครั้งล่าสุด: {last_synced} — จะดึงเฉพาะข้อมูลที่ใหม่กว่านี้")
    else:
        print("ยังไม่เคย sync มาก่อน — จะดึงข้อมูลทั้งหมด")

    new_data = fetch_new_records_from_supabase(since=last_synced)

    if new_data.empty:
        print("ไม่มีข้อมูลใหม่ให้ sync")
        return

    print(f"พบข้อมูลใหม่ {len(new_data)} record กำลัง sync ไป BigQuery...")
    sync_to_bigquery(new_data, bq_client, table_ref)
    print(f"Sync สำเร็จ: {len(new_data)} record")

    print(f"=== เสร็จสิ้น ({datetime.now(timezone.utc).isoformat()}) ===")


if __name__ == "__main__":
    main()
