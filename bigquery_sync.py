"""
bigquery_sync.py

Sync ข้อมูลจาก Supabase (PostgreSQL, OLTP) ไปยัง BigQuery (OLAP)
ออกแบบให้เป็น incremental sync (sync เฉพาะข้อมูลใหม่) ไม่ใช่ full reload
ทุกครั้ง เพื่อประหยัด BigQuery quota และเร็วกว่าเมื่อข้อมูลเยอะขึ้น

วิธีเขียนเข้า BigQuery: staging table + MERGE (idempotent รันซ้ำกี่ครั้งก็ไม่เกิดข้อมูลซ้ำ):
    1. ดึงจาก Supabase โดยเริ่มก่อนจุดที่ sync ล่าสุดเผื่อไว้ BQ_LOOKBACK_HOURS ชั่วโมง
       (ค่าเริ่มต้น 6) กันข้อมูลที่เพิ่งถูกบันทึกหรือมาช้าตกหล่นตรงรอยต่อของรอบ sync
    2. โหลดเข้า "staging table" ชั่วคราว (ชื่อไม่ซ้ำกันทุกรอบ + หมดอายุเองใน 24 ชั่วโมง)
    3. เติมเฉพาะแถวที่ "ยังไม่มี" ในตารางจริง โดยยึด location_name + reading_time เป็นคีย์
    4. ลบ staging table ทิ้ง (แม้ขั้นตอนก่อนหน้าจะล้มเหลวก็ลบ)
    ส่วนที่ดึงย้อนหลังเผื่อไว้จะซ้ำกับของเดิมโดยตั้งใจ และถูกคัดทิ้งในขั้นที่ 3 เอง

BQ_WRITE_STRATEGY (ตั้งใน .env หรือ GitHub Variables) เลือกวิธีทำขั้นที่ 3:
    merge      (ค่าเริ่มต้น) ใช้คำสั่ง MERGE ซึ่งเป็น DML จึงต้องเปิด billing ในโปรเจกต์ GCP
               BigQuery sandbox / free tier ที่ยังไม่เปิด billing จะรัน DML ไม่ได้
    anti_join  ผลลัพธ์เหมือนกัน แต่ใช้ query job ธรรมดาที่เขียนต่อท้ายตาราง (ไม่ใช่ DML)
               ใช้ได้ใน sandbox ข้อแลกเปลี่ยน: ไม่ atomic เท่า MERGE ถ้ารันซ้อนกัน 2 รอบพร้อมกัน
               อาจเกิดข้อมูลซ้ำได้ (workflow ตั้ง concurrency กันไว้แล้ว)

วิธีรัน:
    python3 bigquery_sync.py

Exit code: 0 = สำเร็จ (รวมถึงกรณีไม่มีข้อมูลใหม่), 1 = ตั้งค่าไม่ครบ/schema ไม่ตรง/เขียน BigQuery ไม่ได้

Setup ที่ต้องทำก่อนรัน (ดูรายละเอียดใน README.md):
    1. สร้าง GCP project + เปิดใช้ BigQuery API
    2. สร้าง Service Account ให้สิทธิ์ "BigQuery Data Editor" (+ "BigQuery Job User" เพื่อรัน query)
    3. ดาวน์โหลด JSON key ของ Service Account มาเก็บไว้ในเครื่อง (ห้าม commit)
    4. ตั้งค่าใน .env: GOOGLE_APPLICATION_CREDENTIALS, BQ_PROJECT_ID,
       BQ_DATASET, BQ_TABLE
"""

import os
import sys
import uuid
import psycopg
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from google.cloud import bigquery
from google.api_core.exceptions import Forbidden, NotFound

load_dotenv()

SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET", "weather_pipeline")
BQ_TABLE = os.getenv("BQ_TABLE", "weather_readings")

DEFAULT_LOOKBACK_HOURS = 6
STAGING_TTL_HOURS = 24
WRITE_STRATEGIES = ("merge", "anti_join")

# schema ของตาราง BigQuery กำหนดเองชัดเจนแทนที่จะใช้ autodetect=True เพราะเคยเจอปัญหา
# จริง: autodetect เดา schema ผิดตอน sync ครั้งแรก (เดา column country เป็น int64
# แทนที่จะเป็น string) ทำให้ sync ครั้งถัดๆ มาพัง (ดู DECISIONS.md ข้อ 18)
SCHEMA = [
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
COLUMNS = [field.name for field in SCHEMA]


class SyncError(RuntimeError):
    """ข้อผิดพลาดที่รู้สาเหตุและมีคำแนะนำให้ผู้ใช้ (ต่างจากบั๊กที่ควรเห็น traceback เต็ม)"""


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


def get_lookback_hours() -> int:
    """อ่านจำนวนชั่วโมงที่ดึงย้อนหลังเผื่อไว้จาก BQ_LOOKBACK_HOURS (ว่าง = ค่าเริ่มต้น 6)"""
    raw = os.getenv("BQ_LOOKBACK_HOURS", "").strip()
    if not raw:
        return DEFAULT_LOOKBACK_HOURS
    try:
        hours = int(raw)
    except ValueError:
        raise ValueError(f"BQ_LOOKBACK_HOURS ต้องเป็นจำนวนเต็ม แต่ได้ {raw!r}") from None
    if hours < 0:
        raise ValueError(f"BQ_LOOKBACK_HOURS ต้องไม่ติดลบ แต่ได้ {hours}")
    return hours


def get_write_strategy() -> str:
    """อ่านวิธีเขียนข้อมูลจาก BQ_WRITE_STRATEGY (ว่าง = merge)"""
    raw = os.getenv("BQ_WRITE_STRATEGY", "").strip().lower()
    if not raw:
        return "merge"
    if raw not in WRITE_STRATEGIES:
        raise ValueError(
            f"BQ_WRITE_STRATEGY ต้องเป็นหนึ่งใน {', '.join(WRITE_STRATEGIES)} แต่ได้ {raw!r}"
        )
    return raw


def compute_fetch_start(last_synced: datetime | None, lookback_hours: int) -> datetime | None:
    """
    จุดเริ่มต้นของการดึงข้อมูลจาก Supabase = sync ล่าสุด ลบด้วยช่วงเผื่อย้อนหลัง

    ทำไมต้องเผื่อ: เดิมใช้ `reading_time > MAX(reading_time)` ตรงๆ ถ้ามี record ของบางเมือง
    ถูกบันทึกหลังจาก sync รอบก่อนอ่านข้อมูลไปแล้ว (เช่น กดรัน sync มือระหว่างที่ fetch กำลังทำงาน
    หรือ cron ของ GitHub ถูกเลื่อนเวลา) แต่เมืองอื่นที่ reading_time เท่ากันถูก sync ไปแล้ว
    record นั้นจะถูกข้ามตลอดกาล การดึงเผื่อย้อนหลังทำให้ซ้ำกับของเดิมได้ แต่ MERGE คัดทิ้งให้เอง
    (ใช้ reading_time ไม่ใช่ fetched_at เพราะ API ให้แต่ค่า "ปัจจุบัน" สองเวลานี้จึงห่างกันแค่ไม่กี่นาที)
    """
    if last_synced is None:
        return None
    return last_synced - timedelta(hours=lookback_hours)


_TYPE_ALIASES = {"FLOAT64": "FLOAT", "INT64": "INTEGER", "BOOL": "BOOLEAN"}


def _normalize_type(type_name: str) -> str:
    """BigQuery API คืนชื่อ type แบบเก่า (FLOAT/INTEGER) ต้องเทียบกับชื่อใหม่ (FLOAT64/INT64) ได้"""
    type_name = type_name.upper()
    return _TYPE_ALIASES.get(type_name, type_name)


def ensure_target_table(bq_client: bigquery.Client, table_ref: str) -> None:
    """
    ตรวจว่าตารางจริงมีอยู่และ schema ตรงกับที่โค้ดคาดหวัง ถ้ายังไม่มีให้สร้าง

    ต้องสร้างเองก่อนเสมอ เพราะ MERGE และ query แบบ anti-join ต้องอ้างถึงตารางที่มีอยู่แล้ว
    (เดิม load job สร้างตารางให้เองตอนรอบแรก) และตรวจ schema ตรงนี้เพื่อจับปัญหาแบบ
    DECISIONS.md ข้อ 18 ให้ได้ตั้งแต่ต้น พร้อมข้อความที่บอกว่าผิดตรงไหน
    แทนที่จะไปเจอ error แปลกๆ กลางคันตอน MERGE
    column ที่เพิ่มเข้ามาในตารางเองนอกเหนือจากนี้ไม่ถือว่าผิด
    """
    try:
        table = bq_client.get_table(table_ref)
    except NotFound:
        bq_client.create_table(bigquery.Table(table_ref, schema=SCHEMA), exists_ok=True)
        print(f"สร้างตาราง {table_ref} ใหม่ (ยังไม่เคยมี)")
        return

    existing = {field.name: _normalize_type(field.field_type) for field in table.schema}
    problems = []
    for field in SCHEMA:
        expected = _normalize_type(field.field_type)
        actual = existing.get(field.name)
        if actual is None:
            problems.append(f"ไม่มี column {field.name}")
        elif actual != expected:
            problems.append(f"column {field.name}: ควรเป็น {expected} แต่ตารางเป็น {actual}")
    if problems:
        raise SyncError(
            f"schema ของ {table_ref} ไม่ตรงกับที่โค้ดคาดหวัง: " + "; ".join(problems) + ". "
            "ถ้าข้อมูลใน BigQuery สร้างซ้ำจาก Supabase ได้ ให้ลบตารางทิ้งแล้วรันใหม่ "
            "(ดู DECISIONS.md ข้อ 18)"
        )


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


def _deduplicated_staging_sql(staging_ref: str) -> str:
    """
    เลือกจาก staging โดยคัดแถวที่คีย์ซ้ำกันเองให้เหลือแถวเดียว (เอา fetched_at ล่าสุด)
    ปกติ Supabase มี UNIQUE constraint กันซ้ำอยู่แล้ว นี่คือการป้องกันซ้อนอีกชั้นราคาถูก
    เขียนด้วย ROW_NUMBER() + subquery แทน QUALIFY เพราะ QUALIFY ของ BigQuery
    บังคับให้มี WHERE/GROUP BY/HAVING ประกอบด้วย ซึ่งเป็นกับดักที่เจอบ่อย
    """
    cols = ", ".join(COLUMNS)
    return (
        f"SELECT {cols} FROM (\n"
        f"    SELECT {cols},\n"
        f"           ROW_NUMBER() OVER (\n"
        f"               PARTITION BY location_name, reading_time\n"
        f"               ORDER BY fetched_at DESC\n"
        f"           ) AS rn\n"
        f"    FROM `{staging_ref}`\n"
        f") WHERE rn = 1"
    )


def build_merge_sql(target_ref: str, staging_ref: str) -> str:
    """
    MERGE: เติมแถวที่ยังไม่มีในตารางจริง (ไม่แก้ไข/ลบแถวเดิม)
    มีแต่ WHEN NOT MATCHED เพราะข้อมูลอากาศเป็น append-only (ดู DECISIONS.md ข้อ 13)
    """
    cols = ", ".join(COLUMNS)
    values = ", ".join(f"S.{c}" for c in COLUMNS)
    return (
        f"MERGE `{target_ref}` AS T\n"
        f"USING (\n{_deduplicated_staging_sql(staging_ref)}\n) AS S\n"
        f"ON T.location_name = S.location_name AND T.reading_time = S.reading_time\n"
        f"WHEN NOT MATCHED THEN\n"
        f"  INSERT ({cols})\n"
        f"  VALUES ({values})"
    )


def build_anti_join_sql(target_ref: str, staging_ref: str) -> str:
    """SELECT แถวจาก staging ที่ยังไม่มีในตารางจริง (ใช้กับ query job ที่เขียนต่อท้ายตาราง ไม่ใช่ DML)"""
    cols = ", ".join(f"S.{c}" for c in COLUMNS)
    return (
        f"SELECT {cols}\n"
        f"FROM (\n{_deduplicated_staging_sql(staging_ref)}\n) AS S\n"
        f"WHERE NOT EXISTS (\n"
        f"    SELECT 1 FROM `{target_ref}` AS T\n"
        f"    WHERE T.location_name = S.location_name AND T.reading_time = S.reading_time\n"
        f")"
    )


def _staging_ref(table_ref: str) -> str:
    """ชื่อ staging table ไม่ซ้ำกันทุกรอบ รอบที่รันซ้อนกันจะไม่เหยียบข้อมูลของกันและกัน"""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{table_ref}_staging_{stamp}_{uuid.uuid4().hex[:8]}"


def load_to_staging(df: pd.DataFrame, bq_client: bigquery.Client, staging_ref: str) -> int | None:
    """
    สร้าง staging table (มีวันหมดอายุกันตกค้างถ้าโปรเซสตายกลางทาง) แล้วโหลด DataFrame เข้าไป
    คืนจำนวนแถวที่ BigQuery รายงานว่าโหลดเข้า (None ถ้าไม่รายงาน)
    """
    table = bigquery.Table(staging_ref, schema=SCHEMA)
    table.expires = datetime.now(timezone.utc) + timedelta(hours=STAGING_TTL_HOURS)
    bq_client.create_table(table)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=SCHEMA,
    )
    load_job = bq_client.load_table_from_dataframe(df, staging_ref, job_config=job_config)
    load_job.result()  # รอจนกว่า job จะเสร็จ

    staged = load_job.output_rows
    if staged is not None and staged != len(df):
        raise SyncError(
            f"โหลดเข้า staging ได้ {staged} แถว แต่ควรเป็น {len(df)} แถว ยกเลิกก่อนแตะตารางจริง"
        )
    return staged


def _count_rows(bq_client: bigquery.Client, table_ref: str) -> int:
    """นับแถวทั้งหมดของตาราง (BigQuery ตอบ COUNT(*) ของทั้งตารางจาก metadata ไม่เสียค่าสแกน)"""
    rows = list(bq_client.query(f"SELECT COUNT(*) AS n FROM `{table_ref}`").result())
    return rows[0].n


def merge_staging_into_target(
    bq_client: bigquery.Client, target_ref: str, staging_ref: str, strategy: str
) -> int | None:
    """
    เติมแถวใหม่จาก staging เข้าตารางจริงตาม strategy คืนจำนวนแถวที่เติมเข้าไปจริง
    (None ถ้า BigQuery ไม่รายงานจำนวน)
    """
    try:
        if strategy == "merge":
            job = bq_client.query(build_merge_sql(target_ref, staging_ref))
            job.result()
            return job.num_dml_affected_rows

        job_config = bigquery.QueryJobConfig(
            destination=target_ref,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        # หาจำนวนที่เติมจริงด้วยการนับแถวของตารางจริง "ก่อนและหลัง" ห้ามใช้ job.result().total_rows
        # เพราะเมื่อ query เขียนลงตารางปลายทาง ค่านั้นคือจำนวนแถว "ทั้งตาราง" ไม่ใช่จำนวนที่เพิ่งเติม
        # (เจอจริงบน BigQuery: รายงาน 4020 ทั้งที่ดึงมาแค่ 10 แถว) วิธีนี้ถูกต้องเพราะ workflow ตั้ง
        # concurrency ให้ sync รันทีละรอบ จึงไม่มีใครเขียนตารางนี้ระหว่างที่นับ
        before = _count_rows(bq_client, target_ref)
        bq_client.query(build_anti_join_sql(target_ref, staging_ref), job_config=job_config).result()
        after = _count_rows(bq_client, target_ref)
        return after - before if after >= before else None

    except Forbidden as e:
        text = str(e).lower()
        if strategy == "merge" and ("dml" in text or "billing" in text or "free tier" in text):
            raise SyncError(
                "BigQuery ไม่อนุญาตให้รัน MERGE (เป็นคำสั่ง DML) ในโปรเจกต์นี้ ซึ่งมักเกิดกับ sandbox/free tier "
                "ที่ยังไม่เปิด billing ทางเลือก: (1) เปิด billing ให้โปรเจกต์ GCP หรือ "
                "(2) ตั้ง BQ_WRITE_STRATEGY=anti_join เพื่อใช้วิธีที่ไม่ใช้ DML"
            ) from e
        raise


def _drop_staging(bq_client: bigquery.Client, staging_ref: str) -> None:
    """ลบ staging table ทิ้ง ถ้าลบไม่ได้ก็แค่เตือน (ไม่บังข้อผิดพลาดจริง และตารางหมดอายุเองใน 24 ชม.)"""
    try:
        bq_client.delete_table(staging_ref, not_found_ok=True)
    except Exception as e:  # noqa: BLE001 - การลบล้มเหลวต้องไม่ทำให้ sync ที่สำเร็จแล้วกลายเป็นล้มเหลว
        print(f"[WARNING] ลบ staging table {staging_ref} ไม่สำเร็จ: {e} (จะหมดอายุเองใน {STAGING_TTL_HOURS} ชม.)")


def sync_to_bigquery(
    df: pd.DataFrame, bq_client: bigquery.Client, table_ref: str, strategy: str = "merge"
) -> dict:
    """
    เขียนข้อมูลเข้า BigQuery แบบ staging + เติมเฉพาะแถวที่ยังไม่มี (ไม่เขียนทับ ไม่ซ้ำ)

    ถ้าขั้นใดล้มเหลว ตารางจริงจะไม่ถูกแตะ (โหลดเข้า staging ก่อนเสมอ) รอบถัดไปจึงเริ่มใหม่ได้
    และเพราะการเติมแถวเป็น idempotent ต่อให้รอบที่ล้มไปทำไปได้ครึ่งทาง การรันซ้ำก็ไม่ทำให้ซ้ำ

    คืน dict: fetched (จำนวนแถวที่ดึงมา), staged, inserted (จำนวนที่เติมเข้าตารางจริงจริงๆ)
    """
    staging_ref = _staging_ref(table_ref)
    try:
        staged = load_to_staging(df, bq_client, staging_ref)
        inserted = merge_staging_into_target(bq_client, table_ref, staging_ref, strategy)
    finally:
        _drop_staging(bq_client, staging_ref)
    return {"fetched": len(df), "staged": staged, "inserted": inserted}


def main() -> int:
    if not SUPABASE_DB_URL:
        print("[ERROR] ไม่พบ SUPABASE_DB_URL ใน .env")
        return 1
    if not BQ_PROJECT_ID:
        print("[ERROR] ไม่พบ BQ_PROJECT_ID ใน .env")
        return 1
    try:
        strategy = get_write_strategy()
        lookback_hours = get_lookback_hours()
    except ValueError as e:
        print(f"[ERROR] {e}")
        return 1

    print(f"=== เริ่ม sync ไป BigQuery ({datetime.now(timezone.utc).isoformat()}) ===")
    print(f"วิธีเขียน: {strategy} | ดึงย้อนหลังเผื่อ: {lookback_hours} ชั่วโมง")

    bq_client = bigquery.Client(project=BQ_PROJECT_ID)
    table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    try:
        ensure_target_table(bq_client, table_ref)

        last_synced = get_last_synced_time(bq_client, table_ref)
        since = compute_fetch_start(last_synced, lookback_hours)
        if last_synced:
            print(f"Sync ครั้งล่าสุด: {last_synced} — จะดึงข้อมูลตั้งแต่ {since} เป็นต้นไป "
                  f"(เผื่อย้อนหลัง {lookback_hours} ชั่วโมง ส่วนที่ซ้ำจะถูกคัดทิ้งเอง)")
        else:
            print("ยังไม่เคย sync มาก่อน — จะดึงข้อมูลทั้งหมด")

        new_data = fetch_new_records_from_supabase(since=since)

        if new_data.empty:
            print("ไม่มีข้อมูลใหม่ให้ sync")
            return 0

        print(f"พบข้อมูล {len(new_data)} record กำลัง sync ไป BigQuery...")
        stats = sync_to_bigquery(new_data, bq_client, table_ref, strategy)
    except SyncError as e:
        print(f"[ERROR] {e}")
        return 1

    inserted = stats["inserted"]
    if inserted is None:
        print(f"Sync สำเร็จ: ส่งเข้า staging {stats['fetched']} record "
              f"(BigQuery ไม่รายงานจำนวนที่เติมเข้าตารางจริง)")
    elif inserted > stats["fetched"]:
        print(f"[WARNING] นับจำนวนที่เติมได้ {inserted} แต่ดึงมาเพียง {stats['fetched']} record ซึ่งเป็นไปไม่ได้ "
              f"(อาจมีการเขียนตารางนี้พร้อมกัน) กรุณาตรวจตารางด้วยมือ")
    else:
        skipped = stats["fetched"] - inserted
        print(f"Sync สำเร็จ: เติมใหม่ {inserted} record, ข้ามเพราะมีอยู่แล้ว {skipped} record "
              f"(จากที่ดึงมา {stats['fetched']})")

    print(f"=== เสร็จสิ้น ({datetime.now(timezone.utc).isoformat()}) ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
