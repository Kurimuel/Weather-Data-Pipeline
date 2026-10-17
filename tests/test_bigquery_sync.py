"""
เทสต์ของ bigquery_sync.py (staging + MERGE)

ไม่ต่อ BigQuery จริง ไม่ต่อ Supabase ไม่ใช้อินเทอร์เน็ต: ใช้ BigQuery client จำลองที่บันทึกว่าถูกเรียกอะไรบ้าง
สิ่งที่เทสต์กลุ่มนี้พิสูจน์คือ "ลำดับและเงื่อนไขของ flow" กับ "หน้าตาของ SQL ที่สร้างขึ้น"
ส่วนที่ว่า SQL นั้นรันบน BigQuery จริงแล้วให้ผลถูกต้องหรือไม่ ต้องรันกับ BigQuery จริง (ดู DECISIONS.md)

รันด้วย:  pytest tests/test_bigquery_sync.py -v
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

pd = pytest.importorskip("pandas")
bigquery = pytest.importorskip("google.cloud.bigquery")
from google.api_core.exceptions import Forbidden, NotFound  # noqa: E402

import bigquery_sync as bs  # noqa: E402

TARGET = "my-proj.weather_pipeline.weather_readings"


# ---------------------------------------------------------------------------
# BigQuery client จำลอง
# ---------------------------------------------------------------------------

class _Result(list):
    total_rows = None


class FakeJob:
    def __init__(self, rows=None, num_dml=None, total_rows=None, error=None, output_rows=None):
        self._rows = rows or []
        self._total_rows = total_rows
        self._error = error
        self.num_dml_affected_rows = num_dml
        self.output_rows = output_rows

    def result(self, *args, **kwargs):
        if self._error:
            raise self._error
        res = _Result(self._rows)
        res.total_rows = self._total_rows
        return res


class FakeBQ:
    def __init__(self):
        self.calls = []             # [(ชื่อเมธอด, รายละเอียด...)]
        self.tables = {}            # ref -> schema ของตารางที่ "มีอยู่แล้ว"
        self.last_time = None       # ค่า MAX(reading_time) ที่ตารางจริงจะตอบ
        self.merge_inserted = 3     # จำนวนที่ MERGE รายงานว่าเติมเข้า
        self.anti_inserted = 3      # จำนวนแถวที่ query แบบ anti-join เติมเข้าตารางจริงๆ
        self.table_rows = 4000      # จำนวนแถวทั้งหมดของตารางจริงตอนนี้
        self.load_rows = None       # จำลองโหลดได้ไม่ครบ (None = ครบ)
        self.query_error = None     # error ที่จะเกิดตอน MERGE / anti-join รัน
        self.delete_error = None

    def names(self):
        return [c[0] for c in self.calls]

    def get_table(self, ref):
        self.calls.append(("get_table", ref))
        if ref not in self.tables:
            raise NotFound("ไม่พบตาราง")
        return SimpleNamespace(schema=self.tables[ref])

    def create_table(self, table, exists_ok=False):
        ref = f"{table.project}.{table.dataset_id}.{table.table_id}"
        self.calls.append(("create_table", ref, table.expires, exists_ok))
        self.tables[ref] = table.schema

    def load_table_from_dataframe(self, df, ref, job_config=None):
        self.calls.append(("load", ref, len(df), job_config.write_disposition))
        return FakeJob(output_rows=len(df) if self.load_rows is None else self.load_rows)

    def query(self, sql, job_config=None):
        if "MAX(reading_time)" in sql:
            self.calls.append(("query_max",))
            return FakeJob(rows=[SimpleNamespace(last_time=self.last_time)])
        if "COUNT(*)" in sql:
            self.calls.append(("count",))
            return FakeJob(rows=[SimpleNamespace(n=self.table_rows)])
        self.calls.append(("query", sql, job_config))
        total_rows = None
        if job_config is not None and job_config.destination is not None and self.query_error is None:
            # BigQuery จริง: query ที่เขียนลงตารางปลายทาง รายงาน total_rows = จำนวนแถว "ทั้งตาราง" หลังเขียน
            # (ไม่ใช่จำนวนที่เพิ่งเติม) เคยทำให้ log ขึ้นว่าเติมใหม่ 4020 ทั้งที่ดึงมาแค่ 10 แถว
            self.table_rows += self.anti_inserted
            total_rows = self.table_rows
        return FakeJob(num_dml=self.merge_inserted, total_rows=total_rows, error=self.query_error)

    def delete_table(self, ref, not_found_ok=False):
        self.calls.append(("delete_table", ref))
        if self.delete_error:
            raise self.delete_error


def make_df(n=3):
    t0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
    return pd.DataFrame({
        "location_name": ["Bangkok"] * n,
        "country": ["Thailand"] * n,
        "latitude": [13.7] * n,
        "longitude": [100.5] * n,
        "reading_time": [t0 + timedelta(hours=i) for i in range(n)],
        "temperature_c": [30.0] * n,
        "humidity_percent": [70] * n,
        "wind_speed_kmh": [5.0] * n,
        "fetched_at": [t0 + timedelta(hours=i) for i in range(n)],
    })


@pytest.fixture
def bq():
    return FakeBQ()


# ---------------------------------------------------------------------------
# หน้าตา SQL
# ---------------------------------------------------------------------------

STAGING = "my-proj.weather_pipeline.weather_readings_staging_x"


def test_merge_sql_inserts_only_rows_that_do_not_exist_yet():
    sql = bs.build_merge_sql(TARGET, STAGING)
    assert sql.startswith(f"MERGE `{TARGET}` AS T")
    assert "WHEN NOT MATCHED THEN" in sql
    assert "ON T.location_name = S.location_name AND T.reading_time = S.reading_time" in sql


def test_merge_sql_never_updates_or_deletes_existing_rows():
    sql = bs.build_merge_sql(TARGET, STAGING).upper()
    assert "WHEN MATCHED" not in sql
    assert "UPDATE" not in sql
    assert "DELETE" not in sql


def test_merge_sql_lists_every_column_in_order():
    sql = bs.build_merge_sql(TARGET, STAGING)
    assert f"INSERT ({', '.join(bs.COLUMNS)})" in sql
    assert f"VALUES ({', '.join('S.' + c for c in bs.COLUMNS)})" in sql


def test_sql_deduplicates_staging_by_key_keeping_latest_fetch():
    for sql in (bs.build_merge_sql(TARGET, STAGING), bs.build_anti_join_sql(TARGET, STAGING)):
        assert "ROW_NUMBER() OVER" in sql
        assert "PARTITION BY location_name, reading_time" in sql
        assert "ORDER BY fetched_at DESC" in sql
        assert f"`{STAGING}`" in sql
        assert "QUALIFY" not in sql.upper()  # QUALIFY ของ BigQuery บังคับให้มี WHERE/GROUP BY/HAVING ด้วย


def test_anti_join_sql_is_a_plain_select_not_dml():
    sql = bs.build_anti_join_sql(TARGET, STAGING)
    upper = sql.upper()
    assert upper.lstrip().startswith("SELECT")
    assert "NOT EXISTS" in upper
    assert f"`{TARGET}`" in sql
    for keyword in ("MERGE", "INSERT", "UPDATE", "DELETE"):
        assert keyword not in upper


def test_staging_names_are_unique_and_derived_from_target():
    refs = {bs._staging_ref(TARGET) for _ in range(50)}
    assert len(refs) == 50
    assert all(r.startswith(f"{TARGET}_staging_") for r in refs)
    assert all(r.replace(".", "").replace("-", "").replace("_", "").isalnum() for r in refs)


# ---------------------------------------------------------------------------
# ค่าตั้ง: lookback / strategy
# ---------------------------------------------------------------------------

def test_compute_fetch_start_subtracts_lookback():
    last = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    assert bs.compute_fetch_start(last, 6) == datetime(2026, 9, 1, 6, 0, tzinfo=timezone.utc)
    assert bs.compute_fetch_start(last, 0) == last


def test_compute_fetch_start_is_none_for_first_sync():
    assert bs.compute_fetch_start(None, 6) is None


@pytest.mark.parametrize("raw, expected", [(None, 6), ("", 6), ("  ", 6), ("12", 12), ("0", 0)])
def test_lookback_hours_parsing(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv("BQ_LOOKBACK_HOURS", raising=False)
    else:
        monkeypatch.setenv("BQ_LOOKBACK_HOURS", raw)
    assert bs.get_lookback_hours() == expected


@pytest.mark.parametrize("raw", ["abc", "1.5", "-1"])
def test_invalid_lookback_hours_raises(monkeypatch, raw):
    monkeypatch.setenv("BQ_LOOKBACK_HOURS", raw)
    with pytest.raises(ValueError, match="BQ_LOOKBACK_HOURS"):
        bs.get_lookback_hours()


@pytest.mark.parametrize("raw, expected", [(None, "merge"), ("", "merge"), ("merge", "merge"),
                                           ("ANTI_JOIN", "anti_join"), (" anti_join ", "anti_join")])
def test_write_strategy_parsing(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv("BQ_WRITE_STRATEGY", raising=False)
    else:
        monkeypatch.setenv("BQ_WRITE_STRATEGY", raw)
    assert bs.get_write_strategy() == expected


def test_invalid_write_strategy_raises(monkeypatch):
    monkeypatch.setenv("BQ_WRITE_STRATEGY", "append")
    with pytest.raises(ValueError, match="BQ_WRITE_STRATEGY"):
        bs.get_write_strategy()


# ---------------------------------------------------------------------------
# ensure_target_table
# ---------------------------------------------------------------------------

def test_missing_target_table_is_created_with_our_schema(bq):
    bs.ensure_target_table(bq, TARGET)
    assert bq.tables[TARGET] == bs.SCHEMA
    assert ("create_table", TARGET, None, True) in bq.calls   # exists_ok=True กันชนกันเมื่อรันซ้อน


def test_existing_table_with_matching_schema_is_left_alone(bq):
    bq.tables[TARGET] = bs.SCHEMA
    bs.ensure_target_table(bq, TARGET)
    assert "create_table" not in bq.names()


def test_legacy_type_names_from_the_api_are_accepted(bq):
    """API คืนชื่อ type แบบเก่า (FLOAT/INTEGER) ต้องไม่ถูกมองว่าไม่ตรงกับ FLOAT64/INT64 ที่เราประกาศ"""
    legacy = {"FLOAT64": "FLOAT", "INT64": "INTEGER", "INTEGER": "INTEGER"}
    bq.tables[TARGET] = [bigquery.SchemaField(f.name, legacy.get(f.field_type, f.field_type)) for f in bs.SCHEMA]
    bs.ensure_target_table(bq, TARGET)


def test_extra_columns_in_existing_table_are_allowed(bq):
    bq.tables[TARGET] = bs.SCHEMA + [bigquery.SchemaField("note", "STRING")]
    bs.ensure_target_table(bq, TARGET)


def test_wrong_column_type_is_reported_clearly(bq):
    """กรณีเดียวกับ DECISIONS.md ข้อ 18: country ถูกเดาเป็น INT64"""
    bq.tables[TARGET] = [bigquery.SchemaField(f.name, "INT64" if f.name == "country" else f.field_type)
                         for f in bs.SCHEMA]
    with pytest.raises(bs.SyncError) as err:
        bs.ensure_target_table(bq, TARGET)
    message = str(err.value)
    assert "country" in message and "STRING" in message and "INTEGER" in message


def test_missing_column_is_reported(bq):
    bq.tables[TARGET] = [f for f in bs.SCHEMA if f.name != "humidity_percent"]
    with pytest.raises(bs.SyncError, match="humidity_percent"):
        bs.ensure_target_table(bq, TARGET)


# ---------------------------------------------------------------------------
# sync_to_bigquery: ลำดับของ flow
# ---------------------------------------------------------------------------

def test_merge_flow_order_and_arguments(bq):
    stats = bs.sync_to_bigquery(make_df(3), bq, TARGET, "merge")

    assert bq.names() == ["create_table", "load", "query", "delete_table"]
    create, load, query, delete = bq.calls
    staging_ref = create[1]
    assert staging_ref.startswith(f"{TARGET}_staging_")
    assert load[1] == staging_ref and load[2] == 3
    assert load[3] == bigquery.WriteDisposition.WRITE_APPEND
    assert delete[1] == staging_ref
    assert query[1].startswith("MERGE")
    assert query[2] is None                       # MERGE ไม่ใช้ destination
    assert stats == {"fetched": 3, "staged": 3, "inserted": 3}


def test_staging_table_expires_in_about_a_day_as_a_safety_net(bq):
    bs.sync_to_bigquery(make_df(1), bq, TARGET, "merge")
    expires = bq.calls[0][2]
    remaining = expires - datetime.now(timezone.utc)
    assert timedelta(hours=23) < remaining <= timedelta(hours=bs.STAGING_TTL_HOURS)


def test_real_table_is_never_loaded_to_directly(bq):
    """หัวใจของการเปลี่ยนแปลง: ข้อมูลต้องผ่าน staging เสมอ ห้าม append ลงตารางจริงตรงๆ เหมือนเดิม"""
    bs.sync_to_bigquery(make_df(3), bq, TARGET, "merge")
    bs.sync_to_bigquery(make_df(3), bq, TARGET, "anti_join")
    load_targets = [c[1] for c in bq.calls if c[0] == "load"]
    assert TARGET not in load_targets


def test_anti_join_flow_appends_via_query_job_destination(bq):
    stats = bs.sync_to_bigquery(make_df(5), bq, TARGET, "anti_join")
    query = next(c for c in bq.calls if c[0] == "query")
    config = query[2]
    assert query[1].lstrip().upper().startswith("SELECT")
    assert config.destination.table_id == "weather_readings"
    assert config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
    assert stats["inserted"] == 3                  # = จำนวนแถวตารางหลัง - ก่อน


def test_anti_join_counts_target_rows_before_and_after_the_append(bq):
    bs.sync_to_bigquery(make_df(5), bq, TARGET, "anti_join")
    assert bq.names() == ["create_table", "load", "count", "query", "count", "delete_table"]


def test_anti_join_reports_rows_added_not_the_size_of_the_whole_table(bq):
    """
    บั๊กที่เจอจริงบน BigQuery: ใช้ total_rows ของ query job แล้วได้ขนาดทั้งตาราง (4020) แทนจำนวนที่เติม
    client จำลองนี้ทำตัวเหมือน BigQuery จริง จึงต้องได้จำนวนที่เติม ไม่ใช่ขนาดตาราง
    """
    bq.table_rows = 4010
    bq.anti_inserted = 0
    stats = bs.sync_to_bigquery(make_df(10), bq, TARGET, "anti_join")
    assert stats["inserted"] == 0                  # ไม่ใช่ 4010
    bq.anti_inserted = 7
    stats = bs.sync_to_bigquery(make_df(10), bq, TARGET, "anti_join")
    assert stats["inserted"] == 7                  # ไม่ใช่ 4017


def test_anti_join_count_is_unknown_when_table_shrinks_during_the_run(bq, monkeypatch):
    counts = iter([5000, 4990])
    monkeypatch.setattr(bs, "_count_rows", lambda client, ref: next(counts))
    assert bs.sync_to_bigquery(make_df(3), bq, TARGET, "anti_join")["inserted"] is None


def test_merge_error_leaves_no_staging_behind_and_propagates(bq):
    bq.query_error = RuntimeError("MERGE ล้ม")
    with pytest.raises(RuntimeError, match="MERGE ล้ม"):
        bs.sync_to_bigquery(make_df(2), bq, TARGET, "merge")
    assert bq.names()[-1] == "delete_table"


def test_failure_to_delete_staging_does_not_fail_a_successful_sync(bq, capsys):
    bq.delete_error = RuntimeError("ลบไม่ได้")
    stats = bs.sync_to_bigquery(make_df(2), bq, TARGET, "merge")
    assert stats["inserted"] == 3
    assert "ลบ staging table" in capsys.readouterr().out


def test_incomplete_load_aborts_before_touching_the_real_table(bq):
    bq.load_rows = 1
    with pytest.raises(bs.SyncError, match="ยกเลิกก่อนแตะตารางจริง"):
        bs.sync_to_bigquery(make_df(3), bq, TARGET, "merge")
    assert "query" not in bq.names()               # ยังไม่ได้ MERGE
    assert bq.names()[-1] == "delete_table"        # แต่ staging ถูกลบ


def test_dml_not_allowed_gives_actionable_message_for_merge(bq):
    bq.query_error = Forbidden("Billing has not been enabled. DML queries are not allowed in the free tier.")
    with pytest.raises(bs.SyncError) as err:
        bs.sync_to_bigquery(make_df(2), bq, TARGET, "merge")
    message = str(err.value)
    assert "anti_join" in message and "billing" in message.lower()


def test_unrelated_forbidden_error_is_not_disguised(bq):
    bq.query_error = Forbidden("Access Denied: User does not have permission to query table")
    with pytest.raises(Forbidden):
        bs.sync_to_bigquery(make_df(2), bq, TARGET, "merge")


def test_forbidden_with_anti_join_is_not_converted(bq):
    bq.query_error = Forbidden("DML queries are not allowed in the free tier.")
    with pytest.raises(Forbidden):
        bs.sync_to_bigquery(make_df(2), bq, TARGET, "anti_join")


# ---------------------------------------------------------------------------
# main(): exit code และการต่อ flow
# ---------------------------------------------------------------------------

@pytest.fixture
def app(monkeypatch, bq):
    """ตั้งค่า module ให้พร้อมรัน main() โดยใช้ BigQuery จำลอง และดักค่า since ที่ส่งไปดึงจาก Supabase"""
    state = SimpleNamespace(since="ยังไม่ถูกเรียก", df=make_df(5), bq=bq)

    monkeypatch.setattr(bs, "SUPABASE_DB_URL", "postgresql://fake")
    monkeypatch.setattr(bs, "BQ_PROJECT_ID", "my-proj")
    monkeypatch.setattr(bs, "BQ_DATASET", "weather_pipeline")
    monkeypatch.setattr(bs, "BQ_TABLE", "weather_readings")
    monkeypatch.delenv("BQ_WRITE_STRATEGY", raising=False)
    monkeypatch.delenv("BQ_LOOKBACK_HOURS", raising=False)
    monkeypatch.setattr(bs.bigquery, "Client", lambda project=None, **kw: bq)

    def fake_fetch(since):
        state.since = since
        return state.df

    monkeypatch.setattr(bs, "fetch_new_records_from_supabase", fake_fetch)
    return state


def test_main_success_returns_0_and_reports_inserted_and_skipped(app, capsys):
    app.bq.merge_inserted = 3
    assert bs.main() == 0
    assert "เติมใหม่ 3 record, ข้ามเพราะมีอยู่แล้ว 2 record (จากที่ดึงมา 5)" in capsys.readouterr().out


def test_main_anti_join_summary_uses_rows_added_not_table_size(app, monkeypatch, capsys):
    """ตรงกับ log จริงที่เคยพิมพ์ 'เติมใหม่ 4020 ... ข้าม -4010' เมื่อดึงมา 10 แถวและเติม 0"""
    monkeypatch.setenv("BQ_WRITE_STRATEGY", "anti_join")
    app.df = make_df(10)
    app.bq.table_rows = 4020
    app.bq.anti_inserted = 0
    assert bs.main() == 0
    out = capsys.readouterr().out
    assert "เติมใหม่ 0 record, ข้ามเพราะมีอยู่แล้ว 10 record (จากที่ดึงมา 10)" in out


def test_main_never_prints_a_negative_skipped_count(app, monkeypatch, capsys):
    monkeypatch.setenv("BQ_WRITE_STRATEGY", "anti_join")
    app.df = make_df(5)
    for added in (0, 2, 5):
        app.bq.anti_inserted = added
        bs.main()
    out = capsys.readouterr().out
    assert "ข้ามเพราะมีอยู่แล้ว -" not in out


def test_main_warns_when_count_is_impossible(app, capsys):
    """จำนวนที่เติมมากกว่าจำนวนที่ดึงมาเป็นไปไม่ได้ ต้องเตือน ไม่ใช่พิมพ์เลขแปลกๆ ราวกับปกติ"""
    app.bq.merge_inserted = 99
    assert bs.main() == 0
    out = capsys.readouterr().out
    assert "[WARNING]" in out and "ซึ่งเป็นไปไม่ได้" in out
    assert "ข้ามเพราะมีอยู่แล้ว" not in out


def test_main_reports_gracefully_when_bigquery_does_not_give_a_count(app, capsys):
    app.bq.merge_inserted = None
    assert bs.main() == 0
    assert "ไม่รายงานจำนวน" in capsys.readouterr().out


def test_main_creates_the_target_table_before_reading_the_watermark(app):
    assert bs.main() == 0
    names = app.bq.names()
    assert names.index("get_table") < names.index("query_max")
    assert TARGET in app.bq.tables


def test_main_first_sync_fetches_everything(app):
    app.bq.last_time = None
    bs.main()
    assert app.since is None


def test_main_fetches_from_last_sync_minus_default_lookback(app):
    last = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    app.bq.last_time = last
    bs.main()
    assert app.since == last - timedelta(hours=bs.DEFAULT_LOOKBACK_HOURS)


def test_main_respects_lookback_env(app, monkeypatch):
    last = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    app.bq.last_time = last
    monkeypatch.setenv("BQ_LOOKBACK_HOURS", "24")
    bs.main()
    assert app.since == last - timedelta(hours=24)


def test_main_uses_anti_join_when_configured(app, monkeypatch):
    monkeypatch.setenv("BQ_WRITE_STRATEGY", "anti_join")
    assert bs.main() == 0
    query = next(c for c in app.bq.calls if c[0] == "query")
    assert "MERGE" not in query[1].upper()


def test_main_with_no_new_data_returns_0_and_does_not_create_staging(app, capsys):
    app.df = make_df(0)
    assert bs.main() == 0
    assert "load" not in app.bq.names()
    assert "ไม่มีข้อมูลใหม่" in capsys.readouterr().out


@pytest.mark.parametrize("attr", ["SUPABASE_DB_URL", "BQ_PROJECT_ID"])
def test_main_returns_1_when_required_setting_is_missing(app, monkeypatch, attr):
    monkeypatch.setattr(bs, attr, None)
    assert bs.main() == 1
    assert app.bq.calls == []


@pytest.mark.parametrize("name, value", [("BQ_WRITE_STRATEGY", "bogus"), ("BQ_LOOKBACK_HOURS", "abc")])
def test_main_returns_1_for_invalid_env_settings(app, monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    assert bs.main() == 1
    assert app.bq.calls == []


def test_main_returns_1_when_target_schema_is_wrong(app, capsys):
    app.bq.tables[TARGET] = [bigquery.SchemaField(f.name, "INT64" if f.name == "country" else f.field_type)
                             for f in bs.SCHEMA]
    assert bs.main() == 1
    assert "country" in capsys.readouterr().out
    assert "load" not in app.bq.names()


def test_main_returns_1_with_a_hint_when_dml_is_not_allowed(app, capsys):
    app.bq.query_error = Forbidden("DML queries are not allowed in the free tier.")
    assert bs.main() == 1
    assert "BQ_WRITE_STRATEGY=anti_join" in capsys.readouterr().out


def test_main_does_not_swallow_unexpected_errors(app):
    app.bq.query_error = RuntimeError("บั๊กที่ไม่คาดคิด")
    with pytest.raises(RuntimeError):
        bs.main()
