"""
เทสต์ของ fetch_weather.py (ส่วนที่ไม่ต้องใช้ database)

ไม่ยิง API จริง ไม่ต่อ database ไม่รอจริง (time.sleep ถูกแทนที่ทุกเทสต์)
ส่วนที่ต้องใช้ database จริงอยู่ใน tests/test_db_integration.py

รันด้วย:  pytest tests/test_fetch_weather.py -v
"""

import json
from datetime import datetime, timezone

import pytest
import requests

import fetch_weather as fw
from validate_data import validate_weather_record

LOCATION = {"name": "Bangkok", "country": "Thailand", "lat": 13.7563, "lon": 100.5018}


# ---------------------------------------------------------------------------
# เครื่องมือช่วยจำลอง
# ---------------------------------------------------------------------------

def api_payload(time="2026-09-01T00:15", temp=30.0, humidity=70, wind=5.0,
                lat=13.75, lon=100.5) -> dict:
    """หน้าตา response ของ Open-Meteo ที่โค้ดคาดหวัง"""
    return {
        "latitude": lat,
        "longitude": lon,
        "current": {
            "time": time,
            "temperature_2m": temp,
            "relative_humidity_2m": humidity,
            "wind_speed_10m": wind,
        },
    }


def make_response(status=200, body=None, headers=None, raw=None) -> requests.Response:
    """สร้าง requests.Response จริง (ไม่ใช่ mock) เพื่อให้ raise_for_status() ทำงานตามจริง"""
    r = requests.Response()
    r.status_code = status
    r.url = "https://api.example.test/v1/forecast"
    r._content = raw if raw is not None else json.dumps(body if body is not None else {}).encode()
    r.headers.update(headers or {})
    return r


class FakeApi:
    """
    แทน requests.get: คืน/โยนผลตามลำดับที่กำหนด (ผลสุดท้ายจะถูกใช้ซ้ำถ้าเรียกเกิน)
    เก็บประวัติการเรียกไว้ใน .calls เพื่อให้เทสต์ตรวจได้
    """

    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def __call__(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        outcome = self.outcomes[min(len(self.calls) - 1, len(self.outcomes) - 1)]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


@pytest.fixture
def sleeps(monkeypatch):
    """แทน time.sleep: ไม่รอจริง และบันทึกว่าถูกสั่งรอกี่วินาที"""
    recorded = []
    monkeypatch.setattr(fw.time, "sleep", lambda seconds: recorded.append(seconds))
    return recorded


@pytest.fixture
def install_api(monkeypatch):
    def _install(*outcomes):
        api = FakeApi(*outcomes)
        monkeypatch.setattr(fw.requests, "get", api)
        return api
    return _install


# ---------------------------------------------------------------------------
# LOCATIONS: ข้อมูลตั้งต้นต้องสอดคล้องกับ schema
# ---------------------------------------------------------------------------

def test_locations_names_are_unique():
    """locations.name เป็น UNIQUE ใน schema ชื่อซ้ำจะชนกันตอนบันทึก"""
    names = [loc["name"] for loc in fw.LOCATIONS]
    assert len(names) == len(set(names))


def test_locations_have_required_keys_and_valid_coordinates():
    assert len(fw.LOCATIONS) > 0
    for loc in fw.LOCATIONS:
        assert {"name", "country", "lat", "lon"} <= set(loc), loc
        assert -90 <= loc["lat"] <= 90, loc
        assert -180 <= loc["lon"] <= 180, loc


def test_location_names_fit_varchar_100():
    for loc in fw.LOCATIONS:
        assert 0 < len(loc["name"]) <= 100
        assert len(loc["country"]) <= 100


# ---------------------------------------------------------------------------
# parse_weather_data
# ---------------------------------------------------------------------------

def test_parse_maps_all_fields():
    record = fw.parse_weather_data(api_payload(), LOCATION)
    assert record == {
        "location_name": "Bangkok",
        "country": "Thailand",
        "latitude": 13.75,
        "longitude": 100.5,
        "reading_time": datetime(2026, 9, 1, 0, 15, tzinfo=timezone.utc),
        "temperature_c": 30.0,
        "humidity_percent": 70,
        "wind_speed_kmh": 5.0,
    }


def test_parse_makes_reading_time_utc_aware():
    """
    API ส่งเวลามาเป็น string ไม่มี timezone ต้องกำกับ UTC ให้ชัดเจนเสมอ
    (เคยเป็นต้นเหตุบั๊ก timezone เงียบๆ ดู DECISIONS.md ข้อ 8)
    """
    record = fw.parse_weather_data(api_payload(time="2026-08-04T14:15"), LOCATION)
    assert record["reading_time"].tzinfo is not None
    assert record["reading_time"].utcoffset().total_seconds() == 0
    assert record["reading_time"] == datetime(2026, 8, 4, 14, 15, tzinfo=timezone.utc)


@pytest.mark.parametrize("path", [
    ("current",),
    ("latitude",),
    ("longitude",),
    ("current", "time"),
    ("current", "temperature_2m"),
    ("current", "relative_humidity_2m"),
    ("current", "wind_speed_10m"),
])
def test_parse_returns_none_when_field_is_missing(path):
    """API เปลี่ยนโครงสร้าง -> ต้องได้ None (ไม่ใช่ crash) เพื่อให้เมืองนั้นถูกนับเป็น failed"""
    payload = api_payload()
    target = payload
    for key in path[:-1]:
        target = target[key]
    del target[path[-1]]
    assert fw.parse_weather_data(payload, LOCATION) is None


def test_parse_country_is_optional():
    location = {"name": "Nowhere", "lat": 0.0, "lon": 0.0}
    record = fw.parse_weather_data(api_payload(), location)
    assert record["country"] is None


def test_parsed_good_payload_passes_validation():
    record = fw.parse_weather_data(api_payload(time="2026-01-01T00:00"), LOCATION)
    assert validate_weather_record(record) == []


# ---------------------------------------------------------------------------
# _backoff_seconds
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("attempt, expected", [(1, 2), (2, 4), (3, 8), (4, 16)])
def test_backoff_doubles_each_attempt(attempt, expected):
    assert fw._backoff_seconds(attempt) == expected


def test_backoff_uses_retry_after_header():
    # ตั้งใจใช้ status 429 (error) เพราะ requests.Response ที่ error มีค่าความจริงเป็น False
    # ถ้าโค้ดเช็คแบบ `if response:` แทน `is not None` ค่านี้จะถูกมองข้ามเงียบๆ
    response = make_response(429, headers={"Retry-After": "7"})
    assert bool(response) is False
    assert fw._backoff_seconds(1, response) == 7


def test_backoff_retry_after_is_capped():
    response = make_response(429, headers={"Retry-After": "999"})
    assert fw._backoff_seconds(1, response) == fw.MAX_RETRY_AFTER_SECONDS


@pytest.mark.parametrize("header", ["Wed, 21 Oct 2026 07:28:00 GMT", "soon", "-5", ""])
def test_backoff_ignores_unusable_retry_after(header):
    response = make_response(503, headers={"Retry-After": header})
    assert fw._backoff_seconds(2, response) == 4  # ตกกลับไปใช้ exponential


# ---------------------------------------------------------------------------
# fetch_weather_for_location: retry
# ---------------------------------------------------------------------------

def test_fetch_success_on_first_try(install_api, sleeps):
    api = install_api(make_response(200, api_payload()))
    data = fw.fetch_weather_for_location(LOCATION)
    assert data["current"]["temperature_2m"] == 30.0
    assert len(api.calls) == 1
    assert sleeps == []


def test_fetch_sends_expected_request(install_api, sleeps):
    api = install_api(make_response(200, api_payload()))
    fw.fetch_weather_for_location(LOCATION)
    call = api.calls[0]
    assert call["url"] == fw.API_BASE_URL
    assert call["timeout"] == 15
    assert call["params"]["latitude"] == LOCATION["lat"]
    assert call["params"]["longitude"] == LOCATION["lon"]
    # ขอเวลาเป็น UTC เสมอ ถ้าถูกแก้เป็นอย่างอื่น parse_weather_data() ที่ติดป้าย UTC ให้จะผิด
    assert call["params"]["timezone"] == "UTC"
    for field in ("temperature_2m", "relative_humidity_2m", "wind_speed_10m"):
        assert field in call["params"]["current"]


@pytest.mark.parametrize("failure", [
    requests.exceptions.Timeout(),
    requests.exceptions.ConnectionError(),
], ids=["timeout", "connection_error"])
def test_transient_network_error_is_retried(install_api, sleeps, failure):
    api = install_api(failure, make_response(200, api_payload()))
    assert fw.fetch_weather_for_location(LOCATION) is not None
    assert len(api.calls) == 2
    assert sleeps == [2]


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_retryable_http_status_is_retried(install_api, sleeps, status):
    api = install_api(make_response(status), make_response(200, api_payload()))
    assert fw.fetch_weather_for_location(LOCATION) is not None
    assert len(api.calls) == 2
    assert sleeps == [2]


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_client_errors_are_not_retried(install_api, sleeps, status):
    api = install_api(make_response(status))
    assert fw.fetch_weather_for_location(LOCATION) is None
    assert len(api.calls) == 1
    assert sleeps == []


def test_gives_up_after_max_retries_with_exponential_waits(install_api, sleeps):
    api = install_api(requests.exceptions.Timeout())
    assert fw.fetch_weather_for_location(LOCATION) is None
    assert len(api.calls) == fw.MAX_RETRIES + 1
    assert sleeps == [2, 4]


def test_persistent_503_gives_up_and_returns_none(install_api, sleeps):
    api = install_api(make_response(503))
    assert fw.fetch_weather_for_location(LOCATION) is None
    assert len(api.calls) == fw.MAX_RETRIES + 1


def test_429_waits_for_retry_after_value(install_api, sleeps):
    install_api(make_response(429, headers={"Retry-After": "5"}),
                make_response(200, api_payload()))
    assert fw.fetch_weather_for_location(LOCATION) is not None
    assert sleeps == [5]


def test_max_retries_zero_means_single_attempt(install_api, sleeps):
    api = install_api(requests.exceptions.Timeout())
    assert fw.fetch_weather_for_location(LOCATION, max_retries=0) is None
    assert len(api.calls) == 1
    assert sleeps == []


def test_custom_max_retries_is_respected(install_api, sleeps):
    api = install_api(requests.exceptions.Timeout())
    assert fw.fetch_weather_for_location(LOCATION, max_retries=1) is None
    assert len(api.calls) == 2
    assert sleeps == [2]


def test_invalid_json_returns_none_without_retry(install_api, sleeps):
    api = install_api(make_response(200, raw=b"<html>not json</html>"))
    assert fw.fetch_weather_for_location(LOCATION) is None
    assert len(api.calls) == 1


def test_unexpected_requests_error_returns_none(install_api, sleeps):
    api = install_api(requests.exceptions.TooManyRedirects())
    assert fw.fetch_weather_for_location(LOCATION) is None
    assert len(api.calls) == 1


# ---------------------------------------------------------------------------
# main(): exit code และการนับผล (จำลอง database และ API ทั้งหมด)
# ---------------------------------------------------------------------------

NAMES = [loc["name"] for loc in fw.LOCATIONS]
FAILURE_LIMIT = int(len(NAMES) * fw.MAX_FAILURE_RATIO)  # จำนวนเมืองที่ล้มได้โดย job ยังไม่แดง


class FakeConn:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class Pipeline:
    """สภาพแวดล้อมจำลองของ main() ให้แต่ละเทสต์กำหนดว่าเมืองไหนจะล้ม/ถูกปฏิเสธ"""

    def __init__(self):
        self.fail_fetch = set()
        self.reject = set()          # เมืองที่ API คืนค่าผิดปกติ (ไม่ผ่าน validation)
        self.fail_save = set()
        self.db_down = False
        self.drop_connection_on = None   # เมืองที่จะทำให้สายหลุดตอนบันทึก (ครั้งเดียว)
        self.fetch_calls = []
        self.saved = []
        self.connections = []


@pytest.fixture
def pipeline(monkeypatch):
    p = Pipeline()
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://fake")
    monkeypatch.setattr(fw.time, "sleep", lambda seconds: None)

    def fake_open(db_url):
        if p.db_down:
            return None
        conn = FakeConn()
        p.connections.append(conn)
        return conn

    def fake_fetch(location, *args, **kwargs):
        p.fetch_calls.append(location["name"])
        if location["name"] in p.fail_fetch:
            return None
        temp = 999.0 if location["name"] in p.reject else 30.0
        return api_payload(time="2026-01-01T00:15", temp=temp,
                           lat=location["lat"], lon=location["lon"])

    def fake_save(record, conn):
        name = record["location_name"]
        if p.drop_connection_on == name:
            p.drop_connection_on = None
            conn.closed = True
            return False
        if name in p.fail_save:
            return False
        p.saved.append(name)
        return True

    monkeypatch.setattr(fw, "open_connection", fake_open)
    monkeypatch.setattr(fw, "fetch_weather_for_location", fake_fetch)
    monkeypatch.setattr(fw, "save_to_database", fake_save)
    return p


def test_open_connection_disables_auto_prepare(monkeypatch):
    """
    ใช้ connection เดียวกับ 12 เมือง -> psycopg จะ prepare คำสั่ง SQL อัตโนมัติตั้งแต่ครั้งที่ 6
    ซึ่งพังกับ Supabase pooler แบบ transaction mode (พอร์ต 6543) จึงต้องส่ง prepare_threshold=None เสมอ
    """
    seen = {}

    def fake_connect(url, **kwargs):
        seen["url"] = url
        seen["kwargs"] = kwargs
        return FakeConn()

    monkeypatch.setattr(fw.psycopg, "connect", fake_connect)
    assert fw.open_connection("postgresql://fake") is not None
    assert "prepare_threshold" in seen["kwargs"]
    assert seen["kwargs"]["prepare_threshold"] is None


def test_open_connection_returns_none_when_database_is_unreachable(monkeypatch, capsys):
    def boom(url, **kwargs):
        raise fw.psycopg.OperationalError("connection refused")

    monkeypatch.setattr(fw.psycopg, "connect", boom)
    assert fw.open_connection("postgresql://fake") is None
    assert "เชื่อมต่อ database ไม่ได้" in capsys.readouterr().out


def test_main_all_cities_ok_returns_0(pipeline, capsys):
    assert fw.main() == 0
    assert pipeline.saved == NAMES
    assert f"บันทึกสำเร็จ {len(NAMES)}/{len(NAMES)}" in capsys.readouterr().out


def test_main_opens_exactly_one_connection_and_closes_it(pipeline):
    fw.main()
    assert len(pipeline.connections) == 1
    assert pipeline.connections[0].closed is True


@pytest.mark.parametrize("n_failed, expected_code", [
    (0, 0),
    (1, 0),
    (FAILURE_LIMIT, 0),            # ล้มได้ถึงเกณฑ์พอดี ยังไม่แดง
    (FAILURE_LIMIT + 1, 1),        # เกินเกณฑ์ แดง
    (len(NAMES), 1),
])
@pytest.mark.parametrize("kind", ["fetch", "save"])
def test_main_exit_code_follows_failure_threshold(pipeline, n_failed, expected_code, kind):
    failing = set(NAMES[:n_failed])
    if kind == "fetch":
        pipeline.fail_fetch = failing
    else:
        pipeline.fail_save = failing
    assert fw.main() == expected_code


def test_main_keeps_successful_cities_even_when_returning_1(pipeline):
    pipeline.fail_fetch = set(NAMES[:FAILURE_LIMIT + 1])
    assert fw.main() == 1
    assert pipeline.saved == NAMES[FAILURE_LIMIT + 1:]


def test_main_validation_rejection_is_not_counted_as_failure(pipeline, capsys):
    pipeline.reject = {NAMES[0]}
    assert fw.main() == 0
    out = capsys.readouterr().out
    assert NAMES[0] not in pipeline.saved
    assert "ไม่ผ่าน validation 1" in out
    assert "ล้มเหลว 0" in out


def test_main_all_rejected_means_nothing_saved_so_returns_1(pipeline):
    pipeline.reject = set(NAMES)
    assert fw.main() == 1
    assert pipeline.saved == []


def test_main_failures_plus_rejections_with_nothing_saved_returns_1(pipeline):
    pipeline.fail_fetch = set(NAMES[:2])
    pipeline.reject = set(NAMES[2:])
    assert fw.main() == 1


def test_main_returns_1_without_calling_api_when_db_is_down(pipeline):
    pipeline.db_down = True
    assert fw.main() == 1
    assert pipeline.fetch_calls == []


def test_main_returns_1_when_db_url_is_missing(pipeline, monkeypatch):
    monkeypatch.delenv("SUPABASE_DB_URL")
    assert fw.main() == 1
    assert pipeline.fetch_calls == []


def test_main_reconnects_and_retries_save_when_connection_drops(pipeline):
    pipeline.drop_connection_on = NAMES[2]
    assert fw.main() == 0
    assert pipeline.saved == NAMES               # เมืองที่สายหลุดก็ยังถูกบันทึก ไม่มีช่องว่าง
    assert len(pipeline.connections) == 2        # เปิดใหม่ 1 ครั้ง


def test_main_does_not_reconnect_when_save_fails_on_healthy_connection(pipeline):
    pipeline.fail_save = {NAMES[2]}
    assert fw.main() == 0
    assert NAMES[2] not in pipeline.saved
    assert len(pipeline.connections) == 1


def test_main_fetches_every_location_even_after_a_failure(pipeline):
    pipeline.fail_fetch = {NAMES[0]}
    fw.main()
    assert pipeline.fetch_calls == NAMES


def test_main_summary_line_reports_counts(pipeline, capsys):
    pipeline.fail_fetch = {NAMES[0]}
    pipeline.reject = {NAMES[1]}
    fw.main()
    out = capsys.readouterr().out
    saved = len(NAMES) - 2
    assert f"บันทึกสำเร็จ {saved}/{len(NAMES)} | ไม่ผ่าน validation 1 | ล้มเหลว 1" in out
