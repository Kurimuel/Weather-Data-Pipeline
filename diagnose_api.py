"""
diagnose_api.py  (ไฟล์วินิจฉัยชั่วคราว ไม่ใช่ส่วนหนึ่งของ pipeline ลบทิ้งได้เมื่อหาสาเหตุเจอแล้ว)

เป้าหมาย: หาว่าทำไมบางเมืองถึง Timeout เมื่อ fetch_weather.py รันบน GitHub Actions
ทั้งที่ยิงจากเครื่องของเราแล้วตอบใน ~1 วินาที

สิ่งที่ทำ: ยิง Open-Meteo ครบทุกเมืองใน LOCATIONS หลายรอบ ด้วย requests (ตัวเดียวกับที่ pipeline ใช้)
และพารามิเตอร์เดียวกันกับ fetch_weather.py เมื่อมีคำขอล้มจะยิง curl ไปที่ URL เดียวกันทันที
เพื่อเทียบว่าล้มเพราะเครือข่าย/API (curl ล้มด้วย) หรือเพราะฝั่ง Python (curl ผ่าน)

ปลอดภัย: ไม่ใช้ secret ไม่เขียน database ไม่แตะ BigQuery ยิงเฉพาะ API สาธารณะ
Exit code เป็น 0 เสมอ (เป็นการวัด ไม่ใช่การทดสอบผ่าน/ไม่ผ่าน)

ตั้งค่าผ่าน environment variable: ROUNDS (ค่าเริ่มต้น 3), PAUSE_SECONDS (ค่าเริ่มต้น 30 เว้นระหว่างรอบ)
เวลาสูงสุดโดยประมาณถ้าล้มทุกคำขอ: ROUNDS x 12 เมือง x (15 วินาที + curl สูงสุด 15 วินาที + 1 วินาที)
"""

import os
import statistics
import subprocess
import time
from collections import Counter

import requests

from fetch_weather import API_BASE_URL, LOCATIONS

TIMEOUT = 15          # เท่ากับ fetch_weather.py
CURL_TIMEOUT = 15
CURL_FORMAT = ("dns=%{time_namelookup}s connect=%{time_connect}s tls=%{time_appconnect}s "
               "first_byte=%{time_starttransfer}s total=%{time_total}s http=%{http_code}")


def params_for(location: dict) -> dict:
    """พารามิเตอร์เดียวกับ fetch_weather_for_location() ถ้าฝั่ง production เปลี่ยน ต้องแก้ตรงนี้ให้ตรงกัน"""
    return {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "UTC",
    }


def try_once(location: dict) -> dict:
    """ยิง 1 ครั้งด้วย requests แล้วบันทึกผลและเวลาที่ใช้ (ไม่ retry เพื่อให้เห็นความล้มเหลวดิบ)"""
    start = time.perf_counter()
    status = None
    try:
        response = requests.get(API_BASE_URL, params=params_for(location), timeout=TIMEOUT)
        status = response.status_code
        outcome = "ok" if response.ok else f"http_{status}"
    except requests.exceptions.ConnectTimeout:
        outcome = "ConnectTimeout"          # รอเชื่อมต่อ/ทำ TLS ไม่ทัน
    except requests.exceptions.ReadTimeout:
        outcome = "ReadTimeout"             # เชื่อมต่อได้แล้ว แต่รอคำตอบไม่ทัน
    except requests.exceptions.ConnectionError:
        outcome = "ConnectionError"
    except requests.exceptions.RequestException as e:
        outcome = type(e).__name__
    return {"city": location["name"], "outcome": outcome, "status": status,
            "elapsed": time.perf_counter() - start}


def curl_probe(location: dict) -> str:
    """ยิงด้วย curl ไป URL เดียวกัน คืนข้อความแยกเวลาแต่ละช่วง (DNS / เชื่อมต่อ / TLS / ไบต์แรก / รวม)"""
    url = requests.Request("GET", API_BASE_URL, params=params_for(location)).prepare().url
    try:
        result = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-m", str(CURL_TIMEOUT), "-w", CURL_FORMAT, url],
            capture_output=True, text=True, timeout=CURL_TIMEOUT + 10,
        )
    except FileNotFoundError:
        return "ไม่พบคำสั่ง curl บนเครื่องนี้"
    except subprocess.TimeoutExpired:
        return "curl ค้างเกินเวลาที่กำหนด"
    text = result.stdout.strip() or f"(ไม่มีผลลัพธ์, exit code {result.returncode})"
    if result.returncode != 0:
        text += f"  [curl exit code {result.returncode}]"
    return text


def run_diagnostics(rounds: int, pause_seconds: int) -> list[dict]:
    results = []
    for r in range(1, rounds + 1):
        print(f"\n=== รอบที่ {r}/{rounds} ({time.strftime('%H:%M:%S', time.gmtime())} UTC) ===", flush=True)
        for location in LOCATIONS:
            res = try_once(location)
            res["round"] = r
            if res["outcome"] != "ok":
                res["curl"] = curl_probe(location)
            results.append(res)
            print(f"  {location['name']:<11} {res['outcome']:<15} {res['elapsed']:6.2f}s", flush=True)
            if "curl" in res:
                print(f"      curl ทันที: {res['curl']}", flush=True)
            time.sleep(1)   # เหมือน pipeline: หน่วง 1 วินาทีระหว่างเมือง
        if r < rounds:
            time.sleep(pause_seconds)
    return results


def summarize(results: list[dict]) -> str:
    total = len(results)
    failed = [r for r in results if r["outcome"] != "ok"]
    ok_times = [r["elapsed"] for r in results if r["outcome"] == "ok"]
    lines = [f"## สรุปการวินิจฉัย Open-Meteo (จาก runner ของ GitHub)", "",
             f"- คำขอทั้งหมด: {total} | ล้มเหลว: {len(failed)} ({(len(failed) / total * 100) if total else 0:.0f}%)"]
    if ok_times:
        lines.append(f"- เวลาตอบของคำขอที่สำเร็จ: ค่ากลาง {statistics.median(ok_times):.2f}s | สูงสุด {max(ok_times):.2f}s")
    if failed:
        lines += ["", "ชนิดของความล้มเหลว:", ""]
        lines += [f"- {kind}: {n}" for kind, n in Counter(r["outcome"] for r in failed).most_common()]
        lines += ["", "ล้มเหลวตามเมือง:", ""]
        lines += [f"- {city}: {n}" for city, n in Counter(r["city"] for r in failed).most_common()]
        curl_ok = sum(1 for r in failed if "http=200" in r.get("curl", ""))
        lines += ["", f"ในจำนวนคำขอที่ล้มด้วย Python มี {curl_ok} จาก {len(failed)} ครั้งที่ curl ที่ยิงตามทันทีได้ HTTP 200"]
    else:
        lines += ["", "ไม่มีคำขอใดล้มเหลวในการรันครั้งนี้"]
    return "\n".join(lines)


def main() -> int:
    rounds = int(os.getenv("ROUNDS", "3") or "3")
    pause = int(os.getenv("PAUSE_SECONDS", "30") or "30")
    results = run_diagnostics(rounds, pause)
    summary = summarize(results)
    print("\n" + summary)
    summary_file = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a", encoding="utf-8") as f:
            f.write(summary + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
