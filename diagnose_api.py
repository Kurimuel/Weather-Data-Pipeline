"""
diagnose_api.py  (เวอร์ชัน 2, ไฟล์วินิจฉัยชั่วคราว ไม่ใช่ส่วนหนึ่งของ pipeline ลบทิ้งได้เมื่อหาสาเหตุเจอแล้ว)

เป้าหมาย: หาว่าทำไมบางคำขอไป Open-Meteo จึง Timeout เมื่อรันบน GitHub Actions ทั้งที่ยิงจากเครื่องเราปกติ
เวอร์ชัน 1 พบว่า: ล้มแบบ ReadTimeout, ล้มเป็นช่วงๆ ติดกัน (15 ถึง ~93 วินาที), curl ที่ยิงตามก็ต่อ TCP ไม่ติด
และ "สองคำขอแรกหลังหยุดพัก 30 วินาที" ล้มบ่อยผิดปกติ (7 จาก 8)

เวอร์ชัน 2 ทดสอบสามสิ่งในรอบเดียว:
  1. ช่วงพักระหว่างรอบ (PAUSE_SCHEDULE) หลายค่า: ถ้าความล้มเหลวที่ต้นรอบขึ้นกับช่วงพัก จะเห็นเป็นรูปแบบ
  2. ที่อยู่ IP: บันทึกคำตอบ DNS ก่อนทุกคำขอ + remote_ip ของ curl + ผลของ dig (มี TTL) ถ้ามี
     เพื่อดูว่าความล้มเหลวสัมพันธ์กับที่อยู่ใดหรือไม่
  3. การฟื้นตัว: เมื่อคำขอล้ม จะลองใหม่ที่ +10, 20, 40, 70, 100 วินาที (timeout ครั้งละ 5 วินาที)
     แล้วบันทึกว่าหายเมื่อไร (จำกัดจำนวนครั้งด้วย MAX_RECOVERY_PROBES เพราะใช้เวลานาน)

ปลอดภัย: ไม่ใช้ secret ไม่เขียน database ไม่แตะ BigQuery ยิงเฉพาะ API สาธารณะ exit code เป็น 0 เสมอ

ตั้งค่าผ่าน environment variable:
  PAUSE_SCHEDULE       ช่วงพักก่อนรอบที่ 2, 3, ... (วินาที คั่นด้วยจุลภาค) ค่าเริ่มต้น "5,120,5,120,30,30" (= 7 รอบ)
                       (ถ้าไม่ตั้ง จะใช้ ROUNDS และ PAUSE_SECONDS แบบเวอร์ชัน 1)
  MAX_RECOVERY_PROBES  จำนวนคำขอที่ล้มซึ่งจะวัดการฟื้นตัวเต็มรูปแบบ ค่าเริ่มต้น 6
"""

import os
import re
import shutil
import socket
import statistics
import subprocess
import time
from collections import Counter
from urllib.parse import urlparse

import requests

from fetch_weather import API_BASE_URL, LOCATIONS

TIMEOUT = 15                         # เท่ากับ fetch_weather.py
IMMEDIATE_CURL_TIMEOUT = 5           # curl ที่ยิงตามทันที (สั้นลงเพื่อประหยัดเวลา ยังเห็นว่าติดที่ขั้นไหน)
RECOVERY_TIMEOUT = 5
RECOVERY_OFFSETS = (10, 20, 40, 70, 100)   # ลองใหม่กี่วินาทีหลังคำขอล้ม (นับจากตอนล้ม)
DEFAULT_PAUSE_SCHEDULE = (5, 120, 5, 120, 30, 30)
DEFAULT_MAX_RECOVERY_PROBES = 6
HOST = urlparse(API_BASE_URL).hostname
CURL_FORMAT = ("remote_ip=%{remote_ip} dns=%{time_namelookup}s connect=%{time_connect}s "
               "tls=%{time_appconnect}s first_byte=%{time_starttransfer}s total=%{time_total}s http=%{http_code}")


def params_for(location: dict) -> dict:
    """พารามิเตอร์เดียวกับ fetch_weather_for_location() ถ้าฝั่ง production เปลี่ยน ต้องแก้ตรงนี้ให้ตรงกัน"""
    return {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "UTC",
    }


def resolve_ips() -> tuple:
    """คำตอบ DNS ปัจจุบันของ api host ตามที่ระบบของ runner แปลงให้ (เรียงลำดับแล้ว)"""
    try:
        infos = socket.getaddrinfo(HOST, 443, type=socket.SOCK_STREAM)
    except OSError as e:
        return (f"error:{type(e).__name__}",)
    return tuple(sorted({info[4][0] for info in infos}))


def dns_snapshot() -> str:
    """ผลของ dig (มี TTL) ถ้ามี ไม่งั้นใช้ getent ถ้าไม่มีทั้งสองอย่างก็บอกว่าไม่มี"""
    for cmd in (["dig", "+noall", "+answer", HOST], ["getent", "ahosts", HOST]):
        if shutil.which(cmd[0]) is None:
            continue
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        except (subprocess.TimeoutExpired, OSError):
            continue
        text = " | ".join(line.strip() for line in result.stdout.strip().splitlines() if line.strip())
        if text:
            return f"{cmd[0]}: {text}"
    return "(ไม่มี dig/getent บนเครื่องนี้ จึงไม่เห็น TTL)"


def try_once(location: dict, timeout: float = TIMEOUT) -> dict:
    """ยิง 1 ครั้งด้วย requests แล้วบันทึกผลและเวลาที่ใช้ (ไม่ retry เพื่อให้เห็นความล้มเหลวดิบ)"""
    start = time.monotonic()
    status = None
    try:
        response = requests.get(API_BASE_URL, params=params_for(location), timeout=timeout)
        status = response.status_code
        outcome = "ok" if response.ok else f"http_{status}"
    except requests.exceptions.ConnectTimeout:
        outcome = "ConnectTimeout"
    except requests.exceptions.ReadTimeout:
        # หมายเหตุ: ใน urllib3 การค้างตอนจับมือ TLS ก็ถูกรายงานเป็น ReadTimeout (ตรวจแล้วกับ requests 2.31.0)
        outcome = "ReadTimeout"
    except requests.exceptions.ConnectionError:
        outcome = "ConnectionError"
    except requests.exceptions.RequestException as e:
        outcome = type(e).__name__
    return {"city": location["name"], "outcome": outcome, "status": status,
            "elapsed": time.monotonic() - start}


def curl_probe(location: dict, timeout: float = IMMEDIATE_CURL_TIMEOUT) -> dict:
    """
    ยิงด้วย curl ไป URL เดียวกัน คืน {"text", "ok", "remote_ip"}
    remote_ip คือที่อยู่ที่ curl พยายามเชื่อมต่อ (แม้ต่อไม่สำเร็จ ก็ควรมีค่า)
    """
    url = requests.Request("GET", API_BASE_URL, params=params_for(location)).prepare().url
    try:
        result = subprocess.run(
            ["curl", "-s", "-o", "/dev/null", "-m", str(timeout), "-w", CURL_FORMAT, url],
            capture_output=True, text=True, timeout=timeout + 10,
        )
    except FileNotFoundError:
        return {"text": "ไม่พบคำสั่ง curl บนเครื่องนี้", "ok": False, "remote_ip": None}
    except subprocess.TimeoutExpired:
        return {"text": "curl ค้างเกินเวลาที่กำหนด", "ok": False, "remote_ip": None}
    text = result.stdout.strip() or f"(ไม่มีผลลัพธ์, exit code {result.returncode})"
    if result.returncode != 0:
        text += f"  [curl exit code {result.returncode}]"
    match = re.search(r"remote_ip=(\S*)", text)
    remote_ip = match.group(1) if match and match.group(1) else None
    return {"text": text, "ok": "http=200" in text, "remote_ip": remote_ip}


def recovery_probe(location: dict, t0: float, offsets=RECOVERY_OFFSETS, timeout=RECOVERY_TIMEOUT) -> dict:
    """
    หลังคำขอล้ม ลองใหม่ที่เวลา t0+offset (วินาทีหลังล้ม) ทีละจุดจนกว่าจะสำเร็จ
    recovered_by = วินาทีหลังล้มที่สังเกตเห็นว่าสำเร็จครั้งแรก (เป็นขอบบน: อาจหายไปก่อนหน้านั้น)
    """
    attempts = []
    for offset in offsets:
        wait = t0 + offset - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        result = try_once(location, timeout=timeout)
        attempts.append((offset, result["outcome"]))
        if result["outcome"] == "ok":
            return {"recovered_by": time.monotonic() - t0, "via": f"requests@+{offset}s", "attempts": attempts}
    return {"recovered_by": None, "via": None, "attempts": attempts}


def run_diagnostics(pauses, max_recovery_probes: int) -> list:
    results = []
    probes_done = 0
    last_ips = None
    rounds = len(pauses) + 1
    for r in range(1, rounds + 1):
        idle = 0 if r == 1 else pauses[r - 2]
        if idle:
            time.sleep(idle)
        print(f"\n=== รอบที่ {r}/{rounds} (พักก่อนรอบนี้ {idle} วินาที, {time.strftime('%H:%M:%S', time.gmtime())} UTC) ===",
              flush=True)
        print(f"    DNS: {dns_snapshot()}", flush=True)
        for idx, location in enumerate(LOCATIONS, 1):
            ips = resolve_ips()
            if ips != last_ips:
                print(f"    [DNS ที่ระบบแปลงได้ตอนนี้: {', '.join(ips)}]", flush=True)
                last_ips = ips
            res = try_once(location)
            res.update(round=r, idx=idx, idle_before=idle, ips=ips, curl=None, recovery=None)
            print(f"  {location['name']:<11} {res['outcome']:<15} {res['elapsed']:6.2f}s", flush=True)
            if res["outcome"] != "ok":
                t0 = time.monotonic()
                res["curl"] = curl_probe(location)
                print(f"      curl ทันที: {res['curl']['text']}", flush=True)
                if res["curl"]["ok"]:
                    res["recovery"] = {"recovered_by": time.monotonic() - t0, "via": "curl", "attempts": []}
                elif probes_done < max_recovery_probes:
                    probes_done += 1
                    res["recovery"] = recovery_probe(location, t0)
                    rec = res["recovery"]
                    msg = (f"หายภายใน {rec['recovered_by']:.0f} วินาทีหลังล้ม ({rec['via']})"
                           if rec["recovered_by"] is not None else f"ยังไม่หายหลัง {RECOVERY_OFFSETS[-1]} วินาที")
                    print(f"      ฟื้นตัว: {msg}", flush=True)
            results.append(res)
            time.sleep(1)   # เหมือน pipeline: หน่วง 1 วินาทีระหว่างเมือง
    return results


def _rate(failed: int, total: int) -> str:
    return f"{failed}/{total} ({(failed / total * 100) if total else 0:.0f}%)"


def summarize(results: list) -> str:
    total = len(results)
    failed = [r for r in results if r["outcome"] != "ok"]
    ok_times = [r["elapsed"] for r in results if r["outcome"] == "ok"]
    lines = ["## สรุปการวินิจฉัย Open-Meteo v2 (จาก runner ของ GitHub)", "",
             f"- คำขอทั้งหมด: {total} | ล้มเหลว: {_rate(len(failed), total)}"]
    if ok_times:
        lines.append(f"- เวลาตอบของคำขอที่สำเร็จ: ค่ากลาง {statistics.median(ok_times):.2f}s | สูงสุด {max(ok_times):.2f}s")
    if not failed:
        return "\n".join(lines + ["", "ไม่มีคำขอใดล้มเหลวในการรันครั้งนี้"])

    lines += ["", "**ชนิดของความล้มเหลว**", ""]
    lines += [f"- {kind}: {n}" for kind, n in Counter(r["outcome"] for r in failed).most_common()]

    # 1) ช่วงพัก vs ตำแหน่งในรอบ
    lines += ["", "**1) ความล้มเหลวที่ต้นรอบ เทียบกับช่วงพักก่อนรอบ** (สองคำขอแรกของรอบ vs คำขอที่เหลือ)", ""]
    first = [r for r in results if r["round"] == 1]
    lines.append(f"- รอบแรก (เพิ่งเริ่มโปรเซส ไม่มีช่วงพัก): สองคำขอแรก "
                 f"{_rate(sum(1 for r in first if r['idx'] <= 2 and r['outcome'] != 'ok'), sum(1 for r in first if r['idx'] <= 2))}")
    for idle in sorted({r["idle_before"] for r in results if r["round"] > 1}):
        grp = [r for r in results if r["round"] > 1 and r["idle_before"] == idle]
        head = [r for r in grp if r["idx"] <= 2]
        rest = [r for r in grp if r["idx"] > 2]
        lines.append(f"- พัก {idle} วินาที: สองคำขอแรก {_rate(sum(1 for r in head if r['outcome'] != 'ok'), len(head))}"
                     f" | คำขอที่เหลือ {_rate(sum(1 for r in rest if r['outcome'] != 'ok'), len(rest))}")

    # 2) ที่อยู่ IP
    lines += ["", "**2) ความล้มเหลวตามชุดที่อยู่ที่ DNS แปลงได้ก่อนคำขอนั้น**", ""]
    for ips in sorted({r["ips"] for r in results}):
        grp = [r for r in results if r["ips"] == ips]
        lines.append(f"- [{', '.join(ips)}]: {_rate(sum(1 for r in grp if r['outcome'] != 'ok'), len(grp))}")
    curl_ips = Counter(r["curl"]["remote_ip"] for r in failed if r["curl"] and not r["curl"]["ok"] and r["curl"]["remote_ip"])
    if curl_ips:
        lines += ["", "ที่อยู่ที่ curl พยายามต่อ ตอนที่ต่อไม่สำเร็จ:", ""]
        lines += [f"- {ip}: {n} ครั้ง" for ip, n in curl_ips.most_common()]

    # 3) การฟื้นตัว
    recs = [r["recovery"] for r in failed if r["recovery"]]
    lines += ["", "**3) การฟื้นตัวหลังคำขอล้ม** (เวลานับจากตอนที่ล้ม เป็นขอบบน)", ""]
    if recs:
        recovered = [x["recovered_by"] for x in recs if x["recovered_by"] is not None]
        lines.append(f"- วัดได้ {len(recs)} ครั้ง: หายแล้ว {len(recovered)} | ไม่หายภายใน {RECOVERY_OFFSETS[-1]} วินาที "
                     f"{len(recs) - len(recovered)}")
        for limit in (10, 20, 40, 70, 100):
            lines.append(f"  - หายภายใน {limit} วินาที: {sum(1 for v in recovered if v <= limit)}/{len(recs)}")
        if recovered:
            lines.append(f"- ค่ากลางของเวลาที่หาย: {statistics.median(recovered):.0f} วินาที | นานสุด {max(recovered):.0f} วินาที")
    else:
        lines.append("- ไม่มีข้อมูล")
    return "\n".join(lines)


def parse_pause_schedule(raw: str) -> tuple:
    values = [v.strip() for v in raw.split(",") if v.strip()]
    pauses = tuple(int(v) for v in values)
    if any(p < 0 for p in pauses):
        raise ValueError("PAUSE_SCHEDULE ต้องไม่ติดลบ")
    return pauses


def main() -> int:
    raw_schedule = os.getenv("PAUSE_SCHEDULE", "").strip()
    if raw_schedule:
        pauses = parse_pause_schedule(raw_schedule)
    elif os.getenv("ROUNDS") or os.getenv("PAUSE_SECONDS"):
        rounds = int(os.getenv("ROUNDS", "3") or "3")
        pauses = (int(os.getenv("PAUSE_SECONDS", "30") or "30"),) * (rounds - 1)
    else:
        pauses = DEFAULT_PAUSE_SCHEDULE
    max_probes = int(os.getenv("MAX_RECOVERY_PROBES", "") or DEFAULT_MAX_RECOVERY_PROBES)

    print(f"ตารางช่วงพักระหว่างรอบ: {list(pauses)} วินาที (รวม {len(pauses) + 1} รอบ) | วัดการฟื้นตัวสูงสุด {max_probes} ครั้ง")
    results = run_diagnostics(pauses, max_probes)
    summary = summarize(results)
    print("\n" + summary)
    summary_file = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_file:
        with open(summary_file, "a", encoding="utf-8") as f:
            f.write(summary + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
