"""
dashboard.py

แสดงผลข้อมูลอากาศที่เก็บไว้ใน Supabase ผ่าน Streamlit
วิธีรันตอนทดสอบบนเครื่อง: streamlit run dashboard.py
"""

import os
import streamlit as st
import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Weather Data Pipeline", page_icon="🌤️", layout="wide")


@st.cache_data(ttl=300)  # cache ไว้ 5 นาที ไม่ต้อง query database ทุกครั้งที่ผู้ใช้ interact
def load_weather_data() -> pd.DataFrame:
    """ดึงข้อมูลทั้งหมดจาก Supabase มาเป็น DataFrame"""
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        st.error("ไม่พบ SUPABASE_DB_URL — ตั้งค่าใน .env (รันบนเครื่อง) "
                  "หรือ Streamlit Secrets (ตอน deploy)")
        st.stop()

    query = """
        SELECT location_name, reading_time, temperature_c,
               humidity_percent, wind_speed_kmh, fetched_at
        FROM weather_readings
        ORDER BY reading_time ASC;
    """
    with psycopg.connect(db_url) as conn:
        df = pd.read_sql(query, conn)
    return df


st.title("🌤️ Weather Data Pipeline Dashboard")
st.caption("ข้อมูลจาก Open-Meteo API เก็บอัตโนมัติผ่าน GitHub Actions ทุกชั่วโมง")

df = load_weather_data()

if df.empty:
    st.warning("ยังไม่มีข้อมูลในระบบ — รอ pipeline รันสักครู่ หรือรัน fetch_weather.py ด้วยมือก่อน")
    st.stop()

# --------------------------------------------------------------------------
# ตัวกรองเมือง (รองรับหลายเมืองในอนาคตตามที่ออกแบบไว้ใน LOCATIONS list)
# --------------------------------------------------------------------------
locations = sorted(df["location_name"].unique())
selected_location = st.selectbox("เลือกเมือง", locations)

filtered_df = df[df["location_name"] == selected_location]

# --------------------------------------------------------------------------
# ตัวเลขสรุปล่าสุด
# --------------------------------------------------------------------------
latest = filtered_df.iloc[-1]
col1, col2, col3, col4 = st.columns(4)
col1.metric("อุณหภูมิล่าสุด", f"{latest['temperature_c']:.1f} °C")
col2.metric("ความชื้นล่าสุด", f"{latest['humidity_percent']:.0f} %")
col3.metric("ความเร็วลมล่าสุด", f"{latest['wind_speed_kmh']:.1f} km/h")
col4.metric("จำนวน reading ทั้งหมด", len(filtered_df))

st.caption(f"ข้อมูลล่าสุด (UTC): {latest['reading_time']}")

# --------------------------------------------------------------------------
# กราฟย้อนหลัง
# --------------------------------------------------------------------------
st.subheader("อุณหภูมิย้อนหลัง")
st.line_chart(filtered_df.set_index("reading_time")["temperature_c"])

st.subheader("ความชื้นย้อนหลัง")
st.line_chart(filtered_df.set_index("reading_time")["humidity_percent"])

st.subheader("ความเร็วลมย้อนหลัง")
st.line_chart(filtered_df.set_index("reading_time")["wind_speed_kmh"])

# --------------------------------------------------------------------------
# ตารางข้อมูลดิบ (เผื่ออยากดูรายละเอียด)
# --------------------------------------------------------------------------
with st.expander("ดูข้อมูลดิบทั้งหมด"):
    st.dataframe(filtered_df.sort_values("reading_time", ascending=False))
