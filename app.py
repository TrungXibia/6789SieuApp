import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.constants import *
from src.scraper import fetch_xsmb_full, fetch_station_data, fetch_dien_toan, fetch_than_tai
from src.processor import process_matrix, calculate_frequencies, analyze_bet_cham, extract_numbers_from_data

# Set page config
st.set_page_config(page_title="SieuGa Web - Cyber Dark", layout="wide", initial_sidebar_state="expanded")

# --- CUSTOM CYBER DARK CSS ---
st.markdown("""
<style>
    /* Main Background */
    .stApp { background-color: #0f172a; color: #e2e8f0; }
    
    /* Sidebar */
    [data-testid="stSidebar"] { background-color: #1e293b; border-right: 1px solid #334155; }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] { background-color: #1e293b; border-radius: 12px; padding: 6px; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1); }
    .stTabs [data-baseweb="tab"] { color: #94a3b8; font-weight: 600; padding: 10px 20px; transition: all 0.2s; }
    .stTabs [data-baseweb="tab"]:hover { color: #10b981; }
    .stTabs [data-baseweb="tab"][aria-selected="true"] { color: #10b981; border-bottom: 2px solid #10b981; }

    /* Headers */
    h1, h2, h3 { color: #10b981 !important; font-family: 'Inter', sans-serif; }
    
    /* Dataframes */
    .stDataFrame { border: 1px solid #334155; border-radius: 8px; overflow: hidden; }
    
    /* Custom Badge for Matrix */
    .matrix-hit { background-color: #ef4444; color: white; border-radius: 4px; padding: 2px 4px; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR: FILTERS ---
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/3258/3258446.png", width=60)
    st.title("🦅 SieuGa Web")
    
    region = st.selectbox("🌍 Khu vực", ["Miền Bắc", "Miền Nam", "Miền Trung"])
    
    if region != "Miền Bắc":
        day_of_week = st.selectbox("📅 Ngày quay", ["Chủ Nhật", "Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7"])
        source_map = LICH_QUAY_NAM if region == "Miền Nam" else LICH_QUAY_TRUNG
        stations = source_map.get(day_of_week, [])
        station = st.selectbox("🏢 Chọn đài", stations)
    else:
        station = "MB"
        
    st.divider()
    source_type = st.radio("📡 Nguồn dữ liệu", ["Cả 2 (ĐT+TT)", "Điện Toán", "Thần Tài"], horizontal=False)
    
    col1, col2 = st.columns(2)
    with col1:
        num_days = st.number_input("📅 Số ngày", 30, 200, 60)
    with col2:
        offset = st.number_input("⏪ Backtest", 0, 100, 0)

    if st.button("🔄 Tải lại dữ liệu", use_container_width=True):
        st.cache_data.clear()
        st.session_state.data_ready = False

# --- DATA SESSION MANAGEMENT ---
if 'data_ready' not in st.session_state:
    st.session_state.data_ready = False

@st.cache_data(ttl=3600)
def load_all_data(region, station, num_days):
    # Fetch Master Data (ĐT + TT)
    dt = fetch_dien_toan(num_days + 30)
    tt = fetch_than_tai(num_days + 30)
    
    # Merge into unified master_data list
    m_map = {r['date']: {'dt_numbers': r['dt_numbers']} for r in dt}
    for r in tt:
        if r['date'] in m_map: m_map[r['date']]['tt_number'] = r['tt_number']
        else: m_map[r['date']] = {'tt_number': r['tt_number']}
    
    master_data = sorted([{'date': k, **v} for k, v in m_map.items()], 
                        key=lambda x: datetime.strptime(x['date'], "%d/%m/%Y"), reverse=True)
    
    # Fetch Target Data (Station/MB)
    if region == "Miền Bắc":
        target_data = fetch_xsmb_full(num_days + 30)
    else:
        target_data = fetch_station_data(station, num_days + 30)
        
    return master_data, target_data

if not st.session_state.data_ready or 'last_config' not in st.session_state or st.session_state.last_config != (region, station, num_days):
    with st.spinner(f"Đang đồng bộ dữ liệu {station}..."):
        master, target = load_all_data(region, station, num_days)
        st.session_state.master_data = master
        st.session_state.target_data = target
        st.session_state.data_ready = True
        st.session_state.last_config = (region, station, num_days)

# --- MAIN INTERFACE: TABS ---
t_data, t_matrix, t_freq, t_bet = st.tabs(["📋 DỮ LIỆU", "🎯 MATRIX", "📊 TẦN SUẤT", "📈 BỆT CHẠM"])

with t_data:
    st.subheader(f"Kết quả xổ số: {station}")
    if st.session_state.target_data:
        df_target = pd.DataFrame(st.session_state.target_data).head(20)
        st.dataframe(df_target, use_container_width=True)
    else:
        st.warning("Không có dữ liệu cho đài này.")

with t_matrix:
    st.subheader("Bảng đối soát Matrix (N1-N28)")
    pos = st.radio("Vị trí soi:", ["DE", "CD", "BC"], horizontal=True)
    
    # Data slicing for backtest
    working_target = st.session_state.target_data[offset : offset + 40]
    working_master = st.session_state.master_data # Keep full master for source lookup
    
    matrix_res = process_matrix(working_target, working_master, source_type, pos)
    
    # Construct DataFrame for Matrix
    m_data = []
    for r in matrix_res:
        row = [r['date']]
        # Prize value for display
        db_val = r['items'][0]['db'] if r['items'] else ""
        row.append(db_val)
        
        # Hit values for N1-28
        for cell in r['hits']:
            if cell: row.append(", ".join(cell))
            elif cell is None: row.append("")
            else: row.append("")
        m_data.append(row)
        
    m_cols = ["Ngày", "Giải"] + [f"N{i+1}" for i in range(28)]
    df_matrix = pd.DataFrame(m_data, columns=m_cols)
    
    # --- PANDAS STYLER ---
    def style_matrix(df):
        styles = pd.DataFrame('', index=df.index, columns=df.columns)
        
        # 1. Background for Pending rows (Orange)
        for i, r in enumerate(matrix_res):
            if r['pending']:
                styles.iloc[i, :] = 'background-color: #92400e; color: #fef3c7;' # Dark Orange
                
        # 2. Background for Hits (Red)
        for i, r in enumerate(matrix_res):
            for j, hit_val in enumerate(r['hits']):
                if hit_val:
                    styles.iloc[i, j + 2] = 'background-color: #ef4444; color: white; font-weight: bold;'
        
        return styles

    st.dataframe(df_matrix.style.apply(style_matrix, axis=None), use_container_width=True, height=600)

with t_freq:
    st.subheader("📊 Tần suất Chạm & Cặp (Rolling 7)")
    freq_data = calculate_frequencies(st.session_state.master_data[offset:], source_type)
    
    if freq_data:
        # Show Top Levels highlight
        latest = freq_data[0]
        st.info(f"Kỳ gần nhất: {latest['date']}")
        
        c1, c2 = st.columns(2)
        with c1:
            st.write("🔥 **Chạm Hot (Mức 1, 2, 3):**")
            for i, lv in enumerate(latest['digit_levels']):
                st.write(f"Mức {i+1}: `{', '.join(lv)}`")
        with c2:
            st.write("🔥 **Cặp Hot (Mức 1, 2):**")
            for i, lv in enumerate(latest['pair_levels']):
                st.write(f"Mức {i+1}: `{', '.join(lv)}`")
        
        # Detailed rolling dataframe
        df_f = pd.DataFrame(freq_data)
        st.dataframe(df_f, use_container_width=True)
    else:
        st.info("Cần ít nhất 7 ngày dữ liệu để tính tần suất.")

with t_bet:
    st.subheader("📈 Phân tích nhịp bệt & Gợi ý")
    
    # Example Suggestion Logic: Top hot pairs from Matrix + Freq
    # Collect some numbers to analyze
    suggest_nums = []
    # If freq has mức 1
    if freq_data:
        latest = freq_data[0]
        # Generate 2D from top chams
        top_chams = latest['digit_levels'][0] if latest['digit_levels'] else []
        for a in top_chams:
            for b in top_chams:
                suggest_nums.append(a+b)
                
    if suggest_nums:
        ana = analyze_bet_cham(suggest_nums)
        st.success(f"Dàn gợi ý dựa trên Tần suất Hot ({len(suggest_nums)} số)")
        
        l1, l2 = st.columns(2)
        with l1:
            st.write("**Top Chạm:**")
            st.write(" | ".join([f"{k}({v})" for k, v in ana['top_chams']]))
        with l2:
            st.write("**Top Tổng:**")
            st.write(" | ".join([f"{k}({v})" for k, v in ana['top_tongs']]))
            
        st.write("**Mức số:**")
        for m, nums in ana['levels'].items():
            if m > 0: st.code(f"Mức {m}: {', '.join(nums)}")
    else:
        st.info("Chưa đủ dữ liệu phân tích bệt.")

st.divider()
st.caption(f"SieuGa Streamlit v2.0 | {datetime.now().strftime('%d/%m/%Y %H:%M')}")
