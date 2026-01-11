import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.constants import *
from src.scraper import fetch_xsmb_full, fetch_station_data, fetch_dien_toan, fetch_than_tai
from src.processor import process_matrix, calculate_frequencies, analyze_bet_cham, extract_numbers_from_data, join_bc_cd_de

# Set page config
st.set_page_config(page_title="SieuGa Web - Cyber Dark", layout="wide", initial_sidebar_state="expanded")

# --- CUSTOM CYBER DARK CSS ---
st.markdown("""
<style>
    .stApp { background-color: #0f172a; color: #e2e8f0; }
    [data-testid="stSidebar"] { background-color: #1e293b; border-right: 1px solid #334155; }
    .stTabs [data-baseweb="tab-list"] { background-color: #1e293b; border-radius: 12px; padding: 6px; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1); }
    .stTabs [data-baseweb="tab"] { color: #94a3b8; font-weight: 600; padding: 10px 20px; transition: all 0.2s; }
    .stTabs [data-baseweb="tab"]:hover { color: #10b981; }
    .stTabs [data-baseweb="tab"][aria-selected="true"] { color: #10b981; border-bottom: 2px solid #10b981; }
    h1, h2, h3 { color: #10b981 !important; font-family: 'Inter', sans-serif; }
    .stDataFrame { border: 1px solid #334155; border-radius: 8px; overflow: hidden; }
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
    dt = fetch_dien_toan(num_days + 30)
    tt = fetch_than_tai(num_days + 30)
    m_map = {r['date']: {'dt_numbers': r['dt_numbers']} for r in dt}
    for r in tt:
        if r['date'] in m_map: m_map[r['date']]['tt_number'] = r['tt_number']
        else: m_map[r['date']] = {'tt_number': r['tt_number']}
    master_data = sorted([{'date': k, **v} for k, v in m_map.items()], 
                        key=lambda x: datetime.strptime(x['date'], "%d/%m/%Y"), reverse=True)
    if region == "Miền Bắc": target_data = fetch_xsmb_full(num_days + 30)
    else: target_data = fetch_station_data(station, num_days + 30)
    return master_data, target_data

if not st.session_state.data_ready or 'last_config' not in st.session_state or st.session_state.last_config != (region, station, num_days):
    with st.spinner(f"Đang đồng bộ dữ liệu {station}..."):
        master, target = load_all_data(region, station, num_days)
        st.session_state.master_data = master
        st.session_state.target_data = target
        st.session_state.data_ready = True
        st.session_state.last_config = (region, station, num_days)

# --- APP TABS ---
t_data, t_matrix, t_freq, t_tc3, t_tc4, t_multi, t_scan, t_bet = st.tabs([
    "📋 DỮ LIỆU", "🎯 MATRIX", "📊 TẦN SUẤT 1", "� TỔNG & CHẠM 3", "🔢 TỔNG & CHẠM 4", "🌐 ĐA CHIỀU", "🔍 BỘ CHỌN/SCAN", "�📈 BỆT CHẠM"
])

with t_data:
    st.subheader(f"Kết quả xổ số: {station}")
    if st.session_state.target_data:
        st.dataframe(pd.DataFrame(st.session_state.target_data).head(20), use_container_width=True)
    else: st.warning("Không có dữ liệu.")

with t_matrix:
    st.subheader("Bảng đối soát Matrix (N1-N28)")
    pos = st.radio("Vị trí soi:", ["DE", "CD", "BC"], horizontal=True)
    
    # Logic Processing
    results = process_matrix(st.session_state.target_data[offset:], st.session_state.master_data, source_type, pos)
    
    # Matrix Selector Sidebar logic
    st.sidebar.divider()
    st.sidebar.subheader("💎 GHÉP DÀN BC-CD-DE")
    
    # multi-select or list of checkboxes for Join feature
    if 'selected_join' not in st.session_state:
        st.session_state.selected_join = {} # date -> {bc, cd, de, combos}
        
    m_data = []
    for i, r in enumerate(results[:40]):
        row = [r['date'], r['items'][0]['db'] if r['items'] else ""]
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
        styles.iloc[:, 2:] = 'background-color: #1e293b; color: #94a3b8;'
        for i, r in enumerate(results[:40]):
            if r['pending']: styles.iloc[i, :2] = 'background-color: #92400e; color: #fef3c7;'
            for j, hit_val in enumerate(r['hits']):
                col_idx = j + 2
                if hit_val is None: styles.iloc[i, col_idx] = 'background-color: #000000; color: #000000;'
                elif hit_val: styles.iloc[i, col_idx] = 'background-color: #ef4444; color: #ffffff; font-weight: bold;'
        return styles

    st.dataframe(df_matrix.style.apply(style_matrix, axis=None), use_container_width=True, height=500)

    # --- JOIN SECTION ---
    with st.expander("�️ GHÉP DÀN (Mở rộng 3D/4D từ Matrix)", expanded=True):
        st.write("Chọn các ngày và vị trí muốn ghép dàn:")
        
        # Display selection grid
        sel_dates = [r['date'] for r in results[:15]] # limit to 15 latest
        
        join_map = {}
        cols = st.columns([2, 1, 1, 1])
        cols[0].write("**Ngày**")
        cols[1].write("**BC**")
        cols[2].write("**CD**")
        cols[3].write("**DE**")
        
        for r in results[:15]:
            d = r['date']
            col = st.columns([2, 1, 1, 1])
            col[0].write(d)
            sel_bc = col[1].checkbox("BC", key=f"join_bc_{d}")
            sel_cd = col[2].checkbox("CD", key=f"join_cd_{d}")
            sel_de = col[3].checkbox("DE", key=f"join_de_{d}")
            if sel_bc or sel_cd or sel_de:
                join_map[d] = {'has_bc': sel_bc, 'has_cd': sel_cd, 'has_de': sel_de, 'combos': r['combos']}
        
        if st.button("🔥 GHÉP DÀN & TỔNG HỢP", use_container_width=True):
            if not join_map:
                st.error("Vui lòng tích chọn ít nhất 1 vị trí (BC/CD/DE) để ghép.")
            else:
                lvl_data, max_f = join_bc_cd_de(join_map)
                st.session_state.join_results = (lvl_data, max_f)
                
        if 'join_results' in st.session_state:
            lvl_data, max_f = st.session_state.join_results
            st.divider()
            st.subheader("💎 Kết quả ghép dàn theo Mức")
            
            for key, lab in [('4d', "4D (Bốn càng)"), ('3d', "3D (Ba càng)"), ('2d', "2D (Nhị hợp)")]:
                has_any = any(lvl_data[l][key] for l in range(max_f, 0, -1))
                if has_any:
                    st.markdown(f"### {lab}")
                    for l in range(max_f, 0, -1):
                        nums = sorted(list(lvl_data[l][key]))
                        if nums:
                            st.write(f"**Mức {l}** ({len(nums)} số):")
                            st.code(", ".join(nums))

with t_freq:
    st.subheader("📊 Tần suất Rolling 7")
    freq_data = calculate_frequencies(st.session_state.master_data[offset:], source_type)
    if freq_data:
        latest = freq_data[0]
        c1, c2 = st.columns(2)
        with c1:
            st.write("🔥 **Chạm Hot:**")
            for i, lv in enumerate(latest['digit_levels']): st.write(f"Mức {i+1}: `{', '.join(lv)}`")
        with c2:
            st.write("🔥 **Cặp Hot:**")
            for i, lv in enumerate(latest['pair_levels']): st.write(f"Mức {i+1}: `{', '.join(lv)}`")
        st.dataframe(pd.DataFrame(freq_data), use_container_width=True)
    else: st.info("Không đủ dữ liệu.")

with t_tc3:
    st.subheader("📅 Thống kê Tổng & Chạm 3 Càng")
    st.info("Tính năng đang được di trú từ bản Tkinter...")

with t_tc4:
    st.subheader("� Thống kê Tổng & Chạm 4 Càng")
    st.info("Tính năng đang được di trú từ bản Tkinter...")

with t_multi:
    st.subheader("🌐 Phân tích Đa Chiều (Nhiều nguồn)")
    st.info("Phân tích kết hợp từ Điện Toán + Thần Tài + XSMB...")

with t_scan:
    st.subheader("🔍 Bộ chọn & Scan thông minh")
    st.info("Tạo dàn dựa trên Mức overlap của Chạm, Tổng, Trạm...")

with t_bet:
    st.subheader("📈 Phân tích Bệt Chạm")
    st.info("Phân tích nhịp bệt chuyên sâu...")

st.divider()
st.caption(f"SieuGa Streamlit v2.5 | {datetime.now().strftime('%d/%m/%Y %H:%M')}")
