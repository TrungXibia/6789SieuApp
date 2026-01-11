import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.constants import *
from src.scraper import fetch_xsmb_full, fetch_station_data, fetch_dien_toan, fetch_than_tai
from src.processor import (
    process_matrix, calculate_frequencies, calculate_tc_stats, 
    analyze_bet_cham, extract_numbers_from_data, join_bc_cd_de,
    compute_kybe_cycles, calculate_taixiu_stats, get_kybe_touch_levels,
    get_frequency_matrix, get_bacnho_comb_preds, classify_xito, classify_ngau
)

# Set page config
st.set_page_config(page_title="SieuGa Web - Cyber Dark", layout="wide", initial_sidebar_state="expanded")

# Initialize Session State early
if 'current_domain' not in st.session_state:
    st.session_state.current_domain = API_MIRRORS[0]

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
    else: station = "MB"
        
    st.divider()
    source_type = st.radio("📡 Nguồn dữ liệu", ["Cả 2 (ĐT+TT)", "Điện Toán", "Thần Tài"], horizontal=False)
    
    col1, col2 = st.columns(2)
    with col1: num_days = st.number_input("📅 Số ngày", 30, 200, 60)
    with col2: offset = st.number_input("⏪ Backtest", 0, 100, 0)

    if st.button("🔄 Tải lại dữ liệu", use_container_width=True):
        st.cache_data.clear()
        st.session_state.data_ready = False

# --- DATA SESSION ---
if 'data_ready' not in st.session_state: st.session_state.data_ready = False

@st.cache_data(ttl=3600)
def load_all_data(region, station, num_days):
    dt = fetch_dien_toan(num_days + 30); tt = fetch_than_tai(num_days + 30)
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
        st.session_state.master_data = master; st.session_state.target_data = target
        st.session_state.data_ready = True; st.session_state.last_config = (region, station, num_days)

# --- APP TABS ---
t_data, t_matrix, t_freq, t_tc3, t_tc4, t_bet, t_kybe = st.tabs([
    "📋 DỮ LIỆU", "🎯 MATRIX", "📊 TẦN SUẤT 1", "📅 TỔNG & CHẠM 3", "🔢 TỔNG & CHẠM 4", "📈 BỆT CHẠM", "🧠 KYBE - GROK"
])

with t_data:
    st.subheader(f"Kết quả xổ số: {station}")
    if st.session_state.target_data: st.dataframe(pd.DataFrame(st.session_state.target_data).head(20), use_container_width=True)
    else: st.warning("Không có dữ liệu.")

with t_matrix:
    st.subheader("Bảng đối soát Matrix (N1-N28)")
    pos = st.radio("Vị trí soi:", ["DE", "CD", "BC"], horizontal=True)
    results = process_matrix(st.session_state.target_data[offset:], st.session_state.master_data, source_type, pos)
    
    m_data = []
    for r in results[:40]:
        row = [r['date'], r['raw_src']]
        for cell in r['hits']:
            if cell: row.append(", ".join(cell))
            else: row.append("")
        m_data.append(row)
    df_matrix = pd.DataFrame(m_data, columns=["Ngày", "Nguồn"] + [f"N{i+1}" for i in range(28)])
    
    def style_matrix(df):
        styles = pd.DataFrame('', index=df.index, columns=df.columns)
        # Zone Background Colors (Misses)
        z_miss = ["#154360"]*7 + ["#6e4506"]*7 + ["#511610"]*7 + ["#4b1e52"]*7
        
        for i, r in enumerate(results[:40]):
            # Header columns
            if r['pending']: styles.iloc[i, :2] = 'background-color: #92400e; color: #ffffff; font-weight: bold;'
            else: styles.iloc[i, :2] = 'background-color: #1a1a1a; color: #ffffff;'
            
            for j, hit_val in enumerate(r['hits']):
                col_idx = j + 2
                if hit_val is None: 
                    styles.iloc[i, col_idx] = 'background-color: #000000; color: #000000;'
                elif hit_val: 
                    styles.iloc[i, col_idx] = 'background-color: #ef4444; color: #ffffff; font-weight: bold;'
                else: 
                    styles.iloc[i, col_idx] = f'background-color: {z_miss[j]}; color: #94a3b8;'
        return styles
    st.dataframe(df_matrix.style.apply(style_matrix, axis=None), use_container_width=True, height=500)

    with st.expander("🛠️ GHÉP DÀN (Mở rộng 3D/4D từ Matrix)", expanded=False):
        join_map = {}
        st.write("Chọn ngày & vị trí để ghép dàn:")
        for r in results[:12]:
            d = r['date']; col = st.columns([2, 1, 1, 1])
            col[0].write(d)
            b = col[1].checkbox("BC", key=f"j_bc_{d}"); c = col[2].checkbox("CD", key=f"j_cd_{d}"); de = col[3].checkbox("DE", key=f"j_de_{d}")
            if b or c or de: join_map[d] = {'has_bc':b, 'has_cd':c, 'has_de':de, 'combos':r['combos']}
        if st.button("🔥 GHÉP DÀN", use_container_width=True) and join_map:
            st.session_state.join_res = join_bc_cd_de(join_map)
        if 'join_res' in st.session_state:
            lvl, max_f = st.session_state.join_res
            for k, label in [('4d','4D'),('3d','3D'),('2d','2D')]:
                st.write(f"### {label}")
                for l in range(max_f, 0, -1):
                    nums = sorted(list(lvl[l][k]))
                    if nums: st.write(f"**Mức {l}** ({len(nums)} số):"); st.code(", ".join(nums))

with t_freq:
    st.subheader("📊 Tần suất Rolling 7")
    f_data = calculate_frequencies(st.session_state.master_data[offset:], source_type)
    if f_data:
        lat = f_data[0]; c1, c2 = st.columns(2)
        with c1: 
            st.write("**Chạm Hot:**")
            for i, lv in enumerate(lat['digit_levels']): st.write(f"Mức {i+1}: `{', '.join(lv)}`")
        with c2: 
            st.write("**Cặp Hot:**")
            for i, lv in enumerate(lat['pair_levels']): st.write(f"Mức {i+1}: `{', '.join(lv)}`")
        st.dataframe(pd.DataFrame(f_data), use_container_width=True)
    else: st.info("Không đủ dữ liệu.")

with t_tc3:
    st.subheader("📅 Tổng & Chạm 3 Càng (Hàng Trăm)")
    stats = calculate_tc_stats(st.session_state.target_data[offset:], pos_idx=-3)
    if stats:
        lat = stats[0]; st.write(f"**Gan hiện tại (Kỳ {lat['date']}):**")
        cols = st.columns(10)
        for i in range(10): cols[i].metric(f"Số {i}", lat['cham_gaps'][str(i)])
        df = pd.DataFrame([{'Ngày': r['date'], 'GĐB': r['result'], **{f"G{i}": r['cham_gaps'][str(i)] for i in range(10)}} for r in stats])
        st.dataframe(df.head(20), use_container_width=True)

with t_tc4:
    st.subheader("🔢 Tổng & Chạm 4 Càng (Hàng Nghìn)")
    stats = calculate_tc_stats(st.session_state.target_data[offset:], pos_idx=-4)
    if stats:
        lat = stats[0]; st.write(f"**Gan hiện tại (Kỳ {lat['date']}):**")
        cols = st.columns(10)
        for i in range(10): cols[i].metric(f"Số {i}", lat['cham_gaps'][str(i)])
        df = pd.DataFrame([{'Ngày': r['date'], 'GĐB': r['result'], **{f"G{i}": r['cham_gaps'][str(i)] for i in range(10)}} for r in stats])
        st.dataframe(df.head(20), use_container_width=True)


with t_bet:
    st.subheader("📈 Phân tích Bệt Thẳng & Nhị hợp")
    ana = analyze_bet_cham(st.session_state.target_data[offset:])
    
    c1, c2 = st.columns([1, 2])
    with c1:
        st.write("**Top Chạm gánh:**")
        for k, v in ana['top_chams']:
            st.write(f"- Chạm {k}: {v} lần")
    with c2:
        st.write("**Top Tổng gánh:**")
        cols = st.columns(5)
        for i, (k, v) in enumerate(ana['top_tongs']):
            cols[i].metric(f"Tổng {k}", v)

    st.write("---")
    st.write("### 💎 Dàn Bệt & Nhị hợp mới nhất")
    if ana.get('recent_bets'):
        for bet in ana['recent_bets'][:5]: # Show top 5 recent
            with st.expander(f"🎲 Bệt: {', '.join(bet['bets'])} ({bet['count']} số)"):
                st.code(", ".join(bet['dan']))
    else:
        st.info("Chưa phát hiện tín hiệu Bệt Thẳng trong 30 ngày gần đây.")

with t_kybe:
    st.subheader("🧠 Dashboard Kybe - Grok Advanced")
    
    k_days = st.sidebar.select_slider("Kỳ Kybe:", options=[50, 100, 150, 200], value=100, key="k_period")
    k_data = st.session_state.target_data[offset : offset + k_days]
    
    if k_data:
        # 1. Extract Sequences & Tokens
        seqs = [[] for _ in range(5)]
        dates = []
        for d in k_data:
            val = d.get('xsmb_full') or (str(d['all_prizes'][0]) if 'all_prizes' in d else "")
            if len(val) >= 5:
                digits = [int(c) for c in val[-5:]]
                for p in range(5): seqs[p].append(digits[p])
                dates.append(d['date'])
        
        if seqs[0]:
            L = len(seqs[0])
            xi_toks = [classify_xito([seqs[p][i] for p in range(5)]) for i in range(L)]
            ng_toks = [classify_ngau([seqs[p][i] for p in range(5)]) for i in range(L)]
            sum3_toks = [str((seqs[2][i] + seqs[3][i] + seqs[4][i]) % 10) for i in range(L)]
            sum5_toks = [str(sum(seqs[p][i] for p in range(5)) % 10) for i in range(L)]
            
            c_main, c_side = st.columns([3, 1], gap="small")
            
            with c_main:
                st.write("### 📏 Kybe Grid (40 kỳ gần nhất)")
                # Grid Header
                grid_cols = st.columns([1.8] + [1]*20, gap="small")
                # Row 1-5: Positions
                pos_labels = ["C.Ngàn", "Ngàn", "Trăm", "Chục", "Đơn vị", "Xì Tố", "Ngầu", "Tổng 3", "Tổng 5"]
                
                def render_row(label, data, color="#94a3b8", is_bold=True, p_idx=None):
                    cols = st.columns([1.8] + [1]*20, gap="small")
                    cols[0].write(f"**{label}**")
                    for i in range(min(len(data), 20)):
                        val = data[i]
                        bg = "#1e293b"
                        border_color = "#334155"
                        
                        # Logic "Gan" (Vắng mặt 4 ngày liên tiếp)
                        is_gan = False
                        if i < len(data) - 4 and p_idx is not None and p_idx < 5:
                            found = False
                            for lookback in range(1, 5):
                                try:
                                    # Check across all 5 digit positions in the history
                                    for row_p in range(5):
                                        if seqs[row_p][i+lookback] == val:
                                            found = True; break
                                    if found: break
                                except: pass
                            if not found: is_gan = True
                        
                        if is_gan: 
                            bg = "#854d0e"; border_color = "#facc15" # Burned Orange / Yellow border
                        
                        cols[i+1].markdown(
                            f"<div style='background:{bg}; color:{color}; text-align:center; border:1px solid {border_color}; "
                            f"border-radius:4px; font-weight:{'bold' if is_bold else 'normal'}; font-family:Consolas, monospace; "
                            f"font-size:14px; padding:1px 0;'>{val}</div>", 
                            unsafe_allow_html=True
                        )

                for p in range(5): render_row(pos_labels[p], seqs[p], color="#f8fafc", p_idx=p)
                render_row(pos_labels[5], xi_toks, color="#93c5fd", is_bold=False)
                render_row(pos_labels[6], ng_toks, color="#ec4899")
                render_row(pos_labels[7], sum3_toks, color="#a855f7")
                render_row(pos_labels[8], sum5_toks, color="#f97316")
                
                st.divider()
                st.write("### 🔢 Chu kỳ Bộ 3 & Bộ 4")
                last5 = [seqs[p][0] for p in range(5)]
                import itertools
                combos3 = list(itertools.combinations(last5, 3))
                combos4 = list(itertools.combinations(last5, 4))
                digsets = [set(str(seqs[p][j]) for p in range(5)) for j in range(L)]
                cyc_res = compute_kybe_cycles(digsets, combos3 + combos4)
                
                df_cyc = pd.DataFrame(cyc_res)
                if not df_cyc.empty:
                    df_cyc = df_cyc.head(15)
                    st.table(df_cyc[['tok', 'cyc', 'miss', 'due', 'occ']])
            
            with c_side:
                st.write("### 📈 Phân tích nhanh")
                
                # Tai Xiu Stats
                tx_stats = calculate_taixiu_stats(seqs, dates)
                st.success(f"Tài: {tx_stats['counts'].get('T',0)} | Xỉu: {tx_stats['counts'].get('X',0)}")
                for sig in tx_stats['signals']: st.warning(sig)
                
                st.write("---")
                # Nhị hợp Giao nhau
                nh_stats = get_frequency_matrix(seqs)
                st.write("**Nhị hợp & Giao nhau:**")
                for inter in nh_stats['intersections']:
                    if inter['common']:
                        st.markdown(f"🔹 **{inter['label']}**: Có `{','.join(inter['common'])}`")
                        with st.expander("Xem dàn chung"):
                            st.code(",".join(inter['dan']))

                st.write("---")
                # Bạc nhớ
                bn_rows = [[seqs[p][i] for p in range(5)] for i in range(min(L, 40))]
                p5t = get_bacnho_comb_preds(bn_rows, size=2)
                pht = get_bacnho_comb_preds(bn_rows, size=2) # Adjust for Hậu Tứ if needed
                st.write("**Bạc nhớ 5 Tinh:**")
                st.code(" | ".join(p5t))
                st.write("**Bạc nhớ Hậu Tứ:**")
                st.code(" | ".join(pht))

                st.write("---")
                # Touch Pattern
                st.write("**Touch Pattern:**")
                tp1, tp2 = st.columns(2)
                ng_in = tp1.text_input("Ngầu:", "0,1", key="kybe_ng")
                tg_in = tp2.text_input("Tổng:", "5,6", key="kybe_tg")
                touch_res = get_kybe_touch_levels(set(ng_in.split(",")), set(tg_in.split(",")))
                st.error(f"Mức 2: {','.join(touch_res['muc2'][:10])}...")
                st.warning(f"Mức 1: {','.join(touch_res['muc1'][:10])}...")
                st.success(f"Mức 0: {','.join(touch_res['muc0'][:10])}...")
    else:
        st.info("Không đủ dữ liệu Kybe.")

st.divider()
st.caption(f"SieuGa Streamlit v3.1 (Kybe Edition) | {datetime.now().strftime('%d/%m/%Y %H:%M')}")
