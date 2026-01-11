import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.constants import *
from src.scraper import fetch_xsmb_full, fetch_station_data, fetch_dien_toan, fetch_than_tai
from src.processor import (
    process_matrix, calculate_frequencies, join_bc_cd_de,
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
t_data, t_matrix, t_freq, t_kybe = st.tabs([
    "📋 DỮ LIỆU", "🎯 MATRIX", "📊 TẦN SUẤT 1", "🧠 KYBE - GROK"
])

with t_data:
    st.subheader(f"Kết quả xổ số: {station}")
    if st.session_state.target_data: st.dataframe(pd.DataFrame(st.session_state.target_data).head(20), use_container_width=True)
    else: st.warning("Không có dữ liệu.")

with t_matrix:
    st.subheader("Bảng đối soát Matrix (N1-N28)")
    pos = st.radio("Vị trí soi:", ["DE", "CD", "BC"], horizontal=True)
    results = process_matrix(st.session_state.target_data[offset:], st.session_state.master_data, source_type, pos)
    
    # --- SMART GHÉP DÀN (TOP SELECTION) ---
    with st.container():
        st.write("### 🔥 GHÉP DÀN THÔNG MINH")
        pending_dates = [r['date'] for r in results if r['pending']]
        all_dates = [r['date'] for r in results[:20]]
        
        # 3 Rows of Selections
        sel_bc = st.multiselect("📅 Chọn ngày cho BC:", all_dates, default=pending_dates[:2] if pending_dates else [], key="ms_bc")
        sel_cd = st.multiselect("📅 Chọn ngày cho CD:", all_dates, default=[], key="ms_cd")
        sel_de = st.multiselect("📅 Chọn ngày cho DE:", all_dates, default=pending_dates[2:4] if len(pending_dates) >=4 else pending_dates[:2], key="ms_de")

        # Prepare join_map for processor: { date: [has_bc, has_cd, has_de, combos] }
        join_map = {}
        for d in sel_bc + sel_cd + sel_de:
            if d not in join_map:
                match = next((r for r in results if r['date'] == d), None)
                if match:
                    join_map[d] = [False, False, False, match['combos']]
            if d in join_map:
                if d in sel_bc: join_map[d][0] = True
                if d in sel_cd: join_map[d][1] = True
                if d in sel_de: join_map[d][2] = True
        
        if join_map:
            # --- NOTES (Always visible) ---
            st.write("📝 **Chi tiết dàn nguồn:**")
            for d, flags in join_map.items():
                pos_list = [p.upper() for i, p in enumerate(['bc','cd','de']) if flags[i]]
                num_str = ", ".join(flags[3])
                if len(num_str) > 120: num_str = num_str[:120] + "..."
                st.markdown(f"✅ **{d} ({'+'.join(pos_list)}):** {num_str}")

            # Call processor with tuple values
            lvl_data, max_sh = join_bc_cd_de({k: tuple(v) for k, v in join_map.items()})
            
            st.write("---")
            # Result Visualization
            res_cols = st.columns(3)
            col_labels = [('4d', '🔥 Dàn 4 Càng (4D)'), ('3d', '⭐ Dàn 3 Càng (3D)'), ('2d', '🍀 Dàn 2 Càng (2D)')]
            
            for idx, (k, label) in enumerate(col_labels):
                with res_cols[idx]:
                    with st.container(border=True):
                        st.subheader(label)
                        # Show levels from max_sh down to 0
                        for l in range(max_sh, -1, -1):
                            nums = sorted(list(lvl_data[l][k]))
                            if nums:
                                st.markdown(f"**Mức {l}** ({len(nums)} số):")
                                if l == 0 and len(nums) > 100:
                                    st.code(", ".join(nums[:50]) + f" ... (+{len(nums)-50} số)")
                                else:
                                    st.code(", ".join(nums))
                            elif l > 0:
                                st.caption(f"Mức {l} (0 số)")
                                st.write("—")
            st.write("---")
    
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
        z_miss = ["#154360"]*7 + ["#6e4506"]*7 + ["#511610"]*7 + ["#4b1e52"]*7
        for i, r in enumerate(results[:40]):
            if r['pending']: styles.iloc[i, :2] = 'background-color: #92400e; color: #ffffff; font-weight: bold;'
            else: styles.iloc[i, :2] = 'background-color: #1a1a1a; color: #ffffff;'
            for j, hit_val in enumerate(r['hits']):
                col_idx = j + 2
                if hit_val is None: styles.iloc[i, col_idx] = 'background-color: #000000; color: #000000;'
                elif hit_val: styles.iloc[i, col_idx] = 'background-color: #ef4444; color: #ffffff; font-weight: bold;'
                else: styles.iloc[i, col_idx] = f'background-color: {z_miss[j]}; color: #94a3b8;'
        return styles
    st.dataframe(df_matrix.style.apply(style_matrix, axis=None), use_container_width=True, height=500)

with t_freq:
    st.subheader("📊 Tần suất Rolling 7")
    f_data = calculate_frequencies(st.session_state.master_data[offset:], source_type)
    if f_data:
        # Helper function for color coding - Softer Palette
        def get_digit_color(count):
            if count == 0: return '#1e293b' # Slate-800 (Dark/Neutral)
            elif count <= 2: return '#334155' # Slate-700 (Slightly lighter)
            elif count <= 4: return '#475569' # Slate-600
            elif count <= 6: return '#ca8a04' # Yellow-600 (Darker Gold)
            else: return '#b91c1c' # Red-700 (Darker Red)
        
        def get_pair_color(count):
            if count == 0: return '#1e293b' # Slate-800
            elif count <= 10: return '#334155' # Slate-700
            elif count <= 20: return '#ca8a04' # Yellow-600
            else: return '#b91c1c' # Red-700
        
        # Table 1: Digit Frequency Matrix (0-9)
        st.write("### 🔢 Tần Suất Chạm (0-9)")
        html = '<style>table.freq-table {width:100%; border-collapse: collapse; font-family: sans-serif;} '
        html += 'table.freq-table th {background-color: #0f172a; color: #94a3b8; border:1px solid #334155; padding:8px; font-size:13px;}'
        html += 'table.freq-table td {border:1px solid #334155; padding:6px; text-align:center; font-size:14px;}</style>'
        html += '<table class="freq-table"><tr><th>STT</th><th>Ngày</th><th>Kết Quả (ĐT/TT)</th>'
        for d in range(10):
            html += f'<th>{d}</th>'
        html += '<th>Top 2 Mức</th></tr>'
        
        for idx, row in enumerate(f_data[:10]):
            html += f'<tr><td>{idx+1}</td><td>{row["date"]}</td><td style="font-size:12px; font-family: monospace;">{row["source"]}</td>'
            for d in range(10):
                count = row['digit_stats'][str(d)]
                color = get_digit_color(count)
                # Text color logic: White for dark backgrounds
                text_color = '#e2e8f0' # Slate-200
                font_weight = 'normal'
                if count > 6: font_weight = 'bold'
                
                html += f'<td style="background-color:{color}; color:{text_color}; font-weight:{font_weight};">{count}</td>'
            top2 = ' | '.join([','.join(lv) for lv in row['digit_levels'][:2]])
            html += f'<td style="font-size:12px;">{top2}</td></tr>'
        html += '</table>'
        st.markdown(html, unsafe_allow_html=True)
        
        st.write("---")
        
        # Table 2: Pair Frequency Classification
        st.write("### 🎯 Tần Suất Cặp (00-99)")
        html2 = '<table class="freq-table"><tr><th>STT</th><th>Ngày</th><th>Kết Quả (ĐT/TT)</th>'
        for i in range(6):
            label = f"Về {i} lần" if i < 5 else "Về 5+ lần"
            # Gradient header matching Tkinter concept roughly but softer
            html2 += f'<th>{label}</th>'
        html2 += '</tr>'
        
        for idx, row in enumerate(f_data[:10]):
            html2 += f'<tr><td>{idx+1}</td><td>{row["date"]}</td><td style="font-size:12px; font-family: monospace;">{row["source"]}</td>'
            pc = row['pair_classification']
            for key in ['ve_0', 've_1', 've_2', 've_3', 've_4', 've_5plus']:
                pairs = pc[key]
                count = len(pairs)
                color = get_pair_color(count)
                
                # Show full list (no truncation)
                pairs_str = ','.join(pairs)
                
                html2 += f'<td style="background-color:{color}; color:#e2e8f0; vertical-align: top;">'
                html2 += f'<div style="font-size:11px; line-height: 1.4; max-height: 80px; overflow-y: auto;">{pairs_str}</div>'
                html2 += f'<div style="font-size:10px; color:#94a3b8; margin-top:4px;">SL: {count}</div></td>'
            html2 += '</tr>'
        html2 += '</table>'
        st.markdown(html2, unsafe_allow_html=True)
    else: st.info("Không đủ dữ liệu.")


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
            
            c_main, c_side = st.columns([7, 3], gap="medium")
            
            with c_main:
                st.write("### 📏 Kybe Grid (40 kỳ gần nhất)")
                
                # HTML Table for perfect symmetry and tight padding
                rows_html = []
                pos_labels = ["C.Ngàn", "Ngàn", "Trăm", "Chục", "Đơn vị", "Xì Tố", "Ngầu", "Tổng 3", "Tổng 5"]
                row_colors = ["#f8fafc", "#f8fafc", "#f8fafc", "#f8fafc", "#f8fafc", "#93c5fd", "#ec4899", "#a855f7", "#f97316"]
                
                table_style = """
                <style>
                    .kybe-table { width: 100%; border-collapse: separate; border-spacing: 2px; font-family: 'Consolas', monospace; table-layout: fixed; }
                    .kybe-table th, .kybe-table td { padding: 1px 2px; text-align: center; border-radius: 3px; font-size: 12px; }
                    .label-cell { text-align: left !important; font-weight: bold; color: #94a3b8; width: 80px; }
                    .data-cell { background: #1e293b; color: #f8fafc; border: 1px solid #334155; }
                    .gan-cell { background: #854d0e !important; color: #facc15 !important; border: 1px solid #facc15 !important; font-weight: bold; }
                    .token-cell { background: #0f172a; border: 1px solid #1e293b; }
                </style>
                """
                
                html = '<table class="kybe-table">'
                for p in range(9):
                    html += f'<tr><td class="label-cell">{pos_labels[p]}</td>'
                    data = []
                    if p < 5: data = seqs[p]
                    elif p == 5: data = xi_toks
                    elif p == 6: data = ng_toks
                    elif p == 7: data = sum3_toks
                    elif p == 8: data = sum5_toks
                    
                    color = row_colors[p]
                    for i in range(min(len(data), 20)):
                        val = data[i]
                        cls = "data-cell"
                        if p >= 5: cls = "token-cell"
                        
                        # Logic Gan
                        if p < 5 and i < len(data) - 4:
                            found = False
                            for lb in range(1, 5):
                                for rp in range(5):
                                    if seqs[rp][i+lb] == val: found = True; break
                                if found: break
                            if not found: cls += " gan-cell"
                            
                        html += f'<td class="{cls}" style="color: {color}">{val}</td>'
                    html += '</tr>'
                html += '</table>'
                
                st.markdown(table_style + html, unsafe_allow_html=True)
                
                # Chu kỳ Bộ 3 & Bộ 4 removed as requested
            
            with c_side:
                st.write("### 📈 Phân tích nhanh")
                
                # Tai Xiu Stats
                tx_stats = calculate_taixiu_stats(seqs, dates)
                st.success(f"Tài: {tx_stats['counts'].get('T',0)} | Xỉu: {tx_stats['counts'].get('X',0)}")
                for sig in tx_stats['signals']: st.warning(sig)
                
                st.write("---")
                # Nhị hợp moved to bottom
                pass

                st.write("---")
                # Bạc nhớ
                bn_rows = [[seqs[p][i] for p in range(5)] for i in range(min(L, 40))]
                p5t = get_bacnho_comb_preds(bn_rows, size=2)
                pht = get_bacnho_comb_preds(bn_rows, size=2)
                st.write("**5 Tinh / Hậu Tứ:**")
                st.code(" | ".join(p5t))
                st.code(" | ".join(pht))

                st.write("---")
                # Touch Pattern
                st.write("**Touch Pattern:**")
                tp1, tp2 = st.columns(2)
                ng_in = tp1.text_input("Ngầu:", "0,1", key="kybe_ng")
                tg_in = tp2.text_input("Tổng:", "5,6", key="kybe_tg")
                touch_res = get_kybe_touch_levels(set(ng_in.split(",")), set(tg_in.split(",")))
                st.error(f"M2: {','.join(touch_res['muc2'][:10])}")
                st.warning(f"M1: {','.join(touch_res['muc1'][:10])}")
                st.success(f"M0: {','.join(touch_res['muc0'][:10])}")

            st.divider()
            # Nhị hợp Giao nhau - Full Width Section
            nh_stats = get_frequency_matrix(seqs)
            st.write("### 🔗 Nhị hợp & Giao nhau")
            
            tops = nh_stats['tops']
            intersections = nh_stats['intersections']
            
            nc1, nc2, nc3, nc4 = st.columns(4)
            
            # Helper to display details
            def display_nh_details(col, title, top_digits, intersection):
                with col:
                    st.info(f"**{title}**")
                    st.write(f"Top: `{','.join(top_digits)}`")
                    if intersection and intersection['common']:
                        st.write(f"Cùng {intersection['label']}: `{','.join(intersection['common'])}`")
                        st.text_area("Dàn:", value=','.join(intersection['dan1']), height=70, disabled=True)
                        st.text_area(f"Dàn {intersection['label']}:", value=','.join(intersection['dan2']), height=70, disabled=True)
                        st.text_area("Dàn chung:", value=','.join(intersection['dan_chung']), height=70, disabled=True)
                    else:
                        st.caption("Không có giao nhau.")

            # Row 1: Hiện tại (vs Lùi 1)
            display_nh_details(nc1, "Hiện tại", tops[0], intersections[0])
            
            # Row 2: Lùi 1 (vs Lùi 2)
            display_nh_details(nc2, "Lùi 1", tops[1], intersections[1])
            
            # Row 3: Lùi 2 (vs Lùi 3)
            display_nh_details(nc3, "Lùi 2", tops[2], intersections[2])
            
            # Row 4: Lùi 3
            with nc4:
                st.info("**Lùi 3**")
                st.write(f"Top: `{','.join(tops[3])}`")
                st.caption("(Hết)")
    else:
        st.info("Không đủ dữ liệu Kybe.")

st.divider()
st.caption(f"SieuGa Streamlit v3.1 (Kybe Edition) | {datetime.now().strftime('%d/%m/%Y %H:%M')}")
