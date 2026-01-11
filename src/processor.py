import pandas as pd
from collections import Counter, defaultdict
from datetime import datetime

def extract_numbers_from_data(row, source_type):
    """
    Extract digits and overlapping pairs from row data based on source selection.
    source_type: "Điện Toán", "Thần Tài", or "Cả 2 (ĐT+TT)"
    Returns: (digits_list, nhị_hợp_pairs, raw_source_string)
    """
    if source_type == "Thần Tài":
        num_str = str(row.get('tt_number', ''))
    elif source_type == "Điện Toán":
        num_str = "".join(map(str, row.get('dt_numbers', [])))
    else: # BOTH
        dt = "".join(map(str, row.get('dt_numbers', [])))
        tt = str(row.get('tt_number', ''))
        num_str = dt + tt
    
    digits = sorted(list(set([d for d in num_str if d.isdigit()])))
    pairs = sorted(list(set([a+b for a in digits for b in digits]))) if digits else []
    return digits, pairs, num_str

def process_matrix(data_source, master_data, source_type, pos_type, max_cols=28):
    """
    Logic for Matrix tracking: Matches Tkinter legacy app.
    - Each row (r_idx) is a Source Date.
    - Column Nk (k=1..28) is the Result Date at offset k days AFTER the source.
    - Mapping: target_idx = r_idx - k.
    - If target_idx < 0: result hasn't happened yet (Top-Left Black Area).
    - Pending (Orange): If the source hasn't appeared in any day newer than itself (idx < r_idx).
    """
    if not data_source:
        return []

    # Map master_data for source lookup
    source_map = {}
    for row in master_data:
        d, p, raw = extract_numbers_from_data(row, source_type)
        source_map[row['date']] = {'combos': set(p), 'raw': raw}

    # Prepare historical results for check
    rows_data = [] # Newest first (idx 0 = newest)
    for row in data_source:
        db = ""
        if 'xsmb_full' in row: db = row['xsmb_full']
        elif 'all_prizes' in row: db = str(row['all_prizes'][0]) if row['all_prizes'] else ""
        
        # Slicing rules (Tkinter matches)
        it = [{'db': db, 'bc': db[-4:-2] if len(db)>=4 else "", 
               'cd': db[-3:-1] if len(db)>=3 else "", 
               'de': db[-2:] if len(db)>=2 else ""}]
        
        src_info = source_map.get(row['date'], {'combos': set(), 'raw': ""})
        rows_data.append({
            'date': row['date'],
            'items': it,
            'source_combos': src_info['combos'],
            'raw_src': src_info['raw']
        })

    matrix_results = []
    for r_idx, r_data in enumerate(rows_data):
        row_hits = []
        combos_set = r_data['source_combos']
        
        for k in range(1, max_cols + 1):
            # Target Result Index = r_idx - k
            # Column N1 is r_idx - 1 (Tomorrow)
            t_idx = r_idx - k
            
            if t_idx < 0:
                row_hits.append(None) # Top-Left Black (Future Result doesn't exist yet)
                continue
                
            if t_idx >= len(rows_data):
                row_hits.append([]) # End of history
                continue
                
            target_res_row = rows_data[t_idx]
            hits = []
            for item in target_res_row['items']:
                val = item.get(pos_type.lower())
                if val and val in combos_set:
                    hits.append(val)
            row_hits.append(hits)

        # Pending logic: If source hasn't appeared in any day NEWER than today (relative future)
        has_hit_future = False
        for fut_idx in range(r_idx):
            fut_row = rows_data[fut_idx]
            for it in fut_row['items']:
                if it.get(pos_type.lower()) in combos_set:
                    has_hit_future = True; break
            if has_hit_future: break

        matrix_results.append({
            'date': r_data['date'],
            'raw_src': r_data['raw_src'],
            'items': r_data['items'],
            'hits': row_hits,
            'pending': not has_hit_future and len(combos_set) > 0,
            'combos': list(combos_set)
        })
    return matrix_results

def calculate_frequencies(data_source, source_type="Cả 2 (ĐT+TT)", window_size=7):
    """
    Calculate statistics over a sliding window for every day.
    """
    if not data_source:
        return []
    
    results = []
    for i in range(len(data_source)):
        window = data_source[i : i + window_size]
        if len(window) < window_size:
            break
            
        head_row = window[0]
        total_digits = []
        total_pairs = []
        for row in window:
            d, p, _ = extract_numbers_from_data(row, source_type)
            total_digits.extend(d)
            total_pairs.extend(p)
            
        d_counts = Counter(total_digits)
        p_counts = Counter(total_pairs)
        
        # Rank by levels
        def get_levels(counts_dict, max_levels=3):
            # Group keys by counts
            c_to_k = defaultdict(list)
            for k, v in counts_dict.items():
                if v > 0: c_to_k[v].append(k)
            sorted_c = sorted(c_to_k.keys(), reverse=True)
            return [sorted(c_to_k[c]) for c in sorted_c[:max_levels]]

        results.append({
            'date': head_row['date'],
            'digit_stats': {str(d): d_counts.get(str(d), 0) for d in range(10)},
            'digit_levels': get_levels({str(d): d_counts.get(str(d), 0) for d in range(10)}),
            'pair_levels': get_levels({f"{p:02d}": p_counts.get(f"{p:02d}", 0) for p in range(100)}, max_levels=2)
        })
    return results

def analyze_bet_cham(results_list, n_digits=2):
    """
    Ported from SieuGaApp_Tkinter: Bệt Thẳng & Nhị hợp logic.
    - Finds 'Bệt Thẳng' by comparing GĐB_i with GĐB_{i-1} position by position (0-5).
    - Generates 'Nhị hợp' sets combining Bệt digits with all digits from both days.
    """
    if not results_list or len(results_list) < 2:
        return {'levels': {}, 'top_chams': [], 'top_tongs': [], 'recent_bets': []}

    # Normalize results (Newest first)
    raw_vals = []
    for item in results_list:
        if isinstance(item, dict):
            val = item.get('xsmb_full') or item.get('db') or (str(item['all_prizes'][0]) if 'all_prizes' in item else "")
            raw_vals.append(val)
        else:
            raw_vals.append(str(item))

    # Reverse to chronological order for analysis
    chrono_vals = list(reversed(raw_vals))
    recent_bets = []
    
    # Loop from 1 to latest (Newest is last in chronological list)
    # We analyze up to the last 30 days of bệt history
    start_idx = max(1, len(chrono_vals) - 30)
    for i in range(start_idx, len(chrono_vals)):
        curr = chrono_vals[i]
        prev = chrono_vals[i-1]
        if len(curr) < 4 or len(prev) < 4: continue

        # Pad to 6 digits for alignment (MN/MT/MB mixed)
        c_digits = list(curr)
        while len(c_digits) < 6: c_digits.insert(0, "")
        p_digits = list(prev)
        while len(p_digits) < 6: p_digits.insert(0, "")

        # Find Bệt Thẳng: Same digit at the same position
        bets = []
        for j in range(6):
            if c_digits[j] != "" and c_digits[j] == p_digits[j]:
                bets.append(c_digits[j])

        if bets:
            # Nhị hợp logic: Get all unique digits from both GĐBs
            rep_digits = set(d for d in curr if d.isdigit())
            rep_digits.update(d for d in prev if d.isdigit())
            
            dan_list = []
            for b_digit in bets:
                for d in rep_digits:
                    # Create pairs with the bệt digit
                    p1, p2 = f"{b_digit}{d}", f"{d}{b_digit}"
                    if p1 not in dan_list: dan_list.append(p1)
                    if p2 not in dan_list: dan_list.append(p2)
            
            recent_bets.append({
                'date_idx': i,
                'bets': bets,
                'dan': sorted(dan_list),
                'count': len(dan_list)
            })

    # Basic stats for UI compatibility (Freq of DE)
    chams = Counter()
    tongs = Counter()
    for n in raw_vals:
        de = n[-2:]
        if len(de) == 2 and de.isdigit():
            chams[de[0]] += 1; chams[de[1]] += 1
            tongs[str((int(de[0]) + int(de[1])) % 10)] += 1
            
    return {
        'levels': {}, # Deprecated for this context
        'top_chams': sorted(chams.items(), key=lambda x: x[1], reverse=True)[:5],
        'top_tongs': sorted(tongs.items(), key=lambda x: x[1], reverse=True)[:5],
        'recent_bets': recent_bets[::-1] # Newest first for web display
    }

def join_bc_cd_de(selected_map):
    """
    selected_map: { date: {'has_bc': bool, 'has_cd': bool, 'has_de': bool, 'combos': list} }
    """
    total_2d = Counter()
    total_3d = Counter()
    total_4d = Counter()
    
    for date, info in selected_map.items():
        combos = info['combos']
        if info['has_bc']:
            r3, r4 = set(), set()
            for bc in combos:
                for d in "0123456789": r3.add(bc + d)
                for i in range(100): r4.add(bc + f"{i:02d}")
            total_3d.update(r3); total_4d.update(r4)
        
        if info['has_cd']:
            r3, r4 = set(), set()
            for cd in combos:
                for d in "0123456789": 
                    r3.add(cd + d)
                    r3.add(d + cd)
                for i in range(100): 
                    r4.add(f"{i//10:01d}" + cd + f"{i%10:01d}")
            total_3d.update(r3); total_4d.update(r4)
            
        if info['has_de']:
            r2, r3, r4 = set(), set(), set()
            for de in combos:
                r2.add(de)
                for d in "0123456789": r3.add(d + de)
                for i in range(100): r4.add(f"{i:02d}" + de)
            total_2d.update(r2); total_3d.update(r3); total_4d.update(r4)
            
    lvl_data = defaultdict(lambda: {'2d': set(), '3d': set(), '4d': set()})
    all_freqs = {0}
    for n, f in total_2d.items(): lvl_data[f]['2d'].add(n); all_freqs.add(f)
    for n, f in total_3d.items(): lvl_data[f]['3d'].add(n); all_freqs.add(f)
    for n, f in total_4d.items(): lvl_data[f]['4d'].add(n); all_freqs.add(f)
    
    return lvl_data, max(all_freqs)
def calculate_tc_stats(data_source, pos_idx=-2):
    """
    Calculate Cham/Tong gaps for a specific position (e.g. -2 for DE, -3 for Hundreds, -4 for Thousands).
    """
    if not data_source: return []
    # Use newest first, but we need to scan chronologically to find gaps
    ordered = list(reversed(data_source))
    cham_gan = {str(i): 0 for i in range(10)}
    tong_gan = {str(i): 0 for i in range(10)}
    results = []
    
    for row in ordered:
        db = row.get('xsmb_full') or (str(row['all_prizes'][0]) if 'all_prizes' in row else "")
        if not db: continue
        
        # Digit at position
        digit = db[pos_idx] if len(db) >= abs(pos_idx) else None
        # Tong (last 2 digits)
        t_val = str((int(db[-2]) + int(db[-1])) % 10) if len(db) >= 2 else None
        
        for d in range(10):
            sd = str(d)
            if sd == digit: cham_gan[sd] = 0
            else: cham_gan[sd] += 1
            if sd == t_val: tong_gan[sd] = 0
            else: tong_gan[sd] += 1
            
        results.append({
            'date': row['date'],
            'result': db,
            'cham_gaps': cham_gan.copy(),
            'tong_gaps': tong_gan.copy()
        })
    return list(reversed(results))

def classify_xito(digits):
    """Classify a 5-digit hand into poker-like categories."""
    if not digits or len(digits) < 5: return "—"
    c = Counter(digits)
    counts = sorted(c.values(), reverse=True)
    if counts[0] == 5: return "Năm Quý"
    if counts[0] == 4: return "Tứ Quý"
    if counts[0] == 3 and counts[1] == 2: return "Cù Lũ"
    if counts[0] == 3: return "Sám Cô"
    if counts[0] == 2 and counts[1] == 2: return "Thú"
    if counts[0] == 2: return "Đôi"
    
    # Check straight
    sorted_d = sorted(list(set(digits)))
    if len(sorted_d) == 5 and (sorted_d[-1] - sorted_d[0] == 4): return "Sảnh"
    return "Rác"

def classify_ngau(digits):
    """Classify Ngau pattern (0-9 or K)."""
    if not digits or len(digits) < 5: return "—"
    s = sum(digits) % 10
    return "K" if s == 0 else str(s)

def calculate_taixiu_stats(seqs, dates):
    """
    Calculate Tai/Xiu and Rong/Ho stats from 5-digit sequences.
    Also detects entry signals.
    """
    L = len(seqs[0])
    rh_tokens, tx_tokens = [], []
    counts = Counter()
    signals = []
    
    for i in range(L):
        c0, c4 = seqs[0][i], seqs[4][i]
        rh = "R" if c0 > c4 else ("H" if c0 < c4 else "=")
        total5 = sum(seqs[p][i] for p in range(5))
        tx = "T" if total5 >= 23 else "X"
        rh_tokens.append(rh)
        tx_tokens.append(tx)
        counts[tx] += 1
    
    # Detect signals (RT -> X, HX -> T)
    for i in range(min(L, 10)):
        if rh_tokens[i] == "R" and tx_tokens[i] == "T":
            signals.append(f"Tín hiệu Rồng & Tài -> Vào X (Kỳ {dates[i]})")
            break
    for i in range(min(L, 10)):
        if rh_tokens[i] == "H" and tx_tokens[i] == "X":
            signals.append(f"Tín hiệu Hổ & Xỉu -> Vào T (Kỳ {dates[i]})")
            break
            
    return {
        'rh_tokens': rh_tokens,
        'tx_tokens': tx_tokens,
        'counts': dict(counts),
        'signals': signals,
        'total': L
    }

def get_frequency_matrix(seqs, top_n=5):
    """
    Nhị hợp & Giao nhau: Frequency calculation for 4 offsets.
    """
    L = len(seqs[0])
    import itertools
    
    def get_cnt(offset):
        if offset + 40 > L: return Counter()
        try:
            # Current 5 digits as source
            last5 = [seqs[p][offset] for p in range(5)]
            combos = list(itertools.combinations(last5, 3)) + list(itertools.combinations(last5, 4))
            # Check hits in history relative to these combos
            digsets = [set(seqs[p][j] for p in range(5)) for j in range(offset, offset + 40)]
            cnt = Counter()
            for tup in combos:
                # Find occurrences of combo in slice
                idxs = [j for j, S in enumerate(digsets) if all(d in S for d in tup)]
                for j in idxs:
                    if j > 0: # Check the day AFTER the match (which is index j-1 since newest=0)
                        for p in range(5): cnt[str(seqs[p][offset + j - 1])] += 1
            return cnt
        except: return Counter()

    c_mats = [get_cnt(i) for i in range(4)] # Cur, P1, P2, P3
    taps = []
    for c in c_mats:
        taps.append([d for d, _ in sorted(c.items(), key=lambda x: (-x[1], x[0]))[:top_n]])
    
    # Find Intersections (Giao nhau)
    intersections = []
    import itertools as it_tools
    for i in range(1, 4):
        common = [d for d in taps[0] if d in taps[i]]
        dan_chung = []
        if common:
            # Create pairs for taps[0] (Cur)
            pairs0 = list(it_tools.permutations(taps[0], 2))
            dan0 = set("".join(map(str, p)) for p in pairs0 if p[0] in common or p[1] in common)
            
            # Create pairs for taps[i] (Lùi i)
            pairs_i = list(it_tools.permutations(taps[i], 2))
            dan_i = set("".join(map(str, p)) for p in pairs_i if p[0] in common or p[1] in common)
            
            # Intersection of the two sets of pairs
            dan_chung = sorted(list(dan0 & dan_i))
            
        intersections.append({'label': f"Lùi {i}", 'common': common, 'dan': dan_chung})
        
    return {'mats': taps, 'intersections': intersections}

def get_bacnho_comb_preds(bn_rows, size=2, n_results=3):
    """
    Bạc nhớ tổ hợp (5 Tinh / Hậu Tứ).
    bn_rows: List of lists (recent 5-digit results).
    """
    if not bn_rows or len(bn_rows) < 2: return []
    import itertools
    latest = bn_rows[0]
    # Use only specific positions if it's Hậu Tứ (3,4) or 5 Tinh (all)
    # This simplified version uses top combos
    current_set = set(latest)
    combs = list(itertools.combinations(sorted(list(current_set)), size))
    
    future_counts = Counter()
    for c in combs:
        c_set = set(c)
        for j in range(1, len(bn_rows)):
            h_digits = set(bn_rows[j])
            if c_set.issubset(h_digits):
                # What appeared in the NEXT period (j-1)
                next_res = bn_rows[j-1]
                for next_c in itertools.combinations(sorted(next_res), 2):
                    future_counts["".join(map(str, next_c))] += 1
                    
    top = sorted(future_counts.items(), key=lambda x: (-x[1], x[0]))[:n_results]
    return [x[0] for x in top]

def compute_kybe_cycles(working_data, combos):
    """
    Compute cycles, gaps, and due for sets of numbers.
    - working_data: List of sets, each set containing digits found in GĐB.
    - combos: List of tuples (e.g., 3-digit or 4-digit combos).
    """
    L_total = len(working_data)
    out = []
    for tup in combos:
        # idxs: indices (newest is 0) where tup matches S
        idxs = [i for i, S in enumerate(working_data) if all(d in S for d in tup)]
        if not idxs: continue
        idxs.sort()
        
        # Chronological index = (L_total - 1 - i)
        chrono_idxs = sorted([L_total - 1 - i for i in idxs])
        gaps = [chrono_idxs[i + 1] - chrono_idxs[i] for i in range(len(chrono_idxs) - 1)]
        
        cyc, support = (None, 0)
        if gaps:
            c = Counter(gaps)
            cyc, support = max(c.items(), key=lambda kv: (kv[1], -kv[0]))
        
        last_idx = idxs[0] # Newest occurrence index
        miss = last_idx
        due = None if not cyc else (cyc - (miss % cyc)) % cyc
        gan_max = max(gaps) if gaps else miss
        
        out.append({
            "tok": "".join(map(str, sorted(tup))),
            "cyc": cyc, "support": support, "miss": miss,
            "due": due, "gan_max": gan_max, "occ": len(idxs),
            "last_idx": last_idx
        })
    return sorted(out, key=lambda x: (x['due'] if x['due'] is not None else 999, -x['occ']))

def get_kybe_touch_levels(ngau_digits, tong_digits, mode="Chạm Tổng"):
    """
    Logic for Mức 0, 1, 2 based on selected Chạm and Tổng digits.
    """
    def get_set(digits):
        if not digits: return set()
        res = set()
        for i in range(100):
            s = f"{i:02d}"
            n1, n2 = int(s[0]), int(s[1])
            match_cham = str(n1) in digits or str(n2) in digits
            match_tong = str((n1 + n2) % 10) in digits
            if mode == "Chạm":
                if match_cham: res.add(s)
            elif mode == "Tổng":
                if match_tong: res.add(s)
            else: # Chạm Tổng
                if match_cham or match_tong: res.add(s)
        return res

    set_a = get_set(ngau_digits)
    set_b = get_set(tong_digits)
    
    muc2, muc1, muc0 = [], [], []
    for i in range(100):
        s = f"{i:02d}"
        in_a = s in set_a
        in_b = s in set_b
        if in_a and in_b: muc2.append(s)
        elif in_a or in_b: muc1.append(s)
        else: muc0.append(s)
        
    return {
        'set_a': sorted(list(set_a)),
        'set_b': sorted(list(set_b)),
        'muc2': sorted(muc2),
        'muc1': sorted(muc1),
        'muc0': sorted(muc0)
    }
