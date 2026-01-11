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
