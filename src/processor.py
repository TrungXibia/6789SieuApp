import pandas as pd
from collections import Counter, defaultdict
from datetime import datetime

def extract_numbers_from_data(row, source_type):
    """
    Extract digits and overlapping pairs from row data based on source selection.
    source_type: "Điện Toán", "Thần Tài", or "Cả 2 (ĐT+TT)"
    """
    if source_type == "Thần Tài":
        num_str = row.get('tt_number', '')
    elif source_type == "Điện Toán":
        num_str = "".join(row.get('dt_numbers', []))
    else: # BOTH
        dt = "".join(row.get('dt_numbers', []))
        tt = row.get('tt_number', '')
        num_str = dt + tt
    
    digits = sorted(list(set([d for d in num_str if d.isdigit()])))
    pairs = sorted(list(set([a+b for a in digits for b in digits]))) if digits else []
    return digits, pairs

def process_matrix(data_source, master_data, source_type, pos_type, max_cols=28):
    """
    Logic for Matrix Hit/Miss calculation with absolute mapping (N1 = Source of newest day).
    Creates a Downward Triangle of data with the black area on the left.
    """
    if not data_source:
        return []

    # Map master_data for quick source lookup by date
    source_map = {}
    for row in master_data:
        d, p = extract_numbers_from_data(row, source_type)
        source_map[row['date']] = set(p)

    # Prepare rows with results (newest first)
    rows_data = []
    for row in data_source:
        actual_items = []
        if 'xsmb_full' in row:
            db = row['xsmb_full']
            actual_items.append({'db': db, 'bc': db[-4:-2], 'cd': db[-3:-1], 'de': db[-2:]})
        elif 'all_prizes' in row:
            prizes = row['all_prizes']
            db = str(prizes[0]) if prizes else ""
            actual_items.append({
                'db': db,
                'bc': db[-4:-2] if len(db) >= 4 else '',
                'cd': db[-3:-1] if len(db) >= 3 else '',
                'de': db[-2:] if len(db) >= 2 else ''
            })
        
        rows_data.append({
            'date': row['date'],
            'items': actual_items,
            'source_combos': source_map.get(row['date'], set())
        })

    matrix_results = []
    for r_idx, r_data in enumerate(rows_data):
        row_hits = []
        for k in range(1, max_cols + 1):
            # Absolute Index: Nk is source of newest_date - (k-1)
            source_idx = k - 1
            
            # Triangle Condition: Only show if Result Date >= Source Date
            # i.e. r_idx <= source_idx (since index 0 is newest)
            if source_idx >= len(rows_data) or r_idx > source_idx:
                row_hits.append(None) # Black Area
                continue
                
            source_combos = rows_data[source_idx]['source_combos']
            hits_in_cell = []
            for it in r_data['items']:
                val = it.get(pos_type.lower())
                if val and val in source_combos:
                    hits_in_cell.append(val)
            row_hits.append(hits_in_cell)

        # Check for "Pending" (Orange color) - If the SOURCE of THIS day hits in the FUTURE
        # Future days are newer than current row (indices < r_idx)
        ever_hits_future = False
        this_source = r_data['source_combos']
        for future_idx in range(r_idx + 1):
            future_row = rows_data[future_idx]
            for it in future_row['items']:
                if it.get(pos_type.lower()) in this_source:
                    ever_hits_future = True
                    break
            if ever_hits_future: break

        matrix_results.append({
            'date': r_data['date'],
            'items': r_data['items'],
            'hits': row_hits,
            'pending': not ever_hits_future and len(this_source) > 0,
            'combos': list(this_source)
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
            d, p = extract_numbers_from_data(row, source_type)
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
    Analyze levels and suggestions for a list of predicted numbers.
    """
    if not results_list:
        return {}
    
    counter = Counter(results_list)
    max_c = max(counter.values()) if counter else 0
    levels = {m: sorted([n for n, c in counter.items() if c == m]) for m in range(max_c, -1, -1)}
    
    # Simple Chạm/Tổng hot stats
    chams = Counter()
    tongs = Counter()
    for n in results_list:
        if len(n) == 2 and n.isdigit():
            chams[n[0]] += 1
            chams[n[1]] += 1
            tongs[str((int(n[0]) + int(n[1])) % 10)] += 1
            
    return {
        'levels': levels,
        'top_chams': sorted(chams.items(), key=lambda x: x[1], reverse=True)[:3],
        'top_tongs': sorted(tongs.items(), key=lambda x: x[1], reverse=True)[:3]
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
            r2, r3, r4 = set(), set()
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
