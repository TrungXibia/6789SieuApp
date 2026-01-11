"""
SIÊU GÀ APP - PHÂN TÍCH XỔ SỐ
Ứng dụng Tkinter chạy trên máy tính
Gộp tất cả chức năng trong 1 file
Version 2.0 - Có BẢNG THEO DÕI MATRIX
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import requests
import urllib3
from bs4 import BeautifulSoup
import logging
from typing import List, Dict, Tuple
import json
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import threading
import concurrent.futures
import itertools
from itertools import combinations, product
import pandas as pd
from io import StringIO

# ====================================
# PHẦN 1: DATA FETCHER
# ====================================

logging.basicConfig(level=logging.INFO)
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# API URLs cho các đài
DAI_API = {
    "An Giang": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=angi",
    "Bạc Liêu": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=bali",
    "Bến Tre": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=betr",
    "Bình Dương": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=bidu",
    "Bình Thuận": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=bith",
    "Bình Phước": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=biph",
    "Cà Mau": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=cama",
    "Cần Thơ": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=cath",
    "Đà Lạt": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=dalat",
    "Đồng Nai": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=dona",
    "Đồng Tháp": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=doth",
    "Hậu Giang": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=hagi",
    "Kiên Giang": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=kigi",
    "Long An": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=loan",
    "Sóc Trăng": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=sotr",
    "Tây Ninh": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=tani",
    "Tiền Giang": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=tigi",
    "TP. Hồ Chí Minh": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=tphc",
    "Trà Vinh": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=trvi",
    "Vĩnh Long": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=vilo",
    "Vũng Tàu": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=vuta",
    "Đà Nẵng": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=dana",
    "Bình Định": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=bidi",
    "Đắk Lắk": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=dalak",
    "Đắk Nông": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=dano",
    "Gia Lai": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=gila",
    "Khánh Hòa": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=khho",
    "Kon Tum": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=kotu",
    "Ninh Thuận": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=nith",
    "Phú Yên": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=phye",
    "Quảng Bình": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=qubi",
    "Quảng Nam": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=quna",
    "Quảng Ngãi": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=qung",
    "Quảng Trị": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=qutr",
    "Thừa Thiên Huế": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=thth"
}

# --- Local API Mirror Logic ---
API_MIRRORS = ["www.kqxs88.live", "www.kqxs88.net", "www.kqxs88.top", "www.kqxs88.vip", "www.kqxs88.info"]
CURRENT_DOMAIN = API_MIRRORS[0]

def rotate_api_domain():
    global CURRENT_DOMAIN
    try:
        idx = API_MIRRORS.index(CURRENT_DOMAIN)
        CURRENT_DOMAIN = API_MIRRORS[(idx + 1) % len(API_MIRRORS)]
        logging.info(f"Local App switched API mirror to: {CURRENT_DOMAIN}")
    except: pass

def get_mirrored_url(url):
    global CURRENT_DOMAIN
    for dom in API_MIRRORS:
        if dom in url:
            return url.replace(dom, CURRENT_DOMAIN)
    return url

LICH_QUAY_NAM = {
    "Chủ Nhật": ["Tiền Giang", "Kiên Giang", "Đà Lạt"],
    "Thứ 2": ["Đồng Tháp", "TP. Hồ Chí Minh", "Cà Mau"],
    "Thứ 3": ["Bến Tre", "Vũng Tàu", "Bạc Liêu"],
    "Thứ 4": ["Đồng Nai", "Cần Thơ", "Sóc Trăng"],
    "Thứ 5": ["Tây Ninh", "An Giang", "Bình Thuận"],
    "Thứ 6": ["Trà Vinh", "Vĩnh Long", "Bình Dương"],
    "Thứ 7": ["Long An", "Bình Phước", "Hậu Giang", "TP. Hồ Chí Minh"]
}

LICH_QUAY_TRUNG = {
    "Chủ Nhật": ["Khánh Hòa", "Kon Tum"],
    "Thứ 2": ["Thừa Thiên Huế", "Phú Yên"],
    "Thứ 3": ["Đắk Lắk", "Quảng Nam"],
    "Thứ 4": ["Khánh Hòa", "Đà Nẵng"],
    "Thứ 5": ["Quảng Trị", "Bình Định", "Quảng Bình"],
    "Thứ 6": ["Gia Lai", "Ninh Thuận"],
    "Thứ 7": ["Quảng Ngãi", "Đà Nẵng", "Đắk Nông"]
}

def get_stations_by_day(region: str, day: str) -> List[str]:
    if day == "Tất cả":
        stations = []
        source = LICH_QUAY_NAM if region == "Miền Nam" else LICH_QUAY_TRUNG
        for s_list in source.values():
            stations.extend(s_list)
        return sorted(list(set(stations))) # Trả về danh sách đài duy nhất
    
    if region == "Miền Nam":
        return LICH_QUAY_NAM.get(day, [])
    elif region == "Miền Trung":
        return LICH_QUAY_TRUNG.get(day, [])
    return []


def fetch_dien_toan(total_days: int) -> List[Dict]:
    """Fetch Điện Toán lottery data."""
    def fetch_url(url):
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException:
            return None
    
    soup = fetch_url(f"https://ketqua04.net/so-ket-qua-dien-toan-123/{total_days}")
    data = []
    
    if not soup:
        return data
    
    try:
        divs = soup.find_all("div", class_="result_div", id="result_123")
        for div in divs[:total_days]:
            ds = div.find("span", id="result_date")
            date_raw = ds.text.strip() if ds else ""
            
            if not date_raw:
                continue
            
            if "ngày" in date_raw:
                date_raw = date_raw.split("ngày")[-1].strip()
            date = date_raw.replace("-", "/")
            
            tbl = div.find("table", id="result_tab_123")
            if tbl:
                row = tbl.find("tbody").find("tr")
                cells = row.find_all("td") if row else []
                if len(cells) == 3:
                    nums = [c.text.strip() for c in cells]
                    if all(n.isdigit() for n in nums):
                        data.append({"date": date, "dt_numbers": nums})
    except (AttributeError, TypeError, ValueError) as e:
        logging.warning(f"Error parsing Điện Toán data: {e}")
    
    return data

def fetch_than_tai(total_days: int) -> List[Dict]:
    """Fetch Thần Tài lottery data."""
    def fetch_url(url):
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.RequestException:
            return None
    
    soup = fetch_url(f"https://ketqua04.net/so-ket-qua-than-tai/{total_days}")
    data = []
    
    if not soup:
        return data
    
    try:
        divs = soup.find_all("div", class_="result_div", id="result_tt4")
        for div in divs[:total_days]:
            ds = div.find("span", id="result_date")
            date_raw = ds.text.strip() if ds else ""
            
            if not date_raw:
                continue
            
            if "ngày" in date_raw:
                date_raw = date_raw.split("ngày")[-1].strip()
            date = date_raw.replace("-", "/")
            
            tbl = div.find("table", id="result_tab_tt4")
            if tbl:
                cell = tbl.find("td", id="rs_0_0")
                num = cell.text.strip() if cell else ""
                if num.isdigit() and len(num) == 4:
                    data.append({"date": date, "tt_number": num})
    except (AttributeError, TypeError, ValueError) as e:
        logging.warning(f"Error parsing Thần Tài data: {e}")
    
    return data

def fetch_xsmb(total_days: int) -> Tuple[List[str], List[str], List[List[str]], List[List[str]]]:
    """Fetch MB data from API with local mirror fallback."""
    base_url = f"https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum={total_days}&gameCode=miba"
    for _ in range(len(API_MIRRORS)):
        url = get_mirrored_url(base_url)
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            data = r.json()
            if not data.get("success"):
                rotate_api_domain()
                continue
            
            issue_list = data.get("t", {}).get("issueList", [])
            db_n, g1_n, g7_n, g6_n = [], [], [], []
            for issue in issue_list:
                try:
                    prizes = json.loads(issue.get("detail", ""))
                    db_n.append(prizes[0] if len(prizes) > 0 else "")
                    g1_n.append(prizes[1] if len(prizes) > 1 else "")
                    g6_n.append(prizes[6].split(",") if len(prizes) > 6 else [])
                    g7_n.append(prizes[7].split(",") if len(prizes) > 7 else [])
                except: continue
            return db_n, g1_n, g7_n, g6_n
        except requests.exceptions.RequestException:
            rotate_api_domain()
            continue
    return [], [], [], []

def fetch_xsmb_full(total_days: int) -> Tuple[List[str], List[List[str]]]:
    """Fetch XSMB data with local mirror fallback."""
    base_url = f"https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum={total_days}&gameCode=miba"
    for _ in range(len(API_MIRRORS)):
        url = get_mirrored_url(base_url)
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json()
            if not data.get("success"):
                rotate_api_domain()
                continue
            
            issue_list = data.get("t", {}).get("issueList", [])
            dates = []
            all_prizes = []
            for issue in issue_list[:total_days]:
                detail_str = issue.get("detail", "")
                turn_num = issue.get("turnNum", "")
                if not detail_str or not turn_num: continue
                try:
                    prizes = expand_mb_prizes(detail_str)
                    dates.append(turn_num)
                    all_prizes.append(prizes)
                except: continue
            return dates, all_prizes
        except requests.exceptions.RequestException:
            rotate_api_domain()
            continue
    return [], []

def expand_mb_prizes(detail_json_str):
    try:
        d = json.loads(detail_json_str)
        if not isinstance(d, list): return [""] * 27
        o = []
        # DB, G1
        o.append(str(d[0]) if len(d) > 0 else "")
        o.append(str(d[1]) if len(d) > 1 else "")
        # G2
        if len(d) > 2:
            g2 = d[2].split(",") if isinstance(d[2], str) else d[2]
            for x in g2: o.append(x.strip())
        while len(o) < 4: o.append("")
        # G3
        if len(d) > 3:
            g3 = d[3].split(",") if isinstance(d[3], str) else d[3]
            for x in g3: o.append(x.strip())
        while len(o) < 10: o.append("")
        # G4
        if len(d) > 4:
            g4 = d[4].split(",") if isinstance(d[4], str) else d[4]
            for x in g4: o.append(x.strip())
        while len(o) < 14: o.append("")
        # G5
        if len(d) > 5:
            g5 = d[5].split(",") if isinstance(d[5], str) else d[5]
            for x in g5: o.append(x.strip())
        while len(o) < 20: o.append("")
        # G6
        if len(d) > 6:
            g6 = d[6].split(",") if isinstance(d[6], str) else d[6]
            for x in g6: o.append(x.strip())
        while len(o) < 23: o.append("")
        # G7
        if len(d) > 7:
            g7 = d[7].split(",") if isinstance(d[7], str) else d[7]
            for x in g7: o.append(x.strip())
        while len(o) < 27: o.append("")
        return o[:27]
    except: return [""] * 27

def expand_mnmt_prizes(detail_json_str):
    try:
        d = json.loads(detail_json_str)
        if not isinstance(d, list): return [""] * 18
        # [DB,G1,G2,G3,G4,G5,G6,G7,G8]
        def as_list(v):
            if isinstance(v, list): return v
            if isinstance(v, str): return [x.strip() for x in v.split(",") if x.strip()]
            return []
        
        db = str(d[0]) if len(d) > 0 else ""
        g1 = str(d[1]) if len(d) > 1 else ""
        g2 = str(d[2]) if len(d) > 2 else ""
        g3 = as_list(d[3]) if len(d) > 3 else []
        g4 = as_list(d[4]) if len(d) > 4 else []
        g5 = str(d[5]) if len(d) > 5 else ""
        g6 = as_list(d[6]) if len(d) > 6 else []
        g7 = str(d[7]) if len(d) > 7 else ""
        g8 = str(d[8]) if len(d) > 8 else ""
        
        o = []
        o.append(g8)
        o.append(g7)
        for i in range(3): o.append(g6[i] if i < len(g6) else "")
        o.append(g5)
        for i in range(7): o.append(g4[i] if i < len(g4) else "")
        o.append(g3[0] if len(g3) > 0 else "")
        o.append(g3[1] if len(g3) > 1 else "")
        o.append(g2)
        o.append(g1)
        o.append(db)
        return o[:18]
    except: return [""] * 18

def flatten_prizes(detail_json_str: str) -> List[str]:
    # Deprecated/Generic fallback
    return expand_mb_prizes(detail_json_str)

def fetch_station_data(station_name: str, total_days: int = 60) -> List[Dict]:
    """Fetch MN/MT data from API with local mirror fallback."""
    url_template = DAI_API.get(station_name)
    if not url_template: return []
    base_url = url_template.replace("limitNum=60", f"limitNum={total_days}")
    
    for _ in range(len(API_MIRRORS)):
        url = get_mirrored_url(base_url)
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            data = r.json()
            if not data.get("success"):
                rotate_api_domain()
                continue
            
            issue_list = data.get("t", {}).get("issueList", [])
            results = []
            for issue in issue_list[:total_days]:
                try:
                    detail_str = issue.get("detail", "")
                    prizes = json.loads(detail_str)
                    g6_str = prizes[6] if len(prizes) > 6 else ""
                    g6_list = [num.strip() for num in g6_str.split(",")] if g6_str else []
                    g7 = prizes[7] if len(prizes) > 7 else ""
                    g8 = prizes[8] if len(prizes) > 8 else ""
                    
                    result = {
                        "date": issue.get("turnNum", ""),
                        "db": prizes[0] if len(prizes) > 0 else "",
                        "g1": prizes[1] if len(prizes) > 1 else "",
                        "g2": prizes[2] if len(prizes) > 2 else "",
                        "g3": prizes[3] if len(prizes) > 3 else "",
                        "g4": prizes[4] if len(prizes) > 4 else "",
                        "g5": prizes[5] if len(prizes) > 5 else "",
                        "g6": prizes[6] if len(prizes) > 6 else "",
                        "g7": prizes[7] if len(prizes) > 7 else "",
                        "g8": prizes[8] if len(prizes) > 8 else "",
                        "db_2so": prizes[0][-2:] if prizes[0] else "",
                        "g1_2so": prizes[1][-2:] if prizes[1] else "",
                        "g8_2so": prizes[8][-2:] if prizes[8] else "",
                        "g6_2so": [num[-2:] for num in g6_list if num],
                        "g6_3so": [num[-3:] for num in g6_list if len(num) >= 3],
                        "g7_2so": g7[-2:] if g7 else "",
                        "g7_3so": g7[-3:] if len(g7) >= 3 else "",
                        "all_prizes": expand_mnmt_prizes(detail_str)
                    }
                    results.append(result)
                except: continue
            
            # Ensure sorted by date (newest first)
            try:
                results.sort(key=lambda x: datetime.strptime(x['date'], "%d/%m/%Y"), reverse=True)
            except (ValueError, KeyError):
                pass 
                
            return results
        except requests.exceptions.RequestException:
            rotate_api_domain()
            continue
    return []

# ====================================
# FETCH ALL STATIONS DATA (MN/MT)
# ====================================

def fetch_all_stations_data(region: str, day: str, total_days: int = 60) -> List[Dict]:
    """
    Fetch lottery data from stations of a specific day in a region for continuous day coverage.
    
    Args:
        region: "Miền Nam" or "Miền Trung"
        day: Day of week (e.g., "Thứ 2", "Chủ Nhật")
        total_days: Number of days to fetch
        
    Returns:
        List of dicts sorted by date (newest first), each date having one 3D number
    """
    # Lấy các đài quay trong ngày được chọn
    stations_for_day = get_stations_by_day(region, day)
    
    if not stations_for_day:
        logging.warning(f"No stations found for {region} - {day}")
        return []
    
    logging.info(f"Fetching data from {len(stations_for_day)} stations for {region} - {day}: {stations_for_day}")
    
    # Fetch dữ liệu từ các đài trong ngày đó
    all_data = []
    for station in stations_for_day:
        station_data = fetch_station_data(station, total_days)
        for item in station_data:
            item['station'] = station  # Thêm tên đài
        all_data.extend(station_data)
    
    # Gom theo ngày - mỗi ngày chứa LIST các kết quả
    date_map = {}
    for item in all_data:
        date_str = item['date']
        if date_str not in date_map:
            date_map[date_str] = {
                'date': date_str,
                'items': []
            }
        date_map[date_str]['items'].append(item)
    
    # Sắp xếp theo ngày (newest first)
    def date_sort_key(item):
        try:
            return datetime.strptime(item['date'], "%d/%m/%Y")
        except ValueError:
            return datetime.min
            
    sorted_days = sorted(date_map.values(), key=date_sort_key, reverse=True)
    
    logging.info(f"Merged {len(sorted_days)} unique days with multiple stations from {len(stations_for_day)} stations")
    
    return sorted_days[:total_days]

# ====================================
# HELPER FUNCTIONS: TỔNG & CHẠM 3 CÀNG
# ====================================

def get_3cang_by_tong():
    """Tạo dict phân loại 3 càng theo tổng (0-9)"""
    tong_dict = {i: [] for i in range(10)}
    for i in range(1000):
        num_str = f"{i:03d}"
        tong = sum(int(d) for d in num_str) % 10
        tong_dict[tong].append(num_str)
    return tong_dict

def get_3cang_by_cham():
    """Tạo dict phân loại 3 càng theo số chạm (0-9)"""
    cham_dict = {i: set() for i in range(10)}
    for i in range(1000):
        num_str = f"{i:03d}"
        for digit in num_str:
            cham_dict[int(digit)].add(num_str)
    # Convert to sorted lists
    for k in cham_dict:
        cham_dict[k] = sorted(cham_dict[k])
    return cham_dict

# Pre-compute
TONG_3CANG = get_3cang_by_tong()
CHAM_3CANG = get_3cang_by_cham()

# --- SPECIAL 3D KÉP PATTERNS ---
def _generate_3d_kep_patterns():
    aab, aba, baa = [], [], []
    k_bang_3d = [f"{i}{i}{i}" for i in range(10)]
    for a in range(10):
        for b in range(10):
            if a == b: continue
            aab.append(f"{a}{a}{b}")
            aba.append(f"{a}{b}{a}")
            baa.append(f"{b}{a}{a}")
    
    kep_th = sorted(list(set(aab + aba + baa + k_bang_3d)))
    
    adj_bang, adj_lech, adj_am = set(), set(), set()
    k_bang = ["00","11","22","33","44","55","66","77","88","99"]
    k_lech = ["05","50","16","61","27","72","38","83","49","94"]
    k_am = ["07","70","14","41","29","92","36","63","58","85"]
    
    for i in range(1000):
        s = f"{i:03}"
        ab, bc = s[:2], s[1:]
        if ab in k_bang or bc in k_bang: adj_bang.add(s)
        if ab in k_lech or bc in k_lech: adj_lech.add(s)
        if ab in k_am or bc in k_am: adj_am.add(s)
        
    adj_th = sorted(list(adj_bang | adj_lech | adj_am))
    return {
        "AAB": aab, "ABA": aba, "BAA": baa, "KEP_TH": kep_th,
        "L_BANG": sorted(list(adj_bang)), "L_LECH": sorted(list(adj_lech)), 
        "L_AM": sorted(list(adj_am)), "L_TH": adj_th
    }

KEP_PATTERNS_3D = _generate_3d_kep_patterns()
# Add inverse set: Remaining 3D numbers not in any special pattern
_all_kep_3d = set()
for _v in KEP_PATTERNS_3D.values(): _all_kep_3d.update(_v)
KEP_PATTERNS_3D["CON_LAI"] = [f"{_i:03d}" for _i in range(1000) if f"{_i:03d}" not in _all_kep_3d]

def get_4cang_by_tong():
    """Tạo dict phân loại 4 càng theo tổng (0-9)"""
    tong_dict = {i: [] for i in range(10)}
    for i in range(10000):
        num_str = f"{i:04d}"
        tong = sum(int(d) for d in num_str) % 10
        tong_dict[tong].append(num_str)
    return tong_dict

def get_4cang_by_cham():
    """Tạo dict phân loại 4 càng theo số chạm (0-9)"""
    cham_dict = {i: set() for i in range(10)}
    for i in range(10000):
        num_str = f"{i:04d}"
        for digit in num_str:
            cham_dict[int(digit)].add(num_str)
    # Convert to sorted lists
    for k in cham_dict:
        cham_dict[k] = sorted(cham_dict[k])
    return cham_dict

TONG_4CANG = get_4cang_by_tong()
CHAM_4CANG = get_4cang_by_cham()

# ====================================
# HELPER FUNCTIONS: TỔNG & CHẠM 2 CÀNG
# ====================================

def get_2cang_by_tong():
    """Tạo dict phân loại 2 càng theo tổng (0-9)"""
    tong_dict = {i: [] for i in range(10)}
    for i in range(100):
        num_str = f"{i:02d}"
        tong = sum(int(d) for d in num_str) % 10
        tong_dict[tong].append(num_str)
    return tong_dict

def get_2cang_by_cham():
    """Tạo dict phân loại 2 càng theo số chạm (0-9)"""
    cham_dict = {i: set() for i in range(10)}
    for i in range(100):
        num_str = f"{i:02d}"
        for digit in num_str:
            cham_dict[int(digit)].add(num_str)
    for k in cham_dict:
        cham_dict[k] = sorted(cham_dict[k])
    return cham_dict

TONG_2CANG = get_2cang_by_tong()
CHAM_2CANG = get_2cang_by_cham()

# BỘ SỐ
BO_DICT = {
    "00": ["00","55","05","50"], 
    "11": ["11","66","16","61"], 
    "22": ["22","77","27","72"], 
    "33": ["33","88","38","83"],
    "44": ["44","99","49","94"], 
    "01": ["01","10","06","60","51","15","56","65"], 
    "02": ["02","20","07","70","25","52","57","75"],
    "03": ["03","30","08","80","35","53","58","85"], 
    "04": ["04","40","09","90","45","54","59","95"], 
    "12": ["12","21","17","71","26","62","67","76"],
    "13": ["13","31","18","81","36","63","68","86"], 
    "14": ["14","41","19","91","46","64","69","96"], 
    "23": ["23","32","28","82","73","37","78","87"],
    "24": ["24","42","29","92","74","47","79","97"], 
    "34": ["34","43","39","93","84","48","89","98"]
}

KEP_DICT = {
    "K.AM": ["07","70","14","41","29","92","36","63","58","85"],
    "K.BANG": ["00","11","22","33","44","55","66","77","88","99"],
    "K.LECH": ["05","50","16","61","27","72","38","83","49","94"],
    "S.KEP": ["01","10","12","21","23","32","34","43","45","54","56","65","67","76","78","87","89","98","09","90"]
}
# Add inverse set: 2D numbers that are not any type of kép
_all_kep_2d = set()
for _v in KEP_DICT.values(): _all_kep_2d.update(_v)
KEP_DICT["K.KHONG"] = [f"{_i:02d}" for _i in range(100) if f"{_i:02d}" not in _all_kep_2d]

ZODIAC_DICT = {
    "Tý":   ["00","12","24","36","48","60","72","84","96"],
    "Sửu":  ["01","13","25","37","49","61","73","85","97"],
    "Dần":  ["02","14","26","38","50","62","74","86","98"],
    "Mão":  ["03","15","27","39","51","63","75","87","99"],
    "Thìn": ["04","16","28","40","52","64","76","88"],
    "Tỵ":   ["05","17","29","41","53","65","77","89"],
    "Ngọ":  ["06","18","30","42","54","66","78","90"],
    "Mùi":  ["07","19","31","43","55","67","79","91"],
    "Thân": ["08","20","32","44","56","68","80","92"],
    "Dậu":  ["09","21","33","45","57","69","81","93"],
    "Tuất": ["10","22","34","46","58","70","82","94"],
    "Hợi":  ["11","23","35","47","59","71","83","95"]
}
STATION_ABBREVIATIONS = {
    "Tiền Giang": "TG", "Kiên Giang": "KG", "Đà Lạt": "DL", "Đồng Tháp": "DT", 
    "TP. Hồ Chí Minh": "HCM", "Cà Mau": "CM", "Bến Tre": "BT", "Vũng Tàu": "VT", 
    "Bạc Liêu": "BL", "Đồng Nai": "DN", "Cần Thơ": "CT", "Sóc Trăng": "ST",
    "Tây Ninh": "TN", "An Giang": "AG", "Bình Thuận": "BTh", "Trà Vinh": "TV", 
    "Vĩnh Long": "VL", "Bình Dương": "BD", "Long An": "LA", "Bình Phước": "BP", 
    "Hậu Giang": "HG", "Đà Nẵng": "DNa", "Khánh Hòa": "KH", "Kon Tum": "KT",
    "Thừa Thiên Huế": "TTH", "Phú Yên": "PY", "Đắk Lắk": "DLK", "Quảng Nam": "QNa",
    "Quảng Trị": "QT", "Bình Định": "BDi", "Quảng Bình": "QB", "Gia Lai": "GL", 
    "Ninh Thuận": "NT", "Quảng Ngãi": "QNg", "Đắk Nông": "DNo"
}

MB_PRIZE_NAMES = {
    0: "ĐB", 1: "G1", 2: "G2.1", 3: "G2.2",
    4: "G3.1", 5: "G3.2", 6: "G3.3", 7: "G3.4", 8: "G3.5", 9: "G3.6",
    10: "G4.1", 11: "G4.2", 12: "G4.3", 13: "G4.4",
    14: "G5.1", 15: "G5.2", 16: "G5.3", 17: "G5.4", 18: "G5.5", 19: "G5.6",
    20: "G6.1", 21: "G6.2", 22: "G6.3",
    23: "G7.1", 24: "G7.2", 25: "G7.3", 26: "G7.4"
}

def get_day_name_vi(date_obj):
    days = ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ Nhật"]
    return days[date_obj.weekday()]

# =============================================================================
# LOGIC FOR SOI CẦU PHOI (Integrated from tkinter_app.py)
# =============================================================================

def skp_fetch_data(type_data='month'):
    try:
        sess = requests.Session()
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        if type_data == 'month':
            url = 'https://congcuxoso.com/MienBac/DacBiet/PhoiCauDacBiet/PhoiCauThang5So.aspx'
            r1 = sess.get(url, timeout=10, headers=headers, verify=False)
            soup = BeautifulSoup(r1.text, 'lxml')
            payload = {inp['name']: inp.get('value', '') for inp in soup.find_all('input', {'type': 'hidden'})}
            m, y = str(datetime.now().month), str(datetime.now().year)
            payload.update({
                'ctl00$ContentPlaceHolder1$ddlThang': m,
                'ctl00$ContentPlaceHolder1$ddlNam': y,
                'ctl00$ContentPlaceHolder1$btnXem': 'Xem'
            })
            r2 = sess.post(url, data=payload, timeout=10, headers=headers, verify=False)
            table_kw = 'Ngày'
        else: # year
            url = 'https://congcuxoso.com/MienBac/DacBiet/PhoiCauDacBiet/PhoiCauNam5So.aspx'
            r1 = sess.get(url, timeout=10, headers=headers, verify=False)
            soup = BeautifulSoup(r1.text, 'lxml')
            payload = {inp['name']: inp.get('value', '') for inp in soup.find_all('input', {'type': 'hidden'})}
            y = str(datetime.now().year)
            payload.update({
                'ctl00$ContentPlaceHolder1$ddlNam': y,
                'ctl00$ContentPlaceHolder1$btnXem': 'Xem'
            })
            r2 = sess.post(url, data=payload, timeout=10, headers=headers, verify=False)
            table_kw = 'TH1'

        soup2 = BeautifulSoup(r2.text, 'lxml')
        table = next(t for t in soup2.find_all('table') if t.find('tr') and table_kw in t.find('tr').get_text())
        df = pd.read_html(StringIO(str(table)), header=0)[0].fillna('')
        
        def fmt(v, col_name):
            s = str(v).strip()
            if not s or s == '-----': return ''
            if s.endswith('.0'): s = s[:-2]
            if col_name == 'Ngày': return s
            return s.zfill(5)
        
        df = df.apply(lambda col: col.map(lambda v: fmt(v, col.name)))
        return df
    except Exception as e:
        print(f"Lỗi fetch_data Phoi: {e}")
        return None

def skp_find_pattern_position(value, pattern, allow_reverse=False):
    val_str = str(value).strip()
    if not val_str or not pattern or len(pattern) != 2: return -1
    patterns_to_check = [pattern]
    if allow_reverse and pattern[0] != pattern[1]: patterns_to_check.append(pattern[::-1])
    for i in range(len(val_str) - 1):
        if val_str[i:i+2] in patterns_to_check: return i
    return -1

def skp_matches_last_two_digits(value, pattern, exact=False, position=None, allow_reverse=False):
    val_str = str(value).strip()
    if not val_str or not pattern: return False
    if exact:
        if len(pattern) != 2: return False
        patterns_to_check = [pattern]
        if allow_reverse and pattern[0] != pattern[1]: patterns_to_check.append(pattern[::-1])
        if position is not None:
            if position < 0 or position >= len(val_str) - 1: return False
            return val_str[position:position+2] in patterns_to_check
        for i in range(len(val_str) - 1):
            if val_str[i:i+2] in patterns_to_check: return True
        return False
    else:
        if pattern[0] == pattern[1]: return pattern[0] in val_str
        temp_val = val_str
        for char in pattern:
            if char in temp_val: temp_val = temp_val.replace(char, "", 1)
            else: return False
        return True

def skp_get_prev_cell_year(df, row_idx, col_name):
    if row_idx > 0: return row_idx - 1, col_name
    if not col_name.startswith("TH"): return -1, None
    m = int(col_name[2:])
    pm = 12 if m == 1 else m - 1
    pcol = f"TH{pm}"
    if pcol not in df.columns: return -1, None
    col_data = df[pcol]
    for r in range(len(col_data)-1, -1, -1):
        if col_data.iloc[r] != '': return r, pcol
    return -1, pcol

def skp_get_patterns(df, is_year_data, row_idx, col_name, num_patterns):
    patterns = []
    pattern_months = set()
    if is_year_data:
        cur_day, cur_col = row_idx, col_name
        for _ in range(num_patterns):
            p_day, p_col = skp_get_prev_cell_year(df, cur_day, cur_col)
            if p_day < 0 or not p_col: patterns.append('')
            else:
                val = df.iloc[p_day][p_col]
                patterns.append(val[-2:] if len(val) >= 2 else '')
                pattern_months.add(p_col)
                cur_day, cur_col = p_day, p_col
    else:
        year_col = str(datetime.now().year)
        col_to_use = year_col if year_col in df.columns else df.columns[-1]
        for i in range(1, num_patterns + 1):
            idx = row_idx - i
            if idx >= 0:
                val = df.iloc[idx][col_to_use]
                patterns.append(val[-2:] if len(val) >= 2 else '')
            else: patterns.append('')
    patterns.reverse()
    return patterns, pattern_months

def skp_scan_cau(df, patterns, num_patterns, exact_match, is_year_data, pattern_months, selected_month, target_step=None, allow_reverse=False):
    results = {}; cau_positions = set(); predict_positions = set()
    ignore_cols = ['Ngày']
    if is_year_data: search_cols = [c for c in df.columns if c not in pattern_months and c not in ignore_cols and c != selected_month]
    else:
        now_y = str(datetime.now().year)
        search_cols = [c for c in df.columns if c != now_y and c not in ignore_cols]
    directions = [("Trên xuống (↓)", True), ("Dưới lên (↑)", False)]
    for step in range(6):
        if target_step is not None and step != target_step: continue
        gap = step + 1
        for dir_label, inside in directions:
            count = 0; result_vals = []; key = f"{dir_label} - Cách {step}"
            for col in search_cols:
                for i in range(len(df)):
                    if inside: 
                        if i <= len(df) - num_patterns * gap:
                            ok = True; positions_temp = []; fixed_position = None
                            for k in range(num_patterns):
                                val = df.iloc[i + k*gap][col]
                                if exact_match:
                                    if k == 0:
                                        fixed_position = skp_find_pattern_position(val, patterns[k], allow_reverse)
                                        if fixed_position == -1: ok = False; break
                                    else:
                                        if not skp_matches_last_two_digits(val, patterns[k], exact_match, fixed_position, allow_reverse): ok = False; break
                                else:
                                    if not skp_matches_last_two_digits(val, patterns[k], exact_match, None, allow_reverse): ok = False; break
                                positions_temp.append((i + k*gap, col))
                            if ok:
                                pred_idx = i + (num_patterns - 1)*gap + gap
                                if 0 <= pred_idx < len(df):
                                    pred_val = df.iloc[pred_idx][col]
                                    if pred_val: count += 1
                                    result_vals.append({'value': pred_val, 'predict_pos': (pred_idx, col), 'cau_pos': positions_temp, 'match_position': fixed_position if exact_match else None})
                                    cau_positions.update(positions_temp); predict_positions.add((pred_idx, col))
                    else:
                        if i >= (num_patterns - 1) * gap:
                            ok = True; positions_temp = []; fixed_position = None
                            for k in range(num_patterns):
                                val = df.iloc[i - k*gap][col]
                                if exact_match:
                                    if k == 0:
                                        fixed_position = skp_find_pattern_position(val, patterns[k], allow_reverse)
                                        if fixed_position == -1: ok = False; break
                                    else:
                                        if not skp_matches_last_two_digits(val, patterns[k], exact_match, fixed_position, allow_reverse): ok = False; break
                                else:
                                    if not skp_matches_last_two_digits(val, patterns[k], exact_match, None, allow_reverse): ok = False; break
                                positions_temp.append((i - k*gap, col))
                            if ok:
                                pred_idx = i - (num_patterns - 1)*gap - gap
                                if 0 <= pred_idx < len(df):
                                    pred_val = df.iloc[pred_idx][col]
                                    if pred_val: count += 1
                                    result_vals.append({'value': pred_val, 'predict_pos': (pred_idx, col), 'cau_pos': positions_temp, 'match_position': fixed_position if exact_match else None})
                                    cau_positions.update(positions_temp); predict_positions.add((pred_idx, col))
            pairs = []
            for item in result_vals:
                val = str(item['value']).strip()
                pos = item.get('match_position')
                if exact_match:
                    if pos is not None and 0 <= pos < len(val) - 1:
                        pairs.append(val[pos:pos+2])
                    elif len(val) >= 2:
                        pairs.append(val[-2:])
                else:
                    # Nhị Hợp Expansion: generate all 2-digit pairs from unique digits
                    unique_digits = sorted(list(set([d for d in val if d.isdigit()])))
                    nhi_hop = ["".join(p) for p in product(unique_digits, repeat=2)]
                    pairs.extend(nhi_hop)
            results[key] = {'count': count, 'items': result_vals, 'pairs': pairs}
    return results, cau_positions, predict_positions

def skp_scan_cau_horizontal(df, patterns, num_patterns, exact_match, is_year_data, pattern_months, selected_month, pattern_row_idx, target_step=None, allow_reverse=False):
    results = {}; cau_positions = set(); predict_positions = set()
    ignore_cols = ['Ngày']
    if is_year_data:
        pattern_source_col = selected_month
        search_cols = [c for c in df.columns if c not in pattern_months and c not in ignore_cols and c != selected_month]
    else:
        pattern_source_col = str(datetime.now().year)
        search_cols = [c for c in df.columns if c != pattern_source_col and c not in ignore_cols]
    all_cols = [col for col in df.columns if col not in ignore_cols]
    directions = [("Trái sang phải (→)", True), ("Phải sang trái (←)", False)]
    for step in range(6):
        if target_step is not None and step != target_step: continue
        gap = step + 1
        for dir_label, forward in directions:
            count = 0; result_vals = []; key = f"{dir_label} - Cách {step}"
            for row_idx in range(len(df)):
                for col_start_idx in range(len(all_cols)):
                    start_col = all_cols[col_start_idx]
                    if start_col == pattern_source_col: continue
                    if forward:
                        if col_start_idx + (num_patterns - 1) * gap + gap >= len(all_cols): continue
                        ok = True; positions_temp = []; fixed_position = None
                        for k in range(num_patterns):
                            col_idx = col_start_idx + k * gap
                            if col_idx >= len(all_cols): ok = False; break
                            col = all_cols[col_idx]
                            if col == pattern_source_col: ok = False; break
                            val = df.iloc[row_idx][col]
                            if exact_match:
                                if k == 0:
                                    fixed_position = skp_find_pattern_position(val, patterns[k], allow_reverse)
                                    if fixed_position == -1: ok = False; break
                                else:
                                    if not skp_matches_last_two_digits(val, patterns[k], exact_match, fixed_position, allow_reverse): ok = False; break
                            else:
                                if not skp_matches_last_two_digits(val, patterns[k], exact_match, None, allow_reverse): ok = False; break
                            positions_temp.append((row_idx, col))
                        if ok:
                            pred_col_idx = col_start_idx + (num_patterns - 1) * gap + gap
                            if 0 <= pred_col_idx < len(all_cols):
                                pred_col = all_cols[pred_col_idx]
                                if pred_col != pattern_source_col:
                                    pred_val = df.iloc[row_idx][pred_col]
                                    if pred_val: count += 1
                                    result_vals.append({'value': pred_val, 'predict_pos': (row_idx, pred_col), 'cau_pos': positions_temp, 'match_position': fixed_position if exact_match else None})
                                    cau_positions.update(positions_temp); predict_positions.add((row_idx, pred_col))
                    else:
                        if col_start_idx - (num_patterns - 1) * gap - gap < 0: continue
                        ok = True; positions_temp = []; fixed_position = None
                        for k in range(num_patterns):
                            col_idx = col_start_idx - k * gap
                            if col_idx < 0: ok = False; break
                            col = all_cols[col_idx]
                            if col == pattern_source_col: ok = False; break
                            val = df.iloc[row_idx][col]
                            if exact_match:
                                if k == 0:
                                    fixed_position = skp_find_pattern_position(val, patterns[k], allow_reverse)
                                    if fixed_position == -1: ok = False; break
                                else:
                                    if not skp_matches_last_two_digits(val, patterns[k], exact_match, fixed_position, allow_reverse): ok = False; break
                            else:
                                if not skp_matches_last_two_digits(val, patterns[k], exact_match, None, allow_reverse): ok = False; break
                            positions_temp.append((row_idx, col))
                        if ok:
                            pred_col_idx = col_start_idx - (num_patterns - 1) * gap - gap
                            if 0 <= pred_col_idx < len(all_cols):
                                pred_col = all_cols[pred_col_idx]
                                if pred_col != pattern_source_col:
                                    pred_val = df.iloc[row_idx][pred_col]
                                    if pred_val: count += 1
                                    result_vals.append({'value': pred_val, 'predict_pos': (row_idx, pred_col), 'cau_pos': positions_temp, 'match_position': fixed_position if exact_match else None})
                                    cau_positions.update(positions_temp); predict_positions.add((row_idx, pred_col))
            pairs = []
            for item in result_vals:
                val = str(item['value']).strip()
                pos = item.get('match_position')
                if exact_match:
                    if pos is not None and 0 <= pos < len(val) - 1:
                        pairs.append(val[pos:pos+2])
                    elif len(val) >= 2:
                        pairs.append(val[-2:])
                else:
                    # Nhị Hợp Expansion: generate all 2-digit pairs from unique digits
                    unique_digits = sorted(list(set([d for d in val if d.isdigit()])))
                    nhi_hop = ["".join(p) for p in product(unique_digits, repeat=2)]
                    pairs.extend(nhi_hop)
            results[key] = {'count': count, 'items': result_vals, 'pairs': pairs}
    return results, cau_positions, predict_positions

def assemble_mnmt_prizes(r):
    return r.get('all_prizes', [""] * 18)

def fetch_mnmt_data_7days(mb_date_str: str, num_days: int = 7):
    try:
        mb_date = datetime.strptime(mb_date_str, "%d/%m/%Y")
    except Exception:
        return {}, "Lỗi định dạng ngày."
    
    # Get today's date (without time component) to limit check range
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    check_dates = []
    for i in range(1, num_days + 1):
        d = mb_date + timedelta(days=i)
        # Only include dates up to today, not future dates
        if d <= today:
            check_dates.append(d)
        
    stations_to_fetch = set()
    for d in check_dates:
        day_name = get_day_name_vi(d)
        stations_to_fetch.update(LICH_QUAY_NAM.get(day_name, []))
        stations_to_fetch.update(LICH_QUAY_TRUNG.get(day_name, []))
        
    fetched_data = {} 
    
    def fetch_one(st_name):
        return st_name, fetch_station_data(st_name, total_days=30)

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_st = {executor.submit(fetch_one, st): st for st in stations_to_fetch}
        for future in concurrent.futures.as_completed(future_to_st):
            st_name, rows = future.result()
            formatted_rows = []
            for r in rows:
                formatted_rows.append({"Date": r['date'], "ObjDate": datetime.strptime(r['date'], "%d/%m/%Y"), "Prizes": assemble_mnmt_prizes(r)})
            fetched_data[st_name] = formatted_rows
            
    return fetched_data, ""

HIEU_DICT = {
    0:  ["00","11","22","33","44","55","66","77","88","99"],
    1:  ["09","10","21","32","43","54","65","76","87","98"],
    2:  ["08","19","20","31","42","53","64","75","86","97"],
    3:  ["07","18","29","30","41","52","63","74","85","96"],
    4:  ["06","17","28","39","40","51","62","73","84","95"],
    5:  ["05","16","27","38","49","50","61","72","83","94"],
    6:  ["04","15","26","37","48","59","60","71","82","93"],
    7:  ["03","14","25","36","47","58","69","70","81","92"],
    8:  ["02","13","24","35","46","57","68","79","80","91"],
    9:  ["01","12","23","34","45","56","67","78","89","90"]
}

HIEU_2CANG = {i: HIEU_DICT[i] for i in range(10)}

def bo(db: str) -> str:
    db = db.zfill(2)
    if db in BO_DICT: return db
    for key, vals in BO_DICT.items():
        if db in vals: return key
    return "44"

def kep(db: str) -> str:
    db = db.zfill(2)
    for key, vals in KEP_DICT.items():
        if db in vals: return key
    return "K.KHONG"

def zodiac(pair: str) -> str:
    p = pair.zfill(2)
    return next((a for a, lst in ZODIAC_DICT.items() if p in lst), "-")

def hieu(pair: str) -> int:
    p = pair.zfill(2)
    for delay, nums in HIEU_DICT.items():
        if p in nums: return delay
    return -1


def get_lastN(s, n):
    s = str(s).strip()
    if len(s) >= n and s.isdigit(): return s[-n:]
    return ""

def analyze_aggregated_frequency(numbers_list, n_digits=2):
    if not numbers_list: return None
    count_nums = Counter(numbers_list)
    count_tongs = Counter()
    count_chams = Counter()
    for num_str in numbers_list:
        if len(num_str) == n_digits and num_str.isdigit():
            digits = [int(d) for d in num_str]
            count_tongs[sum(digits) % 10] += 1
            for d in set(digits):
                count_chams[d] += 1
    def get_stats(counter_obj):
        sorted_digits = sorted(range(10), key=lambda x: counter_obj[x], reverse=True)
        return (dict(counter_obj), sorted_digits)
    return {
        "cham": get_stats(count_chams),
        "tong": get_stats(count_tongs),
        "top_cham": sorted(count_chams.items(), key=lambda x: x[1], reverse=True)[:5],
        "top_tong": sorted(count_tongs.items(), key=lambda x: x[1], reverse=True)[:5],
        "raw_counts": count_nums
    }

def check_row_logic(mn_row, mb_dict, station_name, mb_scope="ĐB", mode="2D", mode_gen="Nhị hợp"):
    prizes_mn = mn_row["Prizes"]
    mn_date = mn_row["ObjDate"]
    n_digits = int(mode[0]) if mode[0].isdigit() else 2
    
    # Get today's date (without time component) to limit check range
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    check_dates = []
    for i in range(1, 8):
        d = mn_date + timedelta(days=i)
        # Only include dates up to today, not future dates
        if d <= today:
            check_dates.append(d.strftime("%d/%m/%Y"))
    mb_results = {}
    for d_str in check_dates:
        prizes = mb_dict.get(d_str, [])
        targets = []
        if prizes:
            if mb_scope == "ĐB":
                if prizes[0]:
                    val = get_lastN(prizes[0], n_digits)
                    if val: targets.append(val)
            elif mb_scope == "Đầu ĐB":
                if prizes[0]:
                    full_num = str(prizes[0]).strip()
                    if len(full_num) >= n_digits:
                        val = full_num[:n_digits]
                        if val.isdigit(): targets.append(val)
            elif mb_scope == "G1":
                if prizes[1]:
                    val = get_lastN(prizes[1], n_digits)
                    if val: targets.append(val)
            elif mb_scope == "Đầu G1":
                if prizes[1]:
                    full_num = str(prizes[1]).strip()
                    if len(full_num) >= n_digits:
                        val = full_num[:n_digits]
                        if val.isdigit(): targets.append(val)
        mb_results[d_str] = targets
    results_list = []
    IMPORTANT_INDICES = [0, 1, 16, 17] 
    PRIZES_LABELS = ["ĐB", "G1", "G2", "G3", "G3", "G4", "G4", "G4", "G4", "G4", "G4", "G4", "G5", "G6", "G6", "G6", "G7", "G8"]
    def analyze_nhi_hop_local(prizes, selected_indices, n_digits, gen_mode="Nhị hợp"):
        digits = ""
        val_parts = []
        for idx in selected_indices:
            if idx < len(prizes): 
                val_parts.append(str(prizes[idx]))
                digits += str(prizes[idx])
        if not digits: return [], ""
        
        if gen_mode == "Chọn lọc":
            # "Chọn lọc" mode: use all digits 0-9
            digits = "0123456789"
            
        dan = sorted(set("".join(p) for p in product(digits, repeat=n_digits)))
        return dan, " | ".join(val_parts)

    for r in [2, 3, 4]:
        for combo_indices in combinations(IMPORTANT_INDICES, r):
            dan_so, prize_vals = analyze_nhi_hop_local(prizes_mn, combo_indices, n_digits, gen_mode=mode_gen)
            if not dan_so: continue
            hits = []; has_hit = False; detail_check = []
            for d_str in check_dates:
                targets = mb_results[d_str]
                hit_vals = [t for t in targets if t in dan_so]
                if hit_vals:
                    hits.append(f"{','.join(hit_vals)} ({d_str})")
                    has_hit = True; detail_check.append("✅")
                else: detail_check.append("❌" if targets else "⏳")
            combo_name = " + ".join([PRIZES_LABELS[i] for i in combo_indices if i < len(PRIZES_LABELS)])
            results_list.append({
                "has_hit": has_hit, "Đài": station_name, "Giá trị giải": prize_vals, "Giải ghép": combo_name,
                "Dàn số": ", ".join(dan_so), "Số lượng": len(dan_so), "T7 -> T6": " ".join(detail_check),
                "Ghi chú": "CHƯA RA" if not has_hit else f"NỔ: {', '.join(hits)}"
            })
    return results_list

def scan_multistation_subset(region_code, sel_date, mb_dict, weekday, mb_scope="ĐB", progress_callback=None, mode="2D", mode_gen="Nhị hợp"):
    sts = []
    if region_code == "BOTH":
        sts = get_stations_by_day("Miền Nam", weekday) + get_stations_by_day("Miền Trung", weekday)
    else: sts = get_stations_by_day("Miền Nam" if region_code=="MN" else "Miền Trung", weekday)
    if not sts: return []
    all_rows = []
    for i, st in enumerate(sts):
        if progress_callback: progress_callback(i/len(sts), f"Tải dữ liệu {st}...")
        rows = fetch_station_data(st, total_days=30)
        for r in rows:
            if r['date'] == sel_date: all_rows.append({"Station": st, "Data": r})
    if not all_rows: return []
    results_list = []
    for i, item in enumerate(all_rows):
        st = item["Station"]; r = item["Data"]
        if progress_callback: progress_callback(i/len(all_rows), f"Đang quét {st}...")
        mn_row = {"Prizes": assemble_mnmt_prizes(r), "ObjDate": datetime.strptime(r['date'], "%d/%m/%Y")}
        res = check_row_logic(mn_row, mb_dict, st, mb_scope=mb_scope, mode=mode, mode_gen=mode_gen)
        if res: results_list.extend(res)
    return results_list

def scan_mb_to_mnmt(mb_date_str, prizes_mb, mn_target_indices=None, num_days=7, fetched_data=None, region_filter="BOTH", mode="2D", mode_gen="Nhị hợp"):
    if mn_target_indices is None: mn_target_indices = range(18)
    n_digits = int(mode[0]) if mode[0].isdigit() else 2
    try: mb_date = datetime.strptime(mb_date_str, "%d/%m/%Y")
    except: return [], "Lỗi định dạng ngày."
    
    # Get today's date (without time component) to limit check range
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Only include dates up to today, not future dates
    check_dates = [mb_date + timedelta(days=i) for i in range(1, num_days + 1) if mb_date + timedelta(days=i) <= today]
    results_list = []
    valid_indices = list(range(min(20, len(prizes_mb))))
    def get_mn_prize_name(idx):
        if idx == 0: return "ĐB"; 
        if idx == 1: return "G1"; 
        if idx == 2: return "G2"; 
        if 3 <= idx <= 4: return "G3"; 
        if 5 <= idx <= 11: return "G4"; 
        if idx == 12: return "G5"; 
        if 13 <= idx <= 15: return "G6"; 
        if idx == 16: return "G7"; 
        if idx == 17: return "G8"; 
        return ""
    for idx1, idx2 in combinations(valid_indices, 2):
        val1, val2 = prizes_mb[idx1], prizes_mb[idx2]
        
        if mode_gen == "Chọn lọc":
            digits = "0123456789"
        else:
            digits = str(val1) + str(val2)
            
        dan_so = sorted(set("".join(p) for p in product(digits, repeat=n_digits)))
        if not dan_so: continue
        checklist = []; detailed_hits = []; has_hit = False
        for d in check_dates:
            d_str = d.strftime("%d/%m/%Y"); day_name = get_day_name_vi(d)
            sts = []
            if region_filter == "BOTH": sts = LICH_QUAY_NAM.get(day_name, []) + LICH_QUAY_TRUNG.get(day_name, [])
            elif region_filter == "MN": sts = LICH_QUAY_NAM.get(day_name, [])
            elif region_filter == "MT": sts = LICH_QUAY_TRUNG.get(day_name, [])
            day_hits = []; day_has_data = False
            for st in sts:
                rows = fetched_data.get(st, [])
                row = next((r for r in rows if r["Date"] == d_str), None)
                if row:
                    day_has_data = True; prizes_mn = row["Prizes"]
                    found_details = [] 
                    for i, p in enumerate(prizes_mn):
                        if i not in mn_target_indices: continue
                        val = get_lastN(p, n_digits)
                        if val in dan_so: found_details.append(f"{val} ({get_mn_prize_name(i)})")
                    if found_details: day_hits.append(f"{STATION_ABBREVIATIONS.get(st, st)}: {', '.join(found_details)}")
            if day_hits:
                checklist.append("✅"); detailed_hits.append(f"✅ {d_str}: {'; '.join(day_hits)}"); has_hit = True
            elif day_has_data: checklist.append("❌");
            else: checklist.append("⏳")
        results_list.append({
            "has_hit": has_hit, "Tổ hợp MB": f"{MB_PRIZE_NAMES.get(idx1, f'Idx{idx1}')} + {MB_PRIZE_NAMES.get(idx2, f'Idx{idx2}')}",
            "Giá trị giải": f"{val1} | {val2}", "Dàn số": ", ".join(dan_so), "Số lượng": len(dan_so), "Checklist": " ".join(checklist), "Chi tiết": "\n".join(detailed_hits)
        })
    results_list.sort(key=lambda x: (x["has_hit"], -x["Số lượng"]))
    return results_list, None

# ====================================
# PHẦN 2: GUI TKINTER
# ====================================

class SieuGaApp:
    """Ứng dụng phân tích xổ số với các tính năng Matrix, Tổng & Chạm 3D."""
    
    # Threshold constants
    GAN_HIGH_THRESHOLD = 15      # Ngưỡng gan cao (hiển thị màu đỏ)
    GAN_VERY_HIGH_THRESHOLD = 20 # Ngưỡng gan rất cao
    MAX_DISPLAY_DAYS = 30        # Số ngày tối đa hiển thị trong matrix
    MAX_MATRIX_COLS = 28         # Số cột tối đa trong matrix (Tăng lên 28 theo yêu cầu)
    MIN_FETCH_DAYS = 1           # Số ngày tối thiểu để fetch
    MAX_FETCH_DAYS = 365         # Số ngày tối đa để fetch
    
    def __init__(self, root):
        """Khởi tạo ứng dụng SieuGaApp.
        
        Args:
            root: Tkinter root window
        """
        self.root = root
        self.root.title("🐔 SIÊU GÀ APP")
        self.root.geometry("1200x800")
        try:
            self.root.state('zoomed')
        except:
            pass
        
        # Màu sắc dark theme
        self.bg_color = "#1e1e1e"
        self.fg_color = "#ffffff"
        self.accent_color = "#27ae60"  # Green
        self.secondary_bg = "#2d2d2d"
        
        # Initialize data attributes
        self.master_data = []
        self.station_data = []
        self.is_loading = False
        
        # Configure style
        self.setup_styles()
        
        # New Tab Variables
        self.mnmt_cache = {}
        self.two_way_results_data = [] # Stores full results for table
        self.mn_mt_results = [] # Stores raw numbers for tab 3
        self.mn_mt_date = None
        self.two_way_date_mnmt = None
        self.two_way_direction = tk.StringVar(value="MN") # Default: MN/MT -> MB
        self.two_way_dates_loaded = False
        self._initial_scan_done = False
        self.WEEKDAYS = ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ Nhật"]
        self.last_scan_direction = "MB_TO_MNMT"
        self.scan_mode_var = tk.StringVar(value="2D")
        self.scan_gen_mode = tk.StringVar(value="Nhị hợp")
        
        # Variables for BC-CD-DE Tab
        self.bc_cd_de_vars = {} # (date_key, type_str) -> BooleanVar
        self.bc_cd_de_results = [] # To store display rows
        self.bc_show_sidebar_nums = tk.BooleanVar(value=False)
        self._bc_update_pending = False  # Debouncing flag
        self._bc_last_render_state = None # To prevent redundant UI rebuilds
        self._is_analyzing_bc = False # Thread lock for calculations
        
        # Sync Scan execution across tabs
        self.scan_mode_var.trace_add('write', lambda *a: self._on_scan_mode_changed())
        self.scan_gen_mode.trace_add('write', lambda *a: self._on_scan_mode_changed())
        
        # --- Variables for Soi Cầu Phoi (Integrated) ---
        self.skp_df = None
        self.skp_cache = {} # Cache for Month/Year dataframes
        self.skp_highlight_target = None
        self.skp_data_mode_var = tk.StringVar(value="Tháng")
        self.skp_selected_day_var = tk.StringVar()
        self.skp_selected_month_var = tk.StringVar()
        self.skp_num_patterns_var = tk.IntVar(value=2)
        self.skp_exact_match_var = tk.BooleanVar(value=False)
        self.skp_contains_both_var = tk.BooleanVar(value=True)
        self.skp_allow_reverse_var = tk.BooleanVar(value=False)
        self.skp_selected_step_var = tk.StringVar(value="Tất cả các cách")
        self.skp_view_mode_var = tk.StringVar(value="Highlight Cầu")
        self.skp_search_direction_var = tk.StringVar(value="Lên Xuống")
        self.skp_check_num_var = tk.StringVar()
        self.skp_COLORS = ['#ffcccc', '#ccffcc', '#ccccff', '#ffcc99', '#99ccff', '#ff99cc']

        # Trace SKP variables for automatic updates
        self.skp_data_mode_var.trace_add("write", lambda *a: self.skp_on_mode_change())
        self.skp_selected_day_var.trace_add("write", lambda *a: self.skp_update_patterns())
        self.skp_selected_month_var.trace_add("write", lambda *a: self.skp_update_patterns())
        self.skp_num_patterns_var.trace_add("write", lambda *a: self.skp_update_patterns())
        self.skp_allow_reverse_var.trace_add("write", lambda *a: self.skp_update_patterns())
        self.skp_search_direction_var.trace_add("write", lambda *a: self.skp_update_patterns())
        self.skp_selected_step_var.trace_add("write", lambda *a: self.skp_refresh_tab1())
        self.skp_view_mode_var.trace_add("write", lambda *a: self.skp_refresh_tab1())
        self.skp_exact_match_var.trace_add("write", lambda *a: self.skp_on_exact_change())
        self.skp_contains_both_var.trace_add("write", lambda *a: self.skp_on_contains_change())
        
        # Data
        self.master_data = []
        self.is_loading = False
        self.loading_lock = threading.Lock()  # Thread safety
        
        # Pending set tracking (DE)
        self.pending_vars = {} # (date, combo_tuple) -> BooleanVar
        self.manual_selected_vars = {} # (date, combo_tuple) -> BooleanVar
        self._is_refreshing_stats = False
        
        # --- CD TAB VARIABLES ---
        self.matrix_cd_days_data = []
        self.pending_cd_vars = {}
        self.manual_selected_cd_vars = {}
        self._is_refreshing_cd_stats = False

        # --- BC TAB VARIABLES ---
        self.matrix_bc_days_data = []
        self.pending_bc_vars = {} 
        self.manual_selected_bc_vars = {}
        self._is_refreshing_bc_stats = False
        self.manual_level0_rows = []  # Lưu các hàng được thêm thủ công vào Tab Lọc Mức 0
        
        # --- KYBE TAB VARIABLES ---
        self.kybe_selected_ngau = []
        self.kybe_selected_tong = []
        self.kybe_highlight_sum = []
        self.kybe_highlight_ngau = []
        self.kybe_highlight_digits = []
        self.kybe_last_rows = []
        self.kybe_grid_widgets = {} # Storage for persistent grid labels/buttons
        
        # --- LAZY LOADING & DEBOUNCING ---
        self.last_rendered_matrix_day = {"DE": None, "CD": None, "BC": None}
        self._backtest_timer = None
        self.create_widgets()

        # --- TOOLTIP ---
        self.matrix_tooltip = tk.Label(self.root, text="", bg="#facc15", fg="black", 
                                       font=('Segoe UI', 9), relief='solid', borderwidth=1, padx=5, pady=2)
        self.matrix_tooltip.lower() # Start hidden
        
        # --- Matrix Configuration Mapping ---
        # Moving this after create_widgets because it depends on widgets (canvases, frames, hit_canvas, labels)
        self.matrix_configs = {
            "DE": {
                "slice": slice(-2, None), 
                "vars": self.manual_selected_vars,
                "pending_vars": self.pending_vars,
                "canvases": (self.fixed_canvas, self.matrix_canvas),
                "frames": (self.fixed_content_frame, self.matrix_content_frame),
                "stats_text": self.stats_text,
                "hit_canvas": self.hit_chart_canvas,
                "info_label": self.matrix_info_label,
                "stats_label": getattr(self, 'matrix_stats_label', None),
                "show_de_sync": False,
                "label_prefix": "DE",
                "show_source_var": getattr(self, 'var_show_source', None)
            },
            "CD": {
                "slice": slice(-3, -1),
                "vars": self.manual_selected_cd_vars,
                "pending_vars": self.pending_cd_vars,
                "canvases": (self.fixed_cd_canvas, self.matrix_cd_canvas),
                "frames": (self.fixed_cd_content_frame, self.matrix_cd_content_frame),
                "stats_text": self.stats_cd_text,
                "hit_canvas": self.hit_chart_cd_canvas,
                "info_label": self.matrix_cd_info_label,
                "stats_label": getattr(self, 'matrix_cd_stats_label', None),
                "show_de_sync": True,
                "label_prefix": "CD",
                "show_source_var": getattr(self, 'var_show_source_cd', None)
            },
            "BC": {
                "slice": slice(-4, -2),
                "vars": self.manual_selected_bc_vars,
                "pending_vars": self.pending_bc_vars,
                "canvases": (self.fixed_bc_canvas, self.matrix_bc_canvas),
                "frames": (self.fixed_bc_content_frame, self.matrix_bc_content_frame),
                "stats_text": self.stats_bc_text,
                "hit_canvas": self.hit_chart_bc_canvas,
                "info_label": self.matrix_bc_info_label,
                "stats_label": getattr(self, 'matrix_bc_stats_label', None),
                "show_de_sync": True,
                "label_prefix": "BC",
                "show_source_var": getattr(self, 'var_show_source_bc', None)
            }
        }
        
    def setup_styles(self):
        style = ttk.Style()
        style.theme_use('clam')
        
        # Configure colors
        style.configure('TFrame', background=self.bg_color)
        style.configure('TLabel', background=self.bg_color, foreground=self.fg_color, font=('Segoe UI', 10))
        style.configure('Title.TLabel', font=('Segoe UI', 16, 'bold'), foreground=self.accent_color)
        style.configure('TButton', font=('Segoe UI', 10, 'bold'))
        style.map('TButton', background=[('active', self.accent_color)])
        
        style.configure('Treeview', 
                        background=self.secondary_bg, 
                        foreground=self.fg_color, 
                        fieldbackground=self.secondary_bg,
                        font=('Consolas', 9),
                        rowheight=25)
        style.configure('Treeview.Heading', 
                        background=self.accent_color, 
                        foreground=self.fg_color,
                        font=('Segoe UI', 10, 'bold'))
        
        # Gridlines/Borders emulation using row border width if supported, 
        # or just ensuring style is consistent.
        style.configure('Cyber.Treeview', borderwidth=1, relief='solid')
        style.map('Treeview', background=[('selected', self.accent_color)])
        
        # Configure Scrollbar for visibility in dark theme
        style.configure('TScrollbar',
                        gripcount=0,
                        background='#6b7280',  # Lighter gray thumb
                        troughcolor='#1a1a1a',  # Slightly lighter trough but still dark
                        bordercolor='#111827',
                        lightcolor='#4b5563',
                        darkcolor='#4b5563',
                        arrowcolor='white')
        style.map('TScrollbar',
                  background=[('active', self.accent_color)])
        
        # Configure Progressbar with green color
        style.configure('TProgressbar',
                        troughcolor='#2d2d2d',  # Dark gray background
                        background='#27ae60',    # Green bar
                        thickness=20)
        
    def create_widgets(self):
        # Main container
        main_frame = ttk.Frame(self.root, style='TFrame')
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Title
        title_label = ttk.Label(main_frame, text="🐔 SIÊU GÀ", style='Title.TLabel')
        title_label.pack(pady=(0, 10))
        
        # Control panel
        control_frame = ttk.Frame(main_frame, style='TFrame')
        control_frame.pack(fill=tk.X, pady=(0, 2))
        
        # Single row: All controls in one line
        row1 = ttk.Frame(control_frame, style='TFrame')
        row1.pack(fill=tk.X, pady=1)
        
        ttk.Label(row1, text="Nguồn:", style='TLabel').pack(side=tk.LEFT, padx=2)
        self.source_var = tk.StringVar(value="Điện Toán")
        self.source_var.trace_add('write', lambda *args: self.on_setting_change())
        source_combo = ttk.Combobox(row1, textvariable=self.source_var,
                                     values=["Điện Toán", "Thần Tài"],
                                     state='readonly', width=10)
        source_combo.pack(side=tk.LEFT, padx=2)
        
        ttk.Label(row1, text="Chế độ:", style='TLabel').pack(side=tk.LEFT, padx=2)
        self.mode_var = tk.StringVar(value="2D")
        self.mode_var.trace_add('write', lambda *args: self.on_setting_change())
        mode_combo = ttk.Combobox(row1, textvariable=self.mode_var,
                                   values=["2D", "3D"],
                                   state='readonly', width=5)
        mode_combo.pack(side=tk.LEFT, padx=2)
        
        ttk.Label(row1, text="Giải:", style='TLabel').pack(side=tk.LEFT, padx=2)
        self.prize_var = tk.StringVar(value="XSMB (ĐB)")
        self.prize_var.trace_add('write', lambda *args: self.on_setting_change())
        self.prize_combo = ttk.Combobox(row1, textvariable=self.prize_var,
                                        values=["XSMB (ĐB)", "Giải Nhất", "Giải 7", "Giải 6"],
                                        state='readonly', width=10)
        self.prize_combo.pack(side=tk.LEFT, padx=2)
        
        ttk.Label(row1, text="Ngày:", style='TLabel').pack(side=tk.LEFT, padx=2)
        self.days_fetch_var = tk.IntVar(value=60)
        days_spin = tk.Spinbox(row1, from_=30, to=365, textvariable=self.days_fetch_var, width=5)
        days_spin.pack(side=tk.LEFT, padx=2)
        days_spin.bind('<Return>', lambda e: self.smart_load_data())
        
        ttk.Label(row1, text="Backtest:", style='TLabel').pack(side=tk.LEFT, padx=2)
        self.backtest_var = tk.IntVar(value=0)
        self.backtest_var.trace_add('write', lambda *args: self.on_backtest_change())
        backtest_spin = tk.Spinbox(row1, from_=0, to=60, textvariable=self.backtest_var, width=5,
                                   command=self.on_backtest_change)
        backtest_spin.pack(side=tk.LEFT, padx=2)
        
        # Separator
        ttk.Separator(row1, orient='vertical').pack(side=tk.LEFT, fill='y', padx=5)
        
        ttk.Label(row1, text="Miền:", style='TLabel').pack(side=tk.LEFT, padx=2)
        self.region_var = tk.StringVar(value="Miền Bắc")
        self.region_combo = ttk.Combobox(row1, textvariable=self.region_var,
                                         values=["Miền Bắc", "Miền Nam", "Miền Trung"],
                                         state='readonly', width=10)
        self.region_combo.pack(side=tk.LEFT, padx=2)
        self.region_combo.bind('<<ComboboxSelected>>', lambda e: self.on_region_change())
        
        ttk.Label(row1, text="Thứ:", style='TLabel').pack(side=tk.LEFT, padx=2)
        # Auto-detect current day of week
        import datetime
        current_day = datetime.datetime.now().weekday()  # 0=Monday, 6=Sunday
        day_names = ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ Nhật"]
        default_day = day_names[current_day]
        self.day_var = tk.StringVar(value=default_day)
        self.day_combo = ttk.Combobox(row1, textvariable=self.day_var,
                                       values=["Chủ Nhật", "Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7"],
                                       state='disabled', width=9)
        self.day_combo.pack(side=tk.LEFT, padx=2)
        self.day_combo.bind('<<ComboboxSelected>>', lambda e: self.on_day_change())
        
        ttk.Label(row1, text="Đài:", style='TLabel').pack(side=tk.LEFT, padx=2)
        self.station_var = tk.StringVar(value="")
        self.station_combo = ttk.Combobox(row1, textvariable=self.station_var,
                                          values=[],
                                          state='disabled', width=12)
        self.station_combo.pack(side=tk.LEFT, padx=2)
        self.station_combo.bind('<<ComboboxSelected>>', lambda e: self.smart_load_data())
        
        # Load button
        self.load_btn = ttk.Button(row1, text="🔄 Tải Dữ Liệu", command=self.smart_load_data)
        self.load_btn.pack(side=tk.LEFT, padx=5)
        
        # Station data storage
        self.station_data = []
        
        # Progress label (Hidden by default to save space)
        self.progress_label = ttk.Label(control_frame, text="", style='TLabel')
        self.progress_label.pack(pady=0)
        
        # Notebook for tabs
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # Tab 1: Data
        self.data_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.data_frame, text="📋 Dữ liệu")
        
        # Tab 2: Matrix Tracking (DE)
        self.matrix_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.matrix_frame, text="DE")
        
        # Tab 2a: Matrix Tracking (CD)
        self.matrix_cd_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.matrix_cd_frame, text="CD")
        
        # Tab 2b: Matrix Tracking (BC) - CLONED
        self.matrix_bc_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.matrix_bc_frame, text="BC")
        
        # Tab 11: BC-CD-DE (Refactored)
        self.bc_cd_de_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.bc_cd_de_frame, text="📊 BC-CD-DE")

        # Tab 3: Tổng & Chạm 3 Càng
        self.tong_cham_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.tong_cham_frame, text="📊 Tổng & Chạm 3D")
        
        # Tab 4: Tổng & Chạm 4 Càng
        self.tong_cham_4d_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.tong_cham_4d_frame, text="📊 Tổng & Chạm 4D")
        
        # Tab 5: Phân Tích Đa Chiều
        self.two_way_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.two_way_frame, text="🎯 Phân Tích Đa Chiều")
        
        # Tab 7: Tần Suất
        self.freq_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.freq_frame, text="📊 Tần Suất")
        
        # Tab 8: Bệt Chạm ĐB
        self.bet_cham_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.bet_cham_frame, text="📉 Bệt Chạm ĐB")
        
        # Tab 9: Web Style Dashboard
        self.web_dashboard_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.web_dashboard_frame, text="🔮 Dự Đoán Web Style")
        
        # Tab 10: Soi Cầu Phoi (Integrated)
        self.skp_tab_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.skp_tab_frame, text="🔍 Soi Cầu Phoi")

        # Tab 12: Lọc Mức 0 (Nhị Hợp)
        self.level0_tab_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.level0_tab_frame, text="🎯 Lọc Mức 0 (Nhị Hợp)")

        # Tab 13: Kybe - Grok (New)
        self.kybe_frame = ttk.Frame(self.notebook, style='TFrame')
        self.notebook.add(self.kybe_frame, text="🧠 Kybe - Grok")
        
        # Initialize Tab Views
        self.create_data_table()
        self.create_matrix_view()     # DE Tab
        self.create_matrix_cd_view()  # CD Tab
        self.create_matrix_bc_view()  # BC Tab
        self.create_tong_cham_view()
        self.create_tong_cham_4d_view()
        self.create_selector_view()
        self.create_two_way_analysis_view()
        self.create_frequency_tab_view()
        self.create_bet_cham_view()
        self.create_web_dashboard_view()
        self.create_skp_view()
        self.create_bc_cd_de_view()
        self.create_level0_view()
        self.create_kybe_view()
        
        # Auto-analysis on tab change
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)
        
        # Auto-load data on startup
        self.root.after(500, self.auto_load_on_startup)
        
    def auto_load_on_startup(self):
        """Automatically load data when app starts"""
        self.progress_label.config(text="🔄 Tự động tải dữ liệu...")
        self.load_data()
        
    def _on_tab_changed(self, event):
        """Trigger analysis when switching tabs automatically."""
        tab_idx = self.notebook.index(self.notebook.select())
        text = self.notebook.tab(tab_idx, "text")
        
        if text in ["DE", "CD", "BC"]:
            curr_bt = 0
            try: curr_bt = self.backtest_var.get()
            except: pass
            if self.last_rendered_matrix_day.get(text) != curr_bt:
                self.render_matrix_unified(text)
        elif text == "🧠 Kybe - Grok":
            self.render_kybe_analysis()
        elif text == "📊 BC-CD-DE":
            self.render_bc_cd_de()
        elif text == "📊 Tổng & Chạm 3D":
            self.render_tong_cham()
        elif text == "📊 Tổng & Chạm 4D":
            self.render_tong_cham_4d()
        elif text == "🎯 Phân Tích Đa Chiều":
            self.scan_two_way()
        elif text == "📊 Tần Suất":
            self.render_frequency_tables()
        elif text == "📉 Bệt Chạm ĐB":
            self.render_bet_cham_analysis()
        elif text == "🔮 Dự Đoán Web Style":
            self.render_web_dashboard()
        elif text == "🔍 Soi Cầu Phoi":
            if hasattr(self, 'skp_df') and self.skp_df is not None:
                self.skp_update_patterns()
            else:
                if hasattr(self, 'skp_load_data'): self.skp_load_data()
        elif text == "🎯 Lọc Mức 0 (Nhị Hợp)":
            if hasattr(self, 'render_level0_analysis'): self.render_level0_analysis()

    def on_setting_change(self):
        """Called when user changes source, mode, or prize selection"""
        # Clear all user selections to avoid invalid data mix
        for d in [self.manual_selected_vars, self.manual_selected_cd_vars, self.manual_selected_bc_vars, 
                  self.bc_cd_de_vars, getattr(self, 'level0_check_vars', {})]:
            for var in d.values():
                var.set(False)

        if self.master_data or self.station_data:  # Only update if data is already loaded
            self.render_matrix_unified("DE")
            self.render_matrix_unified("CD")
            self.render_matrix_unified("BC")
            self.render_tong_cham()
            self.render_tong_cham_4d()
            self.update_selector_results()
            self.render_bet_cham_analysis()
            self.render_frequency_tables()
            self.render_web_dashboard()
            if hasattr(self, 'render_level0_analysis'): self.render_level0_analysis()
            
    def on_backtest_change(self):
        """Called when backtest days changes. Now uses debouncing and lazy loading."""
        # Clear all user selections to avoid invalid data mix
        for d in [self.manual_selected_vars, self.manual_selected_cd_vars, self.manual_selected_bc_vars, 
                  self.bc_cd_de_vars, getattr(self, 'level0_check_vars', {})]:
            for var in d.values():
                var.set(False)

        if self._backtest_timer:
            self.root.after_cancel(self._backtest_timer)
        self._backtest_timer = self.root.after(200, self._perform_lazy_backtest_render)

    def _perform_lazy_backtest_render(self):
        """Actually perform the render after debounce timer expires."""
        try:
            if self.master_data or self.station_data:
                curr_bt = 0
                try: curr_bt = self.backtest_var.get()
                except: pass
                
                # Update non-matrix primary tabs (they might still be heavy but usually not as much)
                self.render_tong_cham()
                self.render_tong_cham_4d()
                self.update_selector_results()
                self.render_bet_cham_analysis()
                self.render_frequency_tables()
                self.render_web_dashboard()
                self.skp_update_patterns()
                if hasattr(self, 'render_level0_analysis'): self.render_level0_analysis()
                
                # Mark matrix tabs as out of date
                for k in self.last_rendered_matrix_day:
                    self.last_rendered_matrix_day[k] = None
                
                # Render ONLY the active tab if it's a matrix tab
                tab_idx = self.notebook.index(self.notebook.select())
                text = self.notebook.tab(tab_idx, "text")
                if text in ["DE", "CD", "BC"]:
                    self.render_matrix_unified(text)
                    
        except Exception:
            pass

    
    def on_region_change(self):
        """Update Thứ/Đài/Giải dropdowns based on selected region."""
        region = self.region_var.get()
        
        if region == "Miền Bắc":
            # Disable MN/MT controls
            self.day_combo.config(state='disabled')
            self.station_combo.config(state='disabled')
            # Reset prize options for MB (no G8)
            self.prize_combo['values'] = ["XSMB (ĐB)", "Giải Nhất", "Giải 7", "Giải 6"]
            self.prize_var.set("XSMB (ĐB)")
            # Clear station data
            self.station_data = []
        else:
            # Enable MN/MT controls
            self.day_combo.config(state='readonly')
            self.day_combo['values'] = ["Tất cả", "Chủ Nhật", "Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7"]
            
            # Add G8 for MN/MT
            self.prize_combo['values'] = ["ĐB", "Giải Nhất", "Giải 6", "Giải 7", "Giải 8"]
            self.prize_var.set("ĐB")
            # Trigger day change to update stations
            self.on_day_change()
            
            # Auto-sync "Phân Tích Đa Chiều" tab
            if hasattr(self, 'two_way_direction'):
                # Set direction to MB → MN/MT
                self.two_way_direction.set("MB")
                # Set source region based on selected region
                if hasattr(self, 'two_way_region_mb'):
                    if region == "Miền Nam":
                        self.two_way_region_mb.set("Miền Nam")
                    elif region == "Miền Trung":
                        self.two_way_region_mb.set("Miền Trung")
                # Update UI
                self.update_two_way_ui()
                # Auto-scan with retry mechanism to ensure data and dropdowns are ready
                def try_scan(retry_count=0):
                    # Check if master_data is loaded AND dropdown has values
                    if self.master_data and hasattr(self, 'two_way_date_mb') and self.two_way_date_mb.get():
                        # Everything is ready, scan now
                        self.scan_two_way()
                    elif retry_count < 15:
                        # Not ready yet, retry after 300ms (max 4.5 seconds)
                        self.root.after(300, lambda: try_scan(retry_count + 1))
                    # else: give up after 4.5 seconds
                self.root.after(300, try_scan)
    
    def on_day_change(self):
        """Update Đài dropdown based on selected Miền + Thứ."""
        region = self.region_var.get()
        day = self.day_var.get()
        
        stations = get_stations_by_day(region, day)
        # Thêm "Tất cả" ở đầu danh sách cho MN/MT
        if stations:
            stations = ["Tất cả"] + stations
        self.station_combo['values'] = stations
        self.station_combo.config(state='readonly' if stations else 'disabled')
        
        if stations:
            self.station_var.set(stations[0])
            # Auto-load data if "Tất cả" or a specific station is auto-selected
            self.root.after(100, self.smart_load_data)
    
    def smart_load_data(self):
        """Smart load data based on selected region.
        
        - Miền Bắc: Load XSMB data (Điện Toán, Thần Tài, XSMB)
        - Miền Nam/Trung: Load station-specific data
        """
        # Validate days input
        try:
            days = self.days_fetch_var.get()
            if days < self.MIN_FETCH_DAYS or days > self.MAX_FETCH_DAYS:
                messagebox.showwarning(
                    "Cảnh báo", 
                    f"Số ngày phải từ {self.MIN_FETCH_DAYS}-{self.MAX_FETCH_DAYS}!"
                )
                return
        except tk.TclError:
            messagebox.showwarning("Cảnh báo", "Số ngày không hợp lệ!")
            return
        
        region = self.region_var.get()
        
        if region == "Miền Bắc":
            # Load MB data
            self.load_data()
        else:
            # Load MN/MT station data
            station = self.station_var.get()
            if not station:
                messagebox.showwarning("Cảnh báo", "Vui lòng chọn đài!")
                return
            self.load_station_data()
    
    def load_station_data(self):
        """Fetch data for selected MN/MT station."""
        station = self.station_var.get()
        if not station:
            messagebox.showwarning("Cảnh báo", "Vui lòng chọn đài!")
            return
        
        if self.is_loading:
            messagebox.showwarning("Cảnh báo", "Đang tải dữ liệu, vui lòng đợi!")
            return
        
        # Run in background thread
        if station == "Tất cả":
            region = self.region_var.get()
            day = self.day_var.get()
            thread = threading.Thread(target=self._load_all_stations_async, args=(region, day))
        else:
            thread = threading.Thread(target=self._load_station_data_async, args=(station,))
        thread.daemon = True
        thread.start()
    
    def _load_station_data_async(self, station_name: str):
        """Load station data asynchronously.
        
        Also loads MB data (Điện Toán/Thần Tài) if not already loaded,
        as it's needed as source for MN/MT matrix comparison.
        """
        self.is_loading = True
        self.root.after(0, lambda: self.progress_label.config(text=f"⏳ Đang tải dữ liệu {station_name}..."))
        
        try:
            days = self.days_fetch_var.get()
            
            # Also load MB data if not already loaded (needed for source)
            if not self.master_data:
                self.root.after(0, lambda: self.progress_label.config(
                    text=f"⏳ Tải Điện Toán/Thần Tài + {station_name}..."))
                dt_data = fetch_dien_toan(days)
                tt_data = fetch_than_tai(days)
                mb_dates, mb_all_prizes = fetch_xsmb_full(days)
                
                # Combine MB data
                self.master_data = []
                limit = min(len(dt_data), len(mb_dates), len(mb_all_prizes))
                for i in range(limit):
                    prizes = mb_all_prizes[i]
                    
                    # Extraction logic (Consistent with _load_data_async)
                    xsmb_full = prizes[0] if len(prizes) > 0 else ""
                    g1_full = prizes[1] if len(prizes) > 1 else ""
                    
                    g7_list = [prizes[j] for j in range(16, 20) if j < len(prizes) and prizes[j]]
                    g7_2so_list = [num[-2:] for num in g7_list] if g7_list else []
                    
                    g6_list = [prizes[j] for j in range(13, 16) if j < len(prizes) and prizes[j]]
                    g6_2so_list = [num[-2:] for num in g6_list] if g6_list else []
                    g6_3so_list = [num[-3:] for num in g6_list] if g6_list else []
                    
                    row_data = {
                        'date': mb_dates[i],
                        'dt_numbers': dt_data[i]['dt_numbers'],
                        'tt_number': tt_data[i]['tt_number'] if i < len(tt_data) else '',
                        'xsmb_full': xsmb_full,
                        'xsmb_2so': xsmb_full[-2:] if xsmb_full else "",
                        'xsmb_3so': xsmb_full[-3:] if xsmb_full else "",
                        'g1_full': g1_full,
                        'g1_2so': g1_full[-2:] if g1_full else "",
                        'g1_3so': g1_full[-3:] if g1_full else "",
                        'g7_list': g7_list,
                        'g7_2so': g7_2so_list,
                        'g7_3so': [],
                        'g6_list': g6_list,
                        'g6_2so': g6_2so_list,
                        'g6_3so': g6_3so_list,
                        'all_prizes': prizes
                    }
                    self.master_data.append(row_data)
            
            # Load station data
            self.station_data = fetch_station_data(station_name, days)
            
            if self.station_data:
                self.root.after(0, lambda: self.progress_label.config(text=""))
                self.root.after(100, self.render_tong_cham)
                self.root.after(150, lambda: self.render_matrix_unified("DE"))
                self.root.after(160, lambda: self.render_matrix_unified("CD"))
                self.root.after(170, lambda: self.render_matrix_unified("BC"))
                self.root.after(200, self.render_tong_cham_4d)
                self.root.after(250, self.render_bc_cd_de)
            else:
                self.root.after(0, lambda: self.progress_label.config(
                    text=f"❌ Không có dữ liệu cho {station_name}"))
        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("Lỗi", f"Không thể tải dữ liệu: {str(e)}"))
            self.root.after(0, lambda: self.progress_label.config(text="❌ Lỗi tải dữ liệu"))
        finally:
            self.is_loading = False
    
    def _load_all_stations_async(self, region: str, day: str):
        """Load all stations data for a specific day in a region asynchronously.
        
        Fetches data from stations that play on the selected day and merges by date
        for continuous day coverage (each date has one 3D result).
        """
        self.is_loading = True
        self.root.after(0, lambda: self.progress_label.config(
            text=f"⏳ Đang tải dữ liệu TẤT CẢ đài {region} - {day}..."))
        
        try:
            days = self.days_fetch_var.get()
            
            # MUST LOAD MB DATA as it's the source for matrix comparisons
            if not self.master_data:
                self.master_data = self._load_mb_data_sync()

            # Load all stations data for the selected day
            self.station_data = fetch_all_stations_data(region, day, days)
            
            if self.station_data:
                self.root.after(0, lambda: self.progress_label.config(text=""))
                self.root.after(100, self.render_tong_cham)
                self.root.after(150, lambda: self.render_matrix_unified("DE"))
                self.root.after(160, lambda: self.render_matrix_unified("CD"))
                self.root.after(170, lambda: self.render_matrix_unified("BC"))
                self.root.after(200, self.render_tong_cham_4d)
                self.root.after(250, self.render_bc_cd_de)
            else:
                self.root.after(0, lambda: self.progress_label.config(
                    text=f"❌ Không có dữ liệu cho {region} - {day}"))
        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("Lỗi", f"Không thể tải dữ liệu: {str(e)}"))
            self.root.after(0, lambda: self.progress_label.config(text="❌ Lỗi tải dữ liệu"))
        finally:
            self.is_loading = False
        
    def create_data_table(self):
        # Scrollbar
        scroll_y = ttk.Scrollbar(self.data_frame, orient=tk.VERTICAL)
        scroll_x = ttk.Scrollbar(self.data_frame, orient=tk.HORIZONTAL)
        
        # Treeview
        self.data_tree = ttk.Treeview(self.data_frame, 
                                       yscrollcommand=scroll_y.set,
                                       xscrollcommand=scroll_x.set,
                                       show='tree headings')
        
        scroll_y.config(command=self.data_tree.yview)
        scroll_x.config(command=self.data_tree.xview)
        
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
        self.data_tree.pack(fill=tk.BOTH, expand=True)
        
    def create_matrix_view(self):
        # Info label at top - Professional dark green header
        info_frame = tk.Frame(self.matrix_frame, bg="#1a472a")
        info_frame.pack(fill=tk.X)
        
        self.matrix_info_label = tk.Label(info_frame, text="🎯 Tải dữ liệu để hiển thị bảng theo dõi", 
                                          bg="#1a472a", fg="white", font=('Segoe UI', 12, 'bold'))
        self.matrix_info_label.pack(side=tk.LEFT, padx=20, pady=8)

        # Toggle Source Column (Now "Kết quả")
        self.var_show_source = tk.BooleanVar(value=False)
        self.chk_show_source = tk.Checkbutton(info_frame, text="Hiện Cột Kết quả", variable=self.var_show_source,
                                              bg="#1a472a", fg="white", selectcolor="#1a472a", activebackground="#1a472a",
                                              font=('Segoe UI', 10), command=lambda: self.render_matrix_unified("DE"))
        self.chk_show_source.pack(side=tk.RIGHT, padx=20, pady=8)

        # Labels for Top Khan mirrored from Selector tab
        self.matrix_khan_4d_label = tk.Label(info_frame, text="Khan 4D: --", 
                                             bg="#1a472a", fg="orange", font=('Segoe UI', 9, 'bold'))
        self.matrix_khan_4d_label.pack(side=tk.RIGHT, padx=10, pady=8)
        
        self.matrix_khan_3d_label = tk.Label(info_frame, text="Khan 3D: --", 
                                             bg="#1a472a", fg="gold", font=('Segoe UI', 9, 'bold'))
        self.matrix_khan_3d_label.pack(side=tk.RIGHT, padx=10, pady=8)
        
        # Main content container (Split view)
        content_container = tk.Frame(self.matrix_frame, bg=self.bg_color)
        content_container.pack(fill=tk.BOTH, expand=True)
        
        # === LEFT SIDE: MATRIX (65%) ===
        left_frame = tk.Frame(content_container, bg=self.bg_color)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Scrollable canvas for matrix
        canvas_frame = tk.Frame(left_frame, bg=self.bg_color)
        canvas_frame.pack(fill=tk.BOTH, expand=True)
        
        # 1. Vertical Scrollbar (Controls BOTH)
        self.matrix_v_scroll = ttk.Scrollbar(canvas_frame, orient=tk.VERTICAL)
        self.matrix_v_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # 2. Horizontal Scrollbar (Controls RIGHT/MAIN only)
        self.matrix_h_scroll = ttk.Scrollbar(canvas_frame, orient=tk.HORIZONTAL)
        self.matrix_h_scroll.pack(side=tk.BOTTOM, fill=tk.X)

        # 3. Fixed Canvas (Left) - ONLY "Chọn" column
        self.fixed_canvas = tk.Canvas(canvas_frame, bg="#1e1e1e", highlightthickness=0, width=40)
        self.fixed_canvas.pack(side=tk.LEFT, fill=tk.Y)

        # 4. Main Matrix Canvas (Right) - Scrollable
        self.matrix_canvas = tk.Canvas(canvas_frame, bg="#1e1e1e", highlightthickness=0)
        self.matrix_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Link Scrollbars
        self.matrix_v_scroll.config(command=self._on_matrix_vscroll)
        self.matrix_h_scroll.config(command=self.matrix_canvas.xview)
        self.matrix_canvas.configure(xscrollcommand=self.matrix_h_scroll.set)
        self.matrix_canvas.configure(yscrollcommand=self.matrix_v_scroll.set) 
        
        # Tooltip Bindings
        self.matrix_canvas.bind("<Motion>", lambda e: self._on_matrix_mouse_move(e, "DE"))
        self.matrix_canvas.bind("<Leave>", self._on_matrix_mouse_leave)
        
        self._setup_dual_canvas_mousewheel(self.fixed_canvas, self.matrix_canvas)

        # Create frames inside canvases
        self.fixed_content_frame = tk.Frame(self.fixed_canvas, bg="#1e1e1e")
        self.fixed_canvas_window = self.fixed_canvas.create_window((0, 0), window=self.fixed_content_frame, anchor='nw')
        
        self.matrix_content_frame = tk.Frame(self.matrix_canvas, bg="#1e1e1e")
        self.matrix_canvas_window = self.matrix_canvas.create_window((0, 0), window=self.matrix_content_frame, anchor='nw')
        
        # Stats frame at bottom
        self.matrix_stats_frame = tk.Frame(left_frame, bg="#333333")
        self.matrix_stats_frame.pack(fill=tk.X, padx=10, pady=5)
        self.matrix_stats_label = tk.Label(self.matrix_stats_frame, text="", bg="#333333", fg="white", font=('Segoe UI', 10, 'bold'))
        self.matrix_stats_label.pack(pady=3)
        
        # === RIGHT SIDE: STATISTICS (35%) ===
        right_frame = tk.Frame(content_container, bg="#2d2d2d", width=400)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, padx=(5, 0))
        
        # Stats Header
        stats_header_frame = tk.Frame(right_frame, bg="#333333")
        stats_header_frame.pack(fill=tk.X)
        
        tk.Label(stats_header_frame, text="📊 THỐNG KÊ CHI TIẾT", bg="#333333", fg=self.accent_color, 
                 font=('Segoe UI', 11, 'bold'), pady=5).pack(side=tk.LEFT, padx=10)

        # Toggle Number list visibility
        self.var_show_sidebar_nums = tk.BooleanVar(value=False)
        tk.Checkbutton(stats_header_frame, text="Hiện số", variable=self.var_show_sidebar_nums,
                       bg="#333333", fg="white", selectcolor="#333333", activebackground="#333333",
                       font=('Segoe UI', 9), 
                       command=lambda: self.analyze_statistics_unified("DE", getattr(self, 'matrix_days_data', []))).pack(side=tk.RIGHT, padx=5)
        
        # --- NEW: Bar Chart for "Số lần nổ" (Biên độ nổ) ---
        chart_container = tk.Frame(right_frame, bg="#1e1e1e", height=150)
        chart_container.pack(fill=tk.X, padx=5, pady=5)
        chart_container.pack_propagate(False) # Keep height fixed
        
        tk.Label(chart_container, text="Biểu đồ Số lần nổ (N1-N28)", bg="#1e1e1e", fg="#bdc3c7",
                 font=('Segoe UI', 8, 'italic')).pack(anchor='w')
                 
        self.hit_chart_canvas = tk.Canvas(chart_container, bg="#1a1a1a", highlightthickness=0)
        self.hit_chart_canvas.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)
        
        # Scrollable text area for statistics
        text_frame = tk.Frame(right_frame, bg="#1e1e1e")
        text_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Scrollbar
        stats_scrollbar = ttk.Scrollbar(text_frame)
        stats_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Text widget
        self.stats_text = tk.Text(text_frame, 
                                  wrap=tk.WORD,
                                  bg="#1e1e1e",
                                  fg="white",
                                  font=('Consolas', 10),
                                  width=40,
                                  yscrollcommand=stats_scrollbar.set,
                                  borderwidth=0,
                                  padx=10,
                                  pady=10)
        self.stats_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        stats_scrollbar.config(command=self.stats_text.yview)
        
        # Configure tags for formatting
        self.stats_text.tag_configure('title', font=('Segoe UI', 12, 'bold'), foreground=self.accent_color)
        self.stats_text.tag_configure('subtitle', font=('Segoe UI', 9, 'bold'), foreground="#f39c12")
        self.stats_text.tag_configure('level', font=('Consolas', 10, 'bold'), foreground="#e74c3c")
        self.stats_text.tag_configure('numbers', font=('Consolas', 9), foreground="#bdc3c7")
        self.stats_text.tag_configure('alert_zone', font=('Segoe UI', 10, 'bold'), foreground="#ff4b4b", background="#2d1a1a")
        
    def _on_matrix_vscroll(self, *args):
        """Sync vertical scroll for both fixed and main matrices (DE Tab)"""
        self.fixed_canvas.yview(*args)
        self.matrix_canvas.yview(*args)

    def create_matrix_cd_view(self):
        """Create view for CD tab - Cloned from Matrix (DE/BC)"""
        # Info label at top
        info_frame = tk.Frame(self.matrix_cd_frame, bg="#1a472a")
        info_frame.pack(fill=tk.X)
        
        self.matrix_cd_info_label = tk.Label(info_frame, text="🎯 Tải dữ liệu để hiển thị bảng theo dõi (CD)", 
                                          bg="#1a472a", fg="white", font=('Segoe UI', 12, 'bold'))
        self.matrix_cd_info_label.pack(side=tk.LEFT, padx=20, pady=8)

        # Toggle Source Column
        self.var_show_source_cd = tk.BooleanVar(value=False)
        self.chk_show_source_cd = tk.Checkbutton(info_frame, text="Hiện Cột Kết quả", variable=self.var_show_source_cd,
                                              bg="#1a472a", fg="white", selectcolor="#1a472a", activebackground="#1a472a",
                                              font=('Segoe UI', 10), command=lambda: self.render_matrix_unified("CD"))
        self.chk_show_source_cd.pack(side=tk.RIGHT, padx=20, pady=8)

        # Labels for Top Khan
        self.matrix_cd_khan_4d_label = tk.Label(info_frame, text="Khan 4D: --", 
                                             bg="#1a472a", fg="orange", font=('Segoe UI', 9, 'bold'))
        self.matrix_cd_khan_4d_label.pack(side=tk.RIGHT, padx=10, pady=8)
        
        self.matrix_cd_khan_3d_label = tk.Label(info_frame, text="Khan 3D: --", 
                                             bg="#1a472a", fg="gold", font=('Segoe UI', 9, 'bold'))
        self.matrix_cd_khan_3d_label.pack(side=tk.RIGHT, padx=10, pady=8)
        
        # Main content container
        content_container = tk.Frame(self.matrix_cd_frame, bg=self.bg_color)
        content_container.pack(fill=tk.BOTH, expand=True)
        
        # === LEFT SIDE: MATRIX (65%) ===
        left_frame = tk.Frame(content_container, bg=self.bg_color)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Scrollable canvas for matrix
        canvas_frame = tk.Frame(left_frame, bg=self.bg_color)
        canvas_frame.pack(fill=tk.BOTH, expand=True)
        
        # 1. Vertical Scrollbar
        self.matrix_cd_v_scroll = ttk.Scrollbar(canvas_frame, orient=tk.VERTICAL)
        self.matrix_cd_v_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # 2. Horizontal Scrollbar
        self.matrix_cd_h_scroll = ttk.Scrollbar(canvas_frame, orient=tk.HORIZONTAL)
        self.matrix_cd_h_scroll.pack(side=tk.BOTTOM, fill=tk.X)

        # 3. Fixed Canvas (Left)
        self.fixed_cd_canvas = tk.Canvas(canvas_frame, bg="#1e1e1e", highlightthickness=0, width=40)
        self.fixed_cd_canvas.pack(side=tk.LEFT, fill=tk.Y)

        # 4. Main Matrix Canvas (Right)
        self.matrix_cd_canvas = tk.Canvas(canvas_frame, bg="#1e1e1e", highlightthickness=0)
        self.matrix_cd_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Link Scrollbars
        self.matrix_cd_v_scroll.config(command=self._on_matrix_cd_vscroll)
        self.matrix_cd_h_scroll.config(command=self.matrix_cd_canvas.xview)
        self.matrix_cd_canvas.configure(xscrollcommand=self.matrix_cd_h_scroll.set)
        self.matrix_cd_canvas.configure(yscrollcommand=self.matrix_cd_v_scroll.set) 
        
        # Tooltip Bindings
        self.matrix_cd_canvas.bind("<Motion>", lambda e: self._on_matrix_mouse_move(e, "CD"))
        self.matrix_cd_canvas.bind("<Leave>", self._on_matrix_mouse_leave)
        
        self._setup_dual_canvas_mousewheel(self.fixed_cd_canvas, self.matrix_cd_canvas)

        # Create frames inside canvases
        self.fixed_cd_content_frame = tk.Frame(self.fixed_cd_canvas, bg="#1e1e1e")
        self.fixed_cd_canvas_window = self.fixed_cd_canvas.create_window((0, 0), window=self.fixed_cd_content_frame, anchor='nw')
        
        self.matrix_cd_content_frame = tk.Frame(self.matrix_cd_canvas, bg="#1e1e1e")
        self.matrix_cd_canvas_window = self.matrix_cd_canvas.create_window((0, 0), window=self.matrix_cd_content_frame, anchor='nw')
        
        # Stats frame at bottom
        self.matrix_cd_stats_frame = tk.Frame(left_frame, bg="#333333")
        self.matrix_cd_stats_frame.pack(fill=tk.X, padx=10, pady=5)
        self.matrix_cd_stats_label = tk.Label(self.matrix_cd_stats_frame, text="", bg="#333333", fg="white", font=('Segoe UI', 10, 'bold'))
        self.matrix_cd_stats_label.pack(pady=3)
        
        # === RIGHT SIDE: STATISTICS (35%) ===
        right_frame = tk.Frame(content_container, bg="#2d2d2d", width=400)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, padx=(5, 0))
        
        # Stats Header
        stats_header_frame = tk.Frame(right_frame, bg="#333333")
        stats_header_frame.pack(fill=tk.X)
        
        tk.Label(stats_header_frame, text="📊 THỐNG KÊ CHI TIẾT (CD)", bg="#333333", fg=self.accent_color, 
                 font=('Segoe UI', 11, 'bold'), pady=5).pack(side=tk.LEFT, padx=10)

        # Toggle Number list visibility
        self.var_show_sidebar_nums_cd = tk.BooleanVar(value=False)
        tk.Checkbutton(stats_header_frame, text="Hiện số", variable=self.var_show_sidebar_nums_cd,
                       bg="#333333", fg="white", selectcolor="#333333", activebackground="#333333",
                       font=('Segoe UI', 9), 
                       command=lambda: self.analyze_statistics_unified("CD", getattr(self, 'matrix_cd_days_data', []))).pack(side=tk.RIGHT, padx=5)
        
        # Bar Chart
        chart_container = tk.Frame(right_frame, bg="#1e1e1e", height=150)
        chart_container.pack(fill=tk.X, padx=5, pady=5)
        chart_container.pack_propagate(False)
        
        tk.Label(chart_container, text="Biểu đồ Số lần nổ (CD)", bg="#1e1e1e", fg="#bdc3c7",
                 font=('Segoe UI', 8, 'italic')).pack(anchor='w')
                 
        self.hit_chart_cd_canvas = tk.Canvas(chart_container, bg="#1a1a1a", highlightthickness=0)
        self.hit_chart_cd_canvas.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)
        
        # Scrollable text area
        text_frame = tk.Frame(right_frame, bg="#1e1e1e")
        text_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        stats_scrollbar = ttk.Scrollbar(text_frame)
        stats_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.stats_cd_text = tk.Text(text_frame, 
                                  wrap=tk.WORD,
                                  bg="#1e1e1e",
                                  fg="white",
                                  font=('Consolas', 10),
                                  width=40,
                                  yscrollcommand=stats_scrollbar.set,
                                  borderwidth=0,
                                  padx=10,
                                  pady=10)
        self.stats_cd_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        stats_scrollbar.config(command=self.stats_cd_text.yview)
        
        # Configure tags
        self.stats_cd_text.tag_configure('title', font=('Segoe UI', 12, 'bold'), foreground=self.accent_color)
        self.stats_cd_text.tag_configure('subtitle', font=('Segoe UI', 9, 'bold'), foreground="#f39c12")
        self.stats_cd_text.tag_configure('level', font=('Consolas', 10, 'bold'), foreground="#e74c3c")
        self.stats_cd_text.tag_configure('numbers', font=('Consolas', 9), foreground="#bdc3c7")
        self.stats_cd_text.tag_configure('alert_zone', font=('Segoe UI', 10, 'bold'), foreground="#ff4b4b", background="#2d1a1a")

    def _on_matrix_cd_vscroll(self, *args):
        """Sync vertical scroll for both fixed and main matrices (CD Tab)"""
        self.fixed_cd_canvas.yview(*args)
        self.matrix_cd_canvas.yview(*args)



    def create_matrix_bc_view(self):
        """Create view for BC tab - Cloned from Matrix (DE)"""
        # Info label at top
        info_frame = tk.Frame(self.matrix_bc_frame, bg="#1a472a")
        info_frame.pack(fill=tk.X)
        
        self.matrix_bc_info_label = tk.Label(info_frame, text="🎯 Tải dữ liệu để hiển thị bảng theo dõi (BC)", 
                                          bg="#1a472a", fg="white", font=('Segoe UI', 12, 'bold'))
        self.matrix_bc_info_label.pack(side=tk.LEFT, padx=20, pady=8)

        # Toggle Source Column
        self.var_show_source_bc = tk.BooleanVar(value=False)
        self.chk_show_source_bc = tk.Checkbutton(info_frame, text="Hiện Cột Kết quả", variable=self.var_show_source_bc,
                                              bg="#1a472a", fg="white", selectcolor="#1a472a", activebackground="#1a472a",
                                              font=('Segoe UI', 10), command=lambda: self.render_matrix_unified("BC"))
        self.chk_show_source_bc.pack(side=tk.RIGHT, padx=20, pady=8)

        # Labels for Top Khan
        self.matrix_bc_khan_4d_label = tk.Label(info_frame, text="Khan 4D: --", 
                                             bg="#1a472a", fg="orange", font=('Segoe UI', 9, 'bold'))
        self.matrix_bc_khan_4d_label.pack(side=tk.RIGHT, padx=10, pady=8)
        
        self.matrix_bc_khan_3d_label = tk.Label(info_frame, text="Khan 3D: --", 
                                             bg="#1a472a", fg="gold", font=('Segoe UI', 9, 'bold'))
        self.matrix_bc_khan_3d_label.pack(side=tk.RIGHT, padx=10, pady=8)
        
        # Main content container
        content_container = tk.Frame(self.matrix_bc_frame, bg=self.bg_color)
        content_container.pack(fill=tk.BOTH, expand=True)
        
        # === LEFT SIDE: MATRIX (65%) ===
        left_frame = tk.Frame(content_container, bg=self.bg_color)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Scrollable canvas for matrix
        canvas_frame = tk.Frame(left_frame, bg=self.bg_color)
        canvas_frame.pack(fill=tk.BOTH, expand=True)
        
        # 1. Vertical Scrollbar
        self.matrix_bc_v_scroll = ttk.Scrollbar(canvas_frame, orient=tk.VERTICAL)
        self.matrix_bc_v_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # 2. Horizontal Scrollbar
        self.matrix_bc_h_scroll = ttk.Scrollbar(canvas_frame, orient=tk.HORIZONTAL)
        self.matrix_bc_h_scroll.pack(side=tk.BOTTOM, fill=tk.X)

        # 3. Fixed Canvas (Left)
        self.fixed_bc_canvas = tk.Canvas(canvas_frame, bg="#1e1e1e", highlightthickness=0, width=40)
        self.fixed_bc_canvas.pack(side=tk.LEFT, fill=tk.Y)

        # 4. Main Matrix Canvas (Right)
        self.matrix_bc_canvas = tk.Canvas(canvas_frame, bg="#1e1e1e", highlightthickness=0)
        self.matrix_bc_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Link Scrollbars
        self.matrix_bc_v_scroll.config(command=self._on_matrix_bc_vscroll)
        self.matrix_bc_h_scroll.config(command=self.matrix_bc_canvas.xview)
        self.matrix_bc_canvas.configure(xscrollcommand=self.matrix_bc_h_scroll.set)
        self.matrix_bc_canvas.configure(yscrollcommand=self.matrix_bc_v_scroll.set) 
        
        # Tooltip Bindings
        self.matrix_bc_canvas.bind("<Motion>", lambda e: self._on_matrix_mouse_move(e, "BC"))
        self.matrix_bc_canvas.bind("<Leave>", self._on_matrix_mouse_leave)
        
        self._setup_dual_canvas_mousewheel(self.fixed_bc_canvas, self.matrix_bc_canvas)

        # Create frames inside canvases
        self.fixed_bc_content_frame = tk.Frame(self.fixed_bc_canvas, bg="#1e1e1e")
        self.fixed_bc_canvas_window = self.fixed_bc_canvas.create_window((0, 0), window=self.fixed_bc_content_frame, anchor='nw')
        
        self.matrix_bc_content_frame = tk.Frame(self.matrix_bc_canvas, bg="#1e1e1e")
        self.matrix_bc_canvas_window = self.matrix_bc_canvas.create_window((0, 0), window=self.matrix_bc_content_frame, anchor='nw')
        
        # Stats frame at bottom
        self.matrix_bc_stats_frame = tk.Frame(left_frame, bg="#333333")
        self.matrix_bc_stats_frame.pack(fill=tk.X, padx=10, pady=5)
        self.matrix_bc_stats_label = tk.Label(self.matrix_bc_stats_frame, text="", bg="#333333", fg="white", font=('Segoe UI', 10, 'bold'))
        self.matrix_bc_stats_label.pack(pady=3)
        
        # === RIGHT SIDE: STATISTICS (35%) ===
        right_frame = tk.Frame(content_container, bg="#2d2d2d", width=400)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, padx=(5, 0))
        
        # Stats Header
        stats_header_frame = tk.Frame(right_frame, bg="#333333")
        stats_header_frame.pack(fill=tk.X)
        
        tk.Label(stats_header_frame, text="📊 THỐNG KÊ CHI TIẾT (BC)", bg="#333333", fg=self.accent_color, 
                 font=('Segoe UI', 11, 'bold'), pady=5).pack(side=tk.LEFT, padx=10)

        # Toggle Number list visibility
        self.var_show_sidebar_nums_bc = tk.BooleanVar(value=False)
        tk.Checkbutton(stats_header_frame, text="Hiện số", variable=self.var_show_sidebar_nums_bc,
                       bg="#333333", fg="white", selectcolor="#333333", activebackground="#333333",
                       font=('Segoe UI', 9), 
                       command=lambda: self.analyze_statistics_unified("BC", getattr(self, 'matrix_bc_days_data', []))).pack(side=tk.RIGHT, padx=5)
        
        # Bar Chart
        chart_container = tk.Frame(right_frame, bg="#1e1e1e", height=150)
        chart_container.pack(fill=tk.X, padx=5, pady=5)
        chart_container.pack_propagate(False)
        
        tk.Label(chart_container, text="Biểu đồ Số lần nổ (BC)", bg="#1e1e1e", fg="#bdc3c7",
                 font=('Segoe UI', 8, 'italic')).pack(anchor='w')
                 
        self.hit_chart_bc_canvas = tk.Canvas(chart_container, bg="#1a1a1a", highlightthickness=0)
        self.hit_chart_bc_canvas.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)
        
        # Scrollable text area
        text_frame = tk.Frame(right_frame, bg="#1e1e1e")
        text_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        stats_scrollbar = ttk.Scrollbar(text_frame)
        stats_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.stats_bc_text = tk.Text(text_frame, 
                                  wrap=tk.WORD,
                                  bg="#1e1e1e",
                                  fg="white",
                                  font=('Consolas', 10),
                                  width=40,
                                  yscrollcommand=stats_scrollbar.set,
                                  borderwidth=0,
                                  padx=10,
                                  pady=10)
        self.stats_bc_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        stats_scrollbar.config(command=self.stats_bc_text.yview)
        
        # Configure tags
        self.stats_bc_text.tag_configure('title', font=('Segoe UI', 12, 'bold'), foreground=self.accent_color)
        self.stats_bc_text.tag_configure('subtitle', font=('Segoe UI', 9, 'bold'), foreground="#f39c12")
        self.stats_bc_text.tag_configure('level', font=('Consolas', 10, 'bold'), foreground="#e74c3c")
        self.stats_bc_text.tag_configure('numbers', font=('Consolas', 9), foreground="#bdc3c7")
        self.stats_bc_text.tag_configure('alert_zone', font=('Segoe UI', 10, 'bold'), foreground="#ff4b4b", background="#2d1a1a")

    def _on_matrix_bc_vscroll(self, *args):
        """Sync vertical scroll for both fixed and main matrices (BC Tab)"""
        self.fixed_bc_canvas.yview(*args)
        self.matrix_bc_canvas.yview(*args)

    def _setup_dual_canvas_mousewheel(self, canvas1, canvas2):
        def _on_mousewheel(event):
            delta = int(-1 * (event.delta / 120))
            canvas1.yview_scroll(delta, "units")
            canvas2.yview_scroll(delta, "units")
            return "break"

        for widget in [canvas1, canvas2]:
             widget.bind("<Enter>", lambda e, w=widget: w.bind_all("<MouseWheel>", _on_mousewheel))
             widget.bind("<Leave>", lambda e, e2=None: self.root.unbind_all("<MouseWheel>"))


    def create_tong_cham_view(self):
        """Tạo view cho tab Tổng & Chạm 3 Càng"""
        # Info label at top
        info_frame = tk.Frame(self.tong_cham_frame, bg="#1a472a")
        info_frame.pack(fill=tk.X)
        
        self.tong_cham_info_label = tk.Label(info_frame, 
                                              text="📊 THEO DÕI TỔNG & CHẠM 3 CÀNG - Tải dữ liệu để hiển thị", 
                                              bg="#1a472a", fg="white", font=('Segoe UI', 12, 'bold'))
        self.tong_cham_info_label.pack(pady=8)
        
        # Main container with 3 panels: CHẠM | TỔNG | DỰ ĐOÁN
        main_container = tk.Frame(self.tong_cham_frame, bg="#1e1e1e")
        main_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # === LEFT SIDE: CHẠM 3D ===
        left_frame = tk.LabelFrame(main_container, text="CHẠM 3D", font=('Segoe UI', 10, 'bold'),
                                   bg="#1e1e1e", fg="#e74c3c")
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 2))
        
        # Canvas for scrollable Chạm table
        cham_canvas_frame = tk.Frame(left_frame, bg="#1e1e1e")
        cham_canvas_frame.pack(fill=tk.BOTH, expand=True)
        
        self.cham_canvas = tk.Canvas(cham_canvas_frame, bg="#1e1e1e", highlightthickness=0)
        cham_v_scroll = tk.Scrollbar(cham_canvas_frame, orient=tk.VERTICAL, command=self.cham_canvas.yview)
        cham_h_scroll = tk.Scrollbar(cham_canvas_frame, orient=tk.HORIZONTAL, command=self.cham_canvas.xview)
        
        self.cham_canvas.configure(yscrollcommand=cham_v_scroll.set, xscrollcommand=cham_h_scroll.set)
        
        cham_v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        cham_h_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        self.cham_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._setup_canvas_mousewheel(self.cham_canvas)
        
        # self.cham_content_frame = tk.Frame(self.cham_canvas, bg="white")
        # self.cham_canvas.create_window((0, 0), window=self.cham_content_frame, anchor='nw')
        
        # Chạm stats label
        self.cham_stats_label = tk.Label(left_frame, text="", bg="#2d2d2d", 
                                          font=('Segoe UI', 8, 'bold'), fg="#ecf0f1")
        self.cham_stats_label.pack(fill=tk.X, pady=1)
        
        # === MIDDLE: TỔNG 3D ===
        middle_frame = tk.LabelFrame(main_container, text="TỔNG 3D", font=('Segoe UI', 10, 'bold'),
                                    bg="#1e1e1e", fg="#3498db")
        middle_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=2)
        
        # Canvas for scrollable Tổng table
        tong_canvas_frame = tk.Frame(middle_frame, bg="#1e1e1e")
        tong_canvas_frame.pack(fill=tk.BOTH, expand=True)
        
        self.tong_canvas = tk.Canvas(tong_canvas_frame, bg="#1e1e1e", highlightthickness=0)
        tong_v_scroll = tk.Scrollbar(tong_canvas_frame, orient=tk.VERTICAL, command=self.tong_canvas.yview)
        tong_h_scroll = tk.Scrollbar(tong_canvas_frame, orient=tk.HORIZONTAL, command=self.tong_canvas.xview)
        
        self.tong_canvas.configure(yscrollcommand=tong_v_scroll.set, xscrollcommand=tong_h_scroll.set)
        
        tong_v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        tong_h_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        self.tong_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._setup_canvas_mousewheel(self.tong_canvas)
        
        # self.tong_content_frame = tk.Frame(self.tong_canvas, bg="white")
        # self.tong_canvas.create_window((0, 0), window=self.tong_content_frame, anchor='nw')
        
        # Tổng stats label
        self.tong_stats_label = tk.Label(middle_frame, text="", bg="#2d2d2d",
                                          font=('Segoe UI', 8, 'bold'), fg="#ecf0f1")
        self.tong_stats_label.pack(fill=tk.X, pady=1)
        
        # === RIGHT SIDE: DỰ ĐOÁN + THỐNG KÊ TẦN SUẤT ===
        pred_frame = tk.LabelFrame(main_container, text="DỰ ĐOÁN 3 CÀNG", font=('Segoe UI', 10, 'bold'),
                                   bg="#1a472a", fg="white", width=500)
        pred_frame.pack(side=tk.RIGHT, fill=tk.BOTH, padx=(2, 0))
        pred_frame.pack_propagate(False)
        
        # Section 1: Dàn Chạm
        sec1 = tk.Frame(pred_frame, bg="#1a472a")
        sec1.pack(fill=tk.BOTH, expand=True, pady=(0, 2))
        tk.Label(sec1, text="🎯 DÀN CHẠM:", bg="#1a472a", fg="#f39c12",
                font=('Segoe UI', 9, 'bold')).pack(anchor='w', padx=5, pady=(2,0))
        self.cham_pred_text = scrolledtext.ScrolledText(sec1, wrap=tk.WORD, height=1,
                                       bg="#1e1e1e", fg="white", font=('Consolas', 9),
                                       borderwidth=0, padx=5, pady=5)
        self.cham_pred_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=2)
        
        # Section 2: Dàn Tổng
        sec2 = tk.Frame(pred_frame, bg="#1a472a")
        sec2.pack(fill=tk.BOTH, expand=True, pady=2)
        tk.Label(sec2, text="🎯 DÀN TỔNG:", bg="#1a472a", fg="#3498db",
                font=('Segoe UI', 9, 'bold')).pack(anchor='w', padx=5, pady=(2,0))
        self.tong_pred_text = scrolledtext.ScrolledText(sec2, wrap=tk.WORD, height=1,
                                       bg="#1e1e1e", fg="white", font=('Consolas', 9),
                                       borderwidth=0, padx=5, pady=5)
        self.tong_pred_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=2)
        
        # Section 3: Rare Hundreds
        sec3 = tk.Frame(pred_frame, bg="#1a472a")
        sec3.pack(fill=tk.BOTH, expand=True, pady=(2, 0))
        rare_header = tk.Frame(sec3, bg="#1a472a")
        rare_header.pack(fill=tk.X, padx=5, pady=(2,0))
        tk.Label(rare_header, text="🎯 THỐNG KÊ ĐẦU (RARE):", bg="#1a472a", fg="#2ecc71",
                font=('Segoe UI', 9, 'bold')).pack(side=tk.LEFT)
                
        self.rare_mode_3d = tk.StringVar(value="Khan (Lâu chưa ra)")
        mode_cb = ttk.Combobox(rare_header, textvariable=self.rare_mode_3d, state='readonly', width=20, font=('Segoe UI', 8))
        mode_cb['values'] = ["Khan (Lâu chưa ra)", "Mới (Vừa ra)"]
        mode_cb.pack(side=tk.RIGHT)
        mode_cb.bind("<<ComboboxSelected>>", lambda e: self.render_tong_cham())
        
        self.rare_hundreds_text = scrolledtext.ScrolledText(sec3, wrap=tk.WORD, height=1,
                                           bg="#1e1e1e", fg="#bdc3c7", font=('Consolas', 8),
                                           borderwidth=0, padx=5, pady=5)
        self.rare_hundreds_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=2)
        
        self.rare_hundreds_text.tag_configure('header', foreground="#f1c40f", font=('Segoe UI', 9, 'bold'))
        self.rare_hundreds_text.tag_configure('highlight', foreground="#e74c3c", font=('Consolas', 8, 'bold'))
        
        
    def load_data(self):
        if self.is_loading:
            messagebox.showwarning("Cảnh báo", "Đang tải dữ liệu, vui lòng đợi!")
            return
            
        # Run in background thread
        thread = threading.Thread(target=self._load_data_async)
        thread.daemon = True
        thread.start()
        
    def _load_data_async(self):
        """Load data asynchronously in background thread."""
        self.is_loading = True
        # Use after() instead of update() for thread safety
        self.root.after(0, lambda: self.progress_label.config(text="⏳ Đang tải dữ liệu..."))
        
        try:
            days = self.days_fetch_var.get()
            
            # Fetch data
            dt_data = fetch_dien_toan(days)
            tt_data = fetch_than_tai(days)
            mb_dates, mb_all_prizes = fetch_xsmb_full(days)
            
            # Combine data
            self.master_data = []
            
            limit = min(len(dt_data), len(mb_dates), len(mb_all_prizes))
            
            for i in range(limit):
                prizes = mb_all_prizes[i]
                
                # Extract specific prizes for backward compatibility
                xsmb_full = prizes[0] if len(prizes) > 0 else ""
                g1_full = prizes[1] if len(prizes) > 1 else ""
                
                # G7 (4 numbers, 2-digit each)
                g7_list = [prizes[j] for j in range(16, 20) if j < len(prizes) and prizes[j]]
                g7_2so_list = [num[-2:] for num in g7_list if num]
                
                # G6 (3 numbers, 3-digit each)
                g6_list = [prizes[j] for j in range(13, 16) if j < len(prizes) and prizes[j]]
                g6_2so_list = [num[-2:] for num in g6_list if num]
                g6_3so_list = [num[-3:] for num in g6_list if num]
                
                row_data = {
                    'date': mb_dates[i],
                    'dt_numbers': dt_data[i]['dt_numbers'] if i < len(dt_data) else [],
                    'tt_number': tt_data[i]['tt_number'] if i < len(tt_data) else '',
                    'xsmb_full': xsmb_full,
                    'xsmb_2so': xsmb_full[-2:] if xsmb_full else '',
                    'xsmb_3so': xsmb_full[-3:] if xsmb_full else '',
                    'g1_full': g1_full,
                    'g1_2so': g1_full[-2:] if g1_full else '',
                    'g1_3so': g1_full[-3:] if g1_full else '',
                    'g7_list': g7_list,
                    'g7_2so': g7_2so_list,
                    'g7_3so': [], # G7 does not have 3 digits
                    'g6_list': g6_list,
                    'g6_2so': g6_2so_list,
                    'g6_3so': g6_3so_list,
                    'all_prizes': prizes  # Store all 27 prizes for scanning
                }
                
                self.master_data.append(row_data)
            
            # Update UI
            self.root.after(0, self._update_data_display)
            self.root.after(0, lambda: self.progress_label.config(text=""))
            
            # Auto-update matrix
            self.root.after(100, self.render_tong_cham)
            self.root.after(150, lambda: self.render_matrix_unified("DE"))
            self.root.after(160, lambda: self.render_matrix_unified("CD"))
            self.root.after(170, lambda: self.render_matrix_unified("BC"))
            self.root.after(200, self.render_tong_cham_4d)
            
        except Exception as e:
            self.root.after(0, lambda: messagebox.showerror("Lỗi", f"Không thể tải dữ liệu: {str(e)}"))
            self.root.after(0, lambda: self.progress_label.config(text="❌ Lỗi tải dữ liệu"))
        finally:
            self.is_loading = False
            
    def render_matrix_unified(self, pos_type):
        """
        Unified Render logic for DE, CD, and BC Matrix tabs using raw Canvas drawing.
        Replaces widget-based rendering (thousands of Labels) with efficient canvas items.
        """
        config = self.matrix_configs.get(pos_type)
        if not config: return
        
        region = self.region_var.get()
        is_station_data = (region != "Miền Bắc")
        source = self.source_var.get()
        mode = self.mode_var.get()
        prize = self.prize_var.get()
        mode_3d = (mode == "3D")
        show_result_col = config["show_source_var"].get() if config["show_source_var"] else False
        
        data_source = self.master_data if not is_station_data else self.station_data
        if not data_source:
            config["info_label"].config(text="⚠️ Không có dữ liệu để hiển thị")
            return
            
        fixed_canvas, matrix_canvas = config["canvases"]
        fixed_canvas.delete("all")
        matrix_canvas.delete("all")
        
        # Prepare analysis data
        try: backtest_offset = self.backtest_var.get()
        except: backtest_offset = 0
        
        data_slice = data_source[backtest_offset : backtest_offset + self.MAX_DISPLAY_DAYS]
        mb_source_by_date = {row.get('date', ''): (row.get('tt_number', '') if source == "Thần Tài" else "".join(row.get('dt_numbers', []))) 
                            for row in self.master_data} if is_station_data else {}

        all_days_data = []
        target_slice = config["slice"]
        
        for i, row in enumerate(data_slice):
            date_key = row.get('date', '')
            src_str = mb_source_by_date.get(date_key, '') if is_station_data else (row.get('tt_number', '') if source == "Thần Tài" else "".join(row.get('dt_numbers', [])))
            if not src_str: continue
            
            w = 3 if mode_3d else 2
            res_list, res_list_full = [], []
            
            if is_station_data and 'items' in row:
                for item in row['items']:
                    val = str(item.get('db_full', '') or item.get('db', '')).strip()
                    if len(val) >= abs(target_slice.start):
                        extracted = val[target_slice]
                        res_list.append(extracted)
                        res_list_full.append(extracted)
                    else:
                        res_list.extend(self.get_prize_numbers(item, width=w, tail_only=True))
                        res_list_full.extend(self.get_prize_numbers(item, width=w, tail_only=False))
            else:
                val = str(row.get('db_full', '') or row.get('db', '') or row.get('xsmb_full', '')).strip()
                if len(val) >= abs(target_slice.start):
                    extracted = val[target_slice]
                    res_list = [extracted]
                    res_list_full = [extracted]
                else:
                    res_list = self.get_prize_numbers(row, width=w, tail_only=True)
                    res_list_full = self.get_prize_numbers(row, width=w, tail_only=False)
            
            digits = set(src_str)
            if mode_3d: combos = sorted({a+b+c for a in digits for b in digits for c in digits})
            else: combos = sorted({a+b for a in digits for b in digits})
            
            all_days_data.append({
                'date': date_key, 'source': src_str, 'combos': combos, 'index': i,
                'check_result': res_list, 'check_result_full': res_list_full
            })

        if not all_days_data:
            config["info_label"].config(text="⚠️ Không có dữ liệu phù hợp")
            return
        
        # Info labels
        mode_text = "3D" if mode_3d else "2D"
        target_name = self.station_var.get() if is_station_data else "XSMB"
        pos_info = f" ({pos_type} Position)" if pos_type != "DE" else ""
        config["info_label"].config(text=f"🎯 BẢNG THEO DÕI {pos_type} ({mode_text}) - Nguồn: {source} → So sánh: {target_name} {prize}{pos_info}")

        # CONSTANTS
        ROW_H = 25
        COL_W_CB = 40
        COL_W_STT = 30
        COL_W_DATE = 50
        COL_W_SRC = 100
        COL_W_DE = 35 if config["show_de_sync"] else 0
        COL_W_RES = 160 if show_result_col else 0
        COL_W_N = 30
        Header_H = 26
        
        header_bg = self.accent_color
        
        # --- DRAW HEADERS ---
        # Fixed Canvas Header (Chọn)
        fixed_canvas.create_rectangle(0, 0, COL_W_CB, Header_H, fill=header_bg, outline="#555")
        fixed_canvas.create_text(COL_W_CB/2, Header_H/2, text="Chọn", fill="white", font=('Segoe UI', 8, 'bold'))
        
        # Matrix Canvas Header
        curr_x = 0
        def draw_h(txt, w, bg=header_bg):
            nonlocal curr_x
            matrix_canvas.create_rectangle(curr_x, 0, curr_x + w, Header_H, fill=bg, outline="#555")
            matrix_canvas.create_text(curr_x + w/2, Header_H/2, text=txt, fill="white", font=('Segoe UI', 8, 'bold'))
            curr_x += w

        draw_h("STT", COL_W_STT)
        draw_h("Ngày", COL_W_DATE)
        draw_h("Nguồn", COL_W_SRC)
        if config["show_de_sync"]: draw_h("DE", COL_W_DE, "#5B2C6F")
        if show_result_col: draw_h("Kết quả", COL_W_RES)
        
        for k in range(1, self.MAX_MATRIX_COLS + 1):
            z_bg = "#2980b9" if 1<=k<=7 else "#f39c12" if 8<=k<=14 else "#c0392b" if 15<=k<=21 else "#8e44ad"
            draw_h(f"N{k}", COL_W_N, z_bg)

        # --- DRAW DATA ROWS ---
        total_hits, total_checks = 0, 0
        for row_idx, day_data in enumerate(all_days_data):
            y = Header_H + row_idx * ROW_H
            row_bg = "#2d2d2d" if row_idx % 2 == 0 else "#1e1e1e"
            date, src_str, combos = day_data['date'], day_data['source'], day_data['combos']
            combo_key = (date, tuple(combos))
            
            # Var check
            if combo_key not in config["vars"]:
                var = tk.BooleanVar(value=False)
                var.trace_add('write', lambda *a, p=pos_type: self._on_manual_check_changed_unified(p))
                config["vars"][combo_key] = var
            
            # Hit check for CB Color
            has_hit_any = False
            num_cols_check = min(row_idx + 1, self.MAX_MATRIX_COLS)
            combos_set = set(combos)
            for k_check in range(1, num_cols_check + 1):
                t_idx = row_idx - k_check + 1
                if 0 <= t_idx < len(all_days_data):
                    for v_chk in all_days_data[t_idx]['check_result']:
                        if v_chk and v_chk in combos_set:
                            has_hit_any = True; break
                if has_hit_any: break
            
            cb_bg = "#d35400" if not has_hit_any else row_bg
            
            # 1. FIXED CANVAS - Checkbutton
            fixed_canvas.create_rectangle(0, y, COL_W_CB, y + ROW_H, fill=cb_bg, outline="#333")
            cb = tk.Checkbutton(fixed_canvas, variable=config["vars"][combo_key], 
                               bg=cb_bg, selectcolor="#1e1e1e", activebackground=cb_bg,
                               fg="white", highlightthickness=0, bd=0)
            fixed_canvas.create_window(COL_W_CB/2, y + ROW_H/2, window=cb)
            
            # 2. MATRIX CANVAS - Data Cells
            curr_x = 0
            def draw_cell(txt, w, bg=row_bg, fg="white", f_sz=9, bold=False):
                nonlocal curr_x
                matrix_canvas.create_rectangle(curr_x, y, curr_x + w, y + ROW_H, fill=bg, outline="#333")
                matrix_canvas.create_text(curr_x + w/2, y + ROW_H/2, text=txt, fill=fg, 
                                          font=('Consolas', f_sz, 'bold' if bold else 'normal'))
                curr_x += w
                
            draw_cell(str(len(all_days_data)-row_idx), COL_W_STT)
            draw_cell(date[:5], COL_W_DATE, fg="#bdc3c7", f_sz=8)
            draw_cell(src_str, COL_W_SRC, fg="#f1c40f", bold=True)
            
            # Sync DE Column
            if config["show_de_sync"]:
                de_key = (date, tuple(combos))
                if de_key not in self.manual_selected_vars:
                    v_de = tk.BooleanVar(value=False)
                    v_de.trace_add('write', lambda *a: self._on_manual_check_changed_unified("DE"))
                    self.manual_selected_vars[de_key] = v_de
                
                matrix_canvas.create_rectangle(curr_x, y, curr_x + COL_W_DE, y + ROW_H, fill="white", outline="#333")
                cb_de = tk.Checkbutton(matrix_canvas, variable=self.manual_selected_vars[de_key],
                                      bg="white", activebackground="white", selectcolor="white",
                                      highlightthickness=0, bd=0)
                matrix_canvas.create_window(curr_x + COL_W_DE/2, y + ROW_H/2, window=cb_de)
                curr_x += COL_W_DE
                
            if show_result_col:
                res_f = ", ".join(day_data['check_result_full'])
                draw_cell(res_f, COL_W_RES, fg="#2980b9", bold=True)
                
            # Matrix Cells (N1..N28)
            for k in range(1, self.MAX_MATRIX_COLS + 1):
                z_miss = "#154360" if 1<=k<=7 else "#6e4506" if 8<=k<=14 else "#511610" if 15<=k<=21 else "#4b1e52"
                z_empty = "#0e2d40" if 1<=k<=7 else "#4a2e04" if 8<=k<=14 else "#350e0a" if 15<=k<=21 else "#2e1232"
                
                if k > row_idx + 1:
                    matrix_canvas.create_rectangle(curr_x, y, curr_x + COL_W_N, y + ROW_H, fill=z_empty, outline="#222")
                    curr_x += COL_W_N
                else:
                    target_idx = row_idx - k + 1
                    hit_vals = []
                    if 0 <= target_idx < len(all_days_data):
                        res_list_cell = all_days_data[target_idx]['check_result']
                        for v in res_list_cell:
                            if v and v in combos_set: hit_vals.append(v)
                        total_checks += 1
                        if hit_vals: total_hits += 1
                    
                    is_hit = len(hit_vals) > 0
                    cell_bg = self.accent_color if is_hit else z_miss
                    cell_fg = "white" if is_hit else "#f1c40f"
                    disp_txt = ",".join(hit_vals) if is_hit and len(hit_vals) > 1 else (hit_vals[0] if is_hit else "-")
                    f_sz = 7 if is_hit and len(hit_vals) > 1 else 9
                    draw_cell(disp_txt, COL_W_N, bg=cell_bg, fg=cell_fg, f_sz=f_sz, bold=is_hit)

        # Update Scrollbars
        fixed_canvas.config(scrollregion=(0, 0, COL_W_CB, Header_H + len(all_days_data) * ROW_H))
        matrix_canvas.config(scrollregion=(0, 0, curr_x, Header_H + len(all_days_data) * ROW_H))
        
        # Stats
        ratio = (total_hits / total_checks * 100) if total_checks > 0 else 0
        if config["stats_label"]:
            config["stats_label"].config(text=f"📊 Tổng dàn ({pos_type}): {len(all_days_data)} | Kiểm tra: {total_checks} | Trúng: {total_hits} | Tỷ lệ: {ratio:.1f}%")
        
        # Update last rendered tag
        cur_backtest = 0
        try: cur_backtest = self.backtest_var.get()
        except: pass
        self.last_rendered_matrix_day[pos_type] = cur_backtest
        
        setattr(self, f"matrix_{pos_type.lower()}_days_data", all_days_data)
        self.analyze_statistics_unified(pos_type, all_days_data)
        
        # Persist data for updates
        setattr(self, f"matrix_{pos_type.lower()}_days_data", all_days_data)

        # Analyze and update statistics
        self.analyze_statistics_unified(pos_type, all_days_data)

    def _on_manual_check_changed_unified(self, pos_type):
        """Unified callback for matrix checkbox changes."""
        data = getattr(self, f'matrix_{pos_type.lower()}_days_data', [])
        if data:
            self.analyze_statistics_unified(pos_type, data)

    def analyze_statistics_unified(self, pos_type, all_days_data):
        """Unified Statistics logic for DE, CD, and BC Matrix tabs."""
        config = self.matrix_configs.get(pos_type)
        if not config: return
        
        setattr(self, f"matrix_{pos_type.lower()}_days_data", all_days_data)
        refresh_flag = f"_is_refreshing_{pos_type.lower()}_stats"
        setattr(self, refresh_flag, True)

        # [SYNC] Synchronize DE selection to BC-CD-DE tab if applicable
        if pos_type == "DE":
            for combo_key, var in config["vars"].items():
                if var.get():
                    date_str = combo_key[0]
                    k = (date_str, "ALL", "DE")
                    if k in self.bc_cd_de_vars:
                        self.bc_cd_de_vars[k].set(True)
        
        # Clear previous stats
        config["stats_text"].delete('1.0', tk.END)
        
        if not all_days_data:
            setattr(self, refresh_flag, False)
            config["stats_text"].insert(tk.END, "Không có dữ liệu\n")
            return
        
        # Step 1: Find all pending sets (sets with no hits)
        all_pending = []
        for row_idx, day_data in enumerate(all_days_data):
            combos = day_data['combos']
            date = day_data['date']
            
            has_hit = False
            num_cols_this_row = min(row_idx + 1, self.MAX_MATRIX_COLS)
            combos_set = set(combos)
            
            for k in range(1, num_cols_this_row + 1):
                check_idx = row_idx - k + 1
                if 0 <= check_idx < len(all_days_data):
                    check_result = all_days_data[check_idx].get('check_result', [])
                    is_hit = False
                    if isinstance(check_result, list):
                        for result_val in check_result:
                            if result_val and result_val in combos_set:
                                is_hit = True
                                break
                    else:
                        if check_result and check_result in combos_set: is_hit = True
                    
                    if is_hit:
                        has_hit = True
                        break
            
            if not has_hit:
                all_pending.append({'date': date, 'combos': combos})
        
        # Update pending vars
        current_keys = set()
        for p in all_pending:
            key = (p['date'], tuple(p['combos']))
            current_keys.add(key)
            if key not in config["pending_vars"]:
                var = tk.BooleanVar(value=False)
                var.trace_add('write', lambda *a, p=pos_type: self._on_pending_check_changed_unified(p))
                config["pending_vars"][key] = var
                
        # Cleanup old pending vars
        to_delete = [k for k in config["pending_vars"] if k not in current_keys]
        for k in to_delete: del config["pending_vars"][k]

        # Sync Sidebar to Matrix
        for key, var in config["vars"].items():
            if key in config["pending_vars"]:
                if config["pending_vars"][key].get() != var.get():
                    config["pending_vars"][key].set(var.get())
        
        # Display explanatory note
        config["stats_text"].insert(tk.END, "💡 CHÚ THÍCH: Ô màu CAM bên trái là dàn CHƯA RA.\n", 'analysis')
        config["stats_text"].insert(tk.END, "—"*24 + "\n\n")
        
        # Filter selected sets
        mode_len = 3 if self.mode_var.get() == "3D" else 2
        pending_keys = {(p['date'], tuple(p['combos'])) for p in all_pending}
        manual_selected = []
        for key, var in config["vars"].items():
            if not key[1] or len(key[1][0]) != mode_len: continue
            if var.get() and key not in pending_keys:
                manual_selected.append({'date': key[0], 'combos': list(key[1])})
        
        if manual_selected:
            config["stats_text"].insert(tk.END, f"DÀN CHỌN THỦ CÔNG {pos_type} ({len(manual_selected)}):\n", 'title')
            for idx, pset in enumerate(manual_selected, 1):
                config["stats_text"].insert(tk.END, f" M{idx}. {pset['date']} ({len(pset['combos'])}s):\n", 'subtitle')
                show_nums_var_name = f"var_show_sidebar_nums_{pos_type.lower()}"
                if pos_type == "DE": show_nums_var_name = "var_show_sidebar_nums"
                show_nums_var = getattr(self, show_nums_var_name, None)
                if show_nums_var and show_nums_var.get():
                    config["stats_text"].insert(tk.END, f"   {','.join(pset['combos'])}\n\n", 'numbers')
                else:
                    config["stats_text"].insert(tk.END, "\n")

        config["stats_text"].insert(tk.END, "\n")
        
        # Analyze frequencies (Levels)
        selected_pending = [p for p in all_pending if config["pending_vars"][(p['date'], tuple(p['combos']))].get()]
        all_selected_for_calc = selected_pending + manual_selected
        
        level_groups = defaultdict(list)
        config["stats_text"].insert(tk.END, f"MỨC SỐ {pos_type} (Tổng hợp):\n", 'title')
        
        if all_selected_for_calc:
            number_frequency = defaultdict(int)
            for pset in all_selected_for_calc:
                for num in pset['combos']:
                    number_frequency[num] += 1
            
            for num, freq in number_frequency.items():
                level_groups[freq].append(num)
            
            mode = self.mode_var.get()
            all_possible = {f"{i:03d}" for i in range(1000)} if mode == "3D" else {f"{i:02d}" for i in range(100)}
            level_0 = sorted(all_possible - set(number_frequency.keys()))
            if level_0: level_groups[0] = level_0
            
            show_nums_var_name = f"var_show_sidebar_nums_{pos_type.lower()}"
            if pos_type == "DE": show_nums_var_name = "var_show_sidebar_nums"
            show_nums_var = getattr(self, show_nums_var_name, None)
            show_nums = show_nums_var.get() if show_nums_var else False
            
            use_prefix = (pos_type == "CD" and mode == "3D" and not show_nums)
            top_digits = self._get_cd_fallback_top_digits() if use_prefix else [""]

            if use_prefix:
                config["stats_text"].insert(tk.END, f"Top Prefix CD: {', '.join(top_digits)}\n\n", 'subtitle')

            for freq in sorted(level_groups.keys(), reverse=True):
                nums = sorted(level_groups[freq])
                total_s = len(nums) * (len(top_digits) if use_prefix else 1)
                config["stats_text"].insert(tk.END, f"Mức {freq} ({total_s} số): ", 'level')
                if show_nums or not use_prefix:
                    if use_prefix:
                        joined = [f"{d}{n}" for d in top_digits for n in nums]
                        config["stats_text"].insert(tk.END, f"{', '.join(joined)}\n", 'numbers')
                    else:
                        config["stats_text"].insert(tk.END, f"{', '.join(nums)}\n", 'numbers')
                else:
                    config["stats_text"].insert(tk.END, "[Đã ẩn danh sách]\n", 'numbers')
        else:
             config["stats_text"].insert(tk.END, "Vui lòng chọn ít nhất 1 dàn!\n", 'level')

        self._render_specific_extras(pos_type, config["stats_text"], level_groups, all_days_data)
        self.calculate_hit_chart_unified(pos_type, all_days_data)
        setattr(self, refresh_flag, False)
        self.render_smart_suggestions(all_days_data, config["stats_text"], config["vars"])

    def _render_specific_extras(self, pos_type, stats_text, level_groups, all_days_data):
        if pos_type == "BC":
            stats_text.insert(tk.END, "\n" + "="*25 + "\n")
            stats_text.insert(tk.END, "GHÉP DÀN 4D (BC + DE):\n", 'title')
            selected_de_sets = []
            valid_dates = {d['date'] for d in all_days_data}
            for key, var in self.manual_selected_vars.items():
                if var.get() and key[0] in valid_dates:
                    selected_de_sets.append({'combos': list(key[1])})
            de_level_groups = defaultdict(list)
            if selected_de_sets:
                de_freq = defaultdict(int)
                for pset in selected_de_sets:
                    for num in pset['combos']: de_freq[num] += 1
                for num, freq in de_freq.items(): de_level_groups[freq].append(num)
            bc_levels = sorted(level_groups.keys(), reverse=True)
            de_levels = sorted(de_level_groups.keys(), reverse=True)
            if not de_levels:
                stats_text.insert(tk.END, "-> Chưa tích chọn cột DE để ghép.\n", 'analysis')
            elif not bc_levels:
                stats_text.insert(tk.END, "-> Chưa có dàn BC nào được chọn.\n", 'analysis')
            else:
                count_display = 0
                for bc_l in bc_levels[:2]: 
                    if bc_l == 0 and len(bc_levels) > 1: continue 
                    bc_nums = sorted(level_groups[bc_l])
                    for de_l in de_levels[:2]:
                        de_nums = sorted(de_level_groups[de_l])
                        full_4d_list = [bc + de for bc in bc_nums for de in de_nums]
                        label = f"Mức BC {bc_l} + Mức DE {de_l}"
                        stats_text.insert(tk.END, f"★ {label} ({len(full_4d_list)} số):\n", 'subtitle')
                        if full_4d_list:
                             stats_text.insert(tk.END, f"{', '.join(full_4d_list)}\n\n", 'numbers')
                        count_display += 1
                        if count_display >= 5: break
                    if count_display >= 5: break

    def calculate_hit_chart_unified(self, pos_type, all_days_data):
        config = self.matrix_configs.get(pos_type)
        if not config or not config["hit_canvas"]: return
        try:
            hit_counts = {k: 0 for k in range(1, self.MAX_MATRIX_COLS + 1)}
            for k in range(1, self.MAX_MATRIX_COLS + 1):
                for row_idx, day_data in enumerate(all_days_data):
                    check_idx = row_idx - k + 1
                    if 0 <= check_idx < len(all_days_data):
                        check_result = all_days_data[check_idx].get('check_result', [])
                        combos = day_data.get('combos', [])
                        is_hit = False
                        if isinstance(check_result, list):
                            for rv in check_result:
                                if rv and rv in combos:
                                    is_hit = True; break
                        else:
                            if check_result and check_result in combos: is_hit = True
                        if is_hit: hit_counts[k] += 1
            self.root.after(100, lambda: self.draw_hit_chart_unified(pos_type, hit_counts))
        except Exception as e:
            print(f"Error calculating hit chart {pos_type}: {e}")

    def draw_hit_chart_unified(self, pos_type, hit_counts):
        config = self.matrix_configs.get(pos_type)
        if not config or not config["hit_canvas"]: return
        canvas = config["hit_canvas"]
        canvas.delete("all")
        w = canvas.winfo_width()
        h = canvas.winfo_height()
        if w < 10: w = 400
        if h < 10: h = 120
        max_v = max(hit_counts.values()) if hit_counts.values() else 1
        if max_v == 0: max_v = 1
        bar_w = (w - 20) / self.MAX_MATRIX_COLS
        for k in range(1, self.MAX_MATRIX_COLS + 1):
            count = hit_counts.get(k, 0)
            bar_h = (count / max_v) * (h - 30)
            x1 = 10 + (k-1)*bar_w
            y1 = h - 20 - bar_h
            x2 = x1 + bar_w - 2
            y2 = h - 20
            color = "#2ecc71" if k<=7 else "#3498db" if k<=14 else "#f1c40f" if k<=21 else "#e67e22"
            canvas.create_rectangle(x1, y1, x2, y2, fill=color, outline="")
            canvas.create_text((x1+x2)/2, h - 10, text=f"N{k}", fill="#7f8c8d", font=('Segoe UI', 6))
            if count > 0:
                canvas.create_text((x1+x2)/2, y1 - 5, text=str(count), fill="white", font=('Segoe UI', 6))

    def _on_pending_check_changed_unified(self, pos_type):
        config = self.matrix_configs.get(pos_type)
        if not config: return
        is_refreshing = getattr(self, f"_is_refreshing_{pos_type.lower()}_stats", False)
        if is_refreshing: return
        for key, var in config["pending_vars"].items():
            if key in config["vars"] and config["vars"][key].get() != var.get():
                config["vars"][key].set(var.get())
        data = getattr(self, f"matrix_{pos_type.lower()}_days_data", [])
        if data:
            self.analyze_statistics_unified(pos_type, data)

    def _update_data_display(self):
        # Clear existing data
        for item in self.data_tree.get_children():
            self.data_tree.delete(item)
            
        if not self.master_data:
            return
            
        # Configure columns
        columns = ('Ngày', 'Điện Toán', 'Thần Tài', 'XSMB', 'G1')
        self.data_tree['columns'] = columns
        
        self.data_tree.column('#0', width=0, stretch=tk.NO)
        self.data_tree.column('Ngày', width=100, anchor=tk.CENTER)
        self.data_tree.column('Điện Toán', width=150, anchor=tk.CENTER)
        self.data_tree.column('Thần Tài', width=100, anchor=tk.CENTER)
        self.data_tree.column('XSMB', width=100, anchor=tk.CENTER)
        self.data_tree.column('G1', width=100, anchor=tk.CENTER)
        
        # Create headings
        for col in columns:
            self.data_tree.heading(col, text=col)
            
        # Insert data
        for row in self.master_data[:50]:  # Show first 50
            dt_str = "".join(row['dt_numbers'])
            self.data_tree.insert('', tk.END, values=(
                row['date'],
                dt_str,
                row.get('tt_number', ''),
                row['xsmb_2so'],
                row['g1_2so']
            ))
        
        # Update Two-way analysis tab dates
        self.populate_two_way_mb_dates()
        self._check_auto_scan()
        self.render_web_dashboard()
            

    def get_prize_numbers(self, row, width=3, tail_only=True):
        """Extract List of N-digit numbers from selected prize in row. 
           If tail_only=True, ensures results are exactly 'width' digits by taking the end."""
        region = self.region_var.get()
        prize = self.prize_var.get()
        is_station_data = (region != "Miền Bắc")
        
        # Determine prize prefix
        if "ĐB" in prize:
            prefix = "xsmb" if not is_station_data else "db"
        elif "Nhất" in prize or "G1" in prize:
            prefix = "g1"
        elif "8" in prize:
            prefix = "g8"
        elif "7" in prize:
            prefix = "g7"
        elif "6" in prize:
            prefix = "g6"
        else:
            prefix = "db"
            
        if tail_only:
            key = f"{prefix}_{width}so"
            if key not in row:
                if width == 3 and prefix in ["g7", "g8"]:
                     key = f"{prefix}_2so"
                else:
                     if width > 2:
                         key = f"{prefix}_full" if f"{prefix}_full" in row else (prefix if prefix in row else f"{prefix}_2so")
                     else:
                         key = f"{prefix}_full" if f"{prefix}_full" in row else (f"{prefix}_2so" if f"{prefix}_2so" in row else prefix)
        else:
            # For display purposes, prioritize the fullest version available
            key = f"{prefix}_full" if f"{prefix}_full" in row else (prefix if prefix in row else f"{prefix}_{width}so")

        val = row.get(key, '')
        
        # Normalize to list of valid digits strings
        res = []
        if isinstance(val, list):
            res = [str(v) for v in val if v and str(v).isdigit()]
        elif val and str(val).isdigit():
            res = [str(val)]
        elif val and isinstance(val, str):
            res = [v.strip() for v in val.split(',') if v.strip().isdigit()]
            
        if not tail_only:
            return res
            
        # Truncate to tail-only if requested
        final_res = []
        for s in res:
            if len(s) >= width:
                final_res.append(s[-width:])
            elif len(s) > 0:
                final_res.append(s.zfill(width))
        return final_res

    def render_tong_cham(self):
        """Render Tổng và Chạm 3 Càng tracking tables"""
        region = self.region_var.get()
        prize = self.prize_var.get()
        
        # Determine data source based on region
        if region == "Miền Bắc":
            data_source = self.master_data
            station_name = f"XSMB - {prize}"
        else:
            data_source = self.station_data
            s_name = self.station_var.get()
            if s_name == "Tất cả":
                day = self.day_var.get()
                station_name = f"{day} - TẤT CẢ {region} - {prize}"
            else:
                station_name = f"{s_name} - {prize}"
        
        if not data_source:
            return
            
        # BLOCK 3D for small prizes
        is_blocked = False
        if region == "Miền Bắc" and prize == "Giải 7":
            is_blocked = True
        elif region != "Miền Bắc" and prize == "Giải 8":
            is_blocked = True
            
        if is_blocked:
            self.tong_cham_info_label.config(text=f"⚠️ KHÔNG HỖ TRỢ CHẾ ĐỘ 3D CHO {prize.upper()} {region.upper()}", foreground="red")
            self.cham_canvas.delete("all")
            self.tong_canvas.delete("all")
            self.cham_pred_text.delete('1.0', tk.END)
            self.tong_pred_text.delete('1.0', tk.END)
            self.rare_hundreds_text.delete('1.0', tk.END)
            return

        # Update info label
        self.tong_cham_info_label.config(text=f"📊 THEO DÕI TỔNG & CHẠM 3 CÀNG - {station_name} - {len(data_source)} ngày")
        
        # Prepare data
        days_data = []
        for row in data_source:
            db_3so_list = []
            if 'items' in row:
                # Merged data (Tất cả)
                for item in row['items']:
                    db_3so_list.extend(self.get_prize_numbers(item, width=3))
            else:
                db_3so_list = self.get_prize_numbers(row, width=3)
            
            if not db_3so_list:
                continue
                
            # Filter for 3-digit only
            db_3so_list = [v for v in db_3so_list if len(v) == 3]
            if not db_3so_list: continue

            tongs = [sum(int(d) for d in num) % 10 for num in db_3so_list]
            chams = [set(int(d) for d in num) for num in db_3so_list]
            
            days_data.append({
                'date': row['date'],
                'db_3so': db_3so_list,
                'tong': tongs,
                'cham': chams
            })
        
    
        # ========== BACKTEST LOGIC ==========
        try:
            backtest_days = self.backtest_var.get()
        except:
            backtest_days = 0
            
        check_msg = ""
        if backtest_days > 0 and days_data:
            if backtest_days < len(days_data):
                # Get the result of the "Next Day" relative to the backtest date
                # Since days_data is Newest->Oldest, index [backtest_days-1] is the day AFTER the cutoff
                next_result = days_data[backtest_days - 1]
                
                # Slice the data to simulate being in the past
                days_data = days_data[backtest_days:]
                
                # Formulate the Result Check message
                res_val = ", ".join(next_result['db_3so'])
                check_msg = f" | 🔙 BACKTEST (Đến {days_data[0]['date']}) -> KQ THỰC TẾ {next_result['date']}: {res_val}"
                
                # Highlight this message
                self.tong_cham_info_label.config(text=f"📊 THEO DÕI TỔNG & CHẠM 3 CÀNG {check_msg}", foreground="#ff4b4b")
            else:
                self.tong_cham_info_label.config(text=f"📊 BACKTEST: Không đủ dữ liệu để lùi {backtest_days} ngày", foreground="red")
                return
        else:
            # Restore normal label
            self.tong_cham_info_label.config(text=f"📊 THEO DÕI TỔNG & CHẠM 3 CÀNG - {station_name} - {len(data_source)} ngày", foreground="white")

        if not days_data:
            return
            
        # Colors - Professional Dark Theme
        header_bg = "#1a472a"  # Accent Green
        hit_bg = "#27ae60"    # Green for hit
        miss_bg = "#c0392b"   # Red for high gan
        normal_bg = "#1e1e1e" # Dark background
        highlight_bg = "#2d2d2d" # Slightly lighter for data rows
        
        # Pass original order (oldest first) for gan calculation
        # The render methods will calculate gan oldest→newest, then display newest at top
        
        # ========== RENDER CHẠM 3D TABLE ==========
        # ========== RENDER CHẠM 3D TABLE ==========
        self._render_cham_table(days_data, header_bg, hit_bg, miss_bg, normal_bg, highlight_bg)
        
        # ========== RENDER TỔNG 3D TABLE ==========
        self._render_tong_table(days_data, header_bg, hit_bg, miss_bg, normal_bg, highlight_bg)
        
        # ========== RENDER RARE HUNDREDS ==========
        self._render_rare_hundreds(days_data)

        # ========== CALCULATE 2D PREDICTIONS ==========
        self._calculate_2d_predictions(days_data)
        
        # ========== CALCULATE GROUP GAPS ==========
        self._calculate_group_gaps(days_data)
        
        # Update selector tab
        cham_3d_data = (self.cham_gaps_3d, self.exceeding_cham) if hasattr(self, 'cham_gaps_3d') else None
        tong_3d_data = (self.tong_gaps_3d, self.exceeding_tong) if hasattr(self, 'tong_gaps_3d') else None
        cham_2d_data = (self.cham_gaps_2d, self.exceeding_cham_2d) if hasattr(self, 'cham_gaps_2d') else None
        tong_2d_data = (self.tong_gaps_2d, self.exceeding_tong_2d) if hasattr(self, 'tong_gaps_2d') else None
        
        # Calculate Top Khan for Hàng Trăm (hundreds digit)
        tram_gaps = {i: 0 for i in range(10)}
        for day in reversed(days_data):
            # Increment gap for all digits first
            for digit in range(10):
                tram_gaps[digit] += 1
            # Reset gap to 0 for digits that appeared
            for num in day['db_3so']:
                hundreds_digit = int(num[0])
                tram_gaps[hundreds_digit] = 0
        
        # Get top 5 khan (highest gaps)
        sorted_tram = sorted(tram_gaps.items(), key=lambda x: x[1], reverse=True)
        top_khan_tram = tuple([d for d, g in sorted_tram[:5]])
        
        # Prepare data with hits of today (gap 0)
        c3_hits = [i for i, g in self.cham_gaps_3d.items() if g == 0] if hasattr(self, 'cham_gaps_3d') else []
        t3_hits = [i for i, g in self.tong_gaps_3d.items() if g == 0] if hasattr(self, 'tong_gaps_3d') else []
        c2_hits = [i for i, g in self.cham_gaps_2d.items() if g == 0] if hasattr(self, 'cham_gaps_2d') else []
        t2_hits = [i for i, g in self.tong_gaps_2d.items() if g == 0] if hasattr(self, 'tong_gaps_2d') else []
        h2_hits = [i for i, g in self.hieu_gaps_2d.items() if g == 0] if hasattr(self, 'hieu_gaps_2d') else []
        tr_hits = [i for i, g in tram_gaps.items() if g == 0]

        self.update_rare_info_on_selector(
            khan_3d_str=(tram_gaps, top_khan_tram, tr_hits),
            cham_3d=(self.cham_gaps_3d, self.exceeding_cham, c3_hits) if hasattr(self, 'cham_gaps_3d') else None,
            tong_3d=(self.tong_gaps_3d, self.exceeding_tong, t3_hits) if hasattr(self, 'tong_gaps_3d') else None,
            cham_2d=(self.cham_gaps_2d, self.exceeding_cham_2d, c2_hits) if hasattr(self, 'cham_gaps_2d') else None,
            tong_2d=(self.tong_gaps_2d, self.exceeding_tong_2d, t2_hits) if hasattr(self, 'tong_gaps_2d') else None,
            hieu_2d=(self.hieu_gaps_2d, self.exceeding_hieu_2d, h2_hits) if hasattr(self, 'hieu_gaps_2d') else None
        )
        
        # Update group tracking suggestion text
        self._update_group_suggestions()
        self.update_3d_kep_stats(days_data)

    def _update_group_suggestions(self):
        """Update suggestion text for 2D group tracking rows"""
        def update_widget(widget, top_khan_list):
            if not widget: return
            widget.config(state='normal')
            widget.delete('1.0', tk.END)
            if not top_khan_list:
                widget.config(state='disabled')
                return
            
            # Format: "01(15), K.Bang(12)..." - group(gap days)
            for i, (name, gap) in enumerate(top_khan_list[:5]):
                tag = 'rare' if gap >= 20 else 'normal'
                widget.insert(tk.END, f"{name}({gap})", tag)
                if i < len(top_khan_list[:5]) - 1:
                    widget.insert(tk.END, ", ", 'normal')
            widget.config(state='disabled')
        
        # Update 2D Group Suggestions
        if hasattr(self, 'sugg_2d_bo'): update_widget(self.sugg_2d_bo, getattr(self, 'top_khan_bo', []))
        if hasattr(self, 'sugg_2d_kep'): update_widget(self.sugg_2d_kep, getattr(self, 'top_khan_kep', []))
        if hasattr(self, 'sugg_2d_zodiac'): update_widget(self.sugg_2d_zodiac, getattr(self, 'top_khan_zodiac', []))

    def _calculate_2d_predictions(self, days_data):
        """Calculate gaps and exceeding sets for 2D Chạm and Tổng"""
        if not days_data: return

        # Reverse to process oldest to newest for gap calculation
        days_ordered = list(reversed(days_data))
        
        # Initialize
        c_gaps = {i: 0 for i in range(10)}
        t_gaps = {i: 0 for i in range(10)}
        h_gaps = {i: 0 for i in range(10)}
        c_max_gaps = {i: 0 for i in range(10)}
        t_max_gaps = {i: 0 for i in range(10)} 
        h_max_gaps = {i: 0 for i in range(10)}

        for day in days_ordered:
            # Get 2D tails from all 3D/4D results of the day
            tails = []
            db_list = day.get('db_3so') or day.get('db_4so') or []
            for val in db_list:
                if len(val) >= 2:
                    tails.append(val[-2:])
            
            # Determine hits for this day
            c_hits = set()
            t_hits = set()
            h_hits = set()
            
            for tail in tails:
                # Cham hits
                for char in tail:
                    if char.isdigit():
                        c_hits.add(int(char))
                
                # Tong hits
                if tail.isdigit():
                    t_val = (int(tail[0]) + int(tail[1])) % 10
                    t_hits.add(t_val)
                    
                    # Hieu hits
                    h_val = hieu(tail)
                    if h_val != -1:
                        h_hits.add(h_val)
            
            # Update Gaps
            for i in range(10):
                # Cham
                if i in c_hits:
                    c_gaps[i] = 0
                else:
                    c_gaps[i] += 1
                    if c_gaps[i] > c_max_gaps[i]: c_max_gaps[i] = c_gaps[i]
                
                # Tong
                if i in t_hits:
                    t_gaps[i] = 0
                else:
                    t_gaps[i] += 1
                    if t_gaps[i] > t_max_gaps[i]: t_max_gaps[i] = t_gaps[i]

                # Hieu
                if i in h_hits:
                    h_gaps[i] = 0
                else:
                    h_gaps[i] += 1
                    if h_gaps[i] > h_max_gaps[i]: h_max_gaps[i] = h_gaps[i]

        # Calculate Average and identify Exceeding (Khan)
        # Threshold: > Average Max Gap / 2 ? Or just use constants?
        # Creating simple average from max gaps
        c_exceeding = []
        for i in range(10):
            avg = c_max_gaps[i] / 2 if c_max_gaps[i] > 0 else 5
            if c_gaps[i] >= avg:
                c_exceeding.append(i)
                
        t_exceeding = []
        for i in range(10):
            avg = t_max_gaps[i] / 2 if t_max_gaps[i] > 0 else 5
            if t_gaps[i] >= avg:
                t_exceeding.append(i)

        h_exceeding = []
        for i in range(10):
            avg = h_max_gaps[i] / 2 if h_max_gaps[i] > 0 else 5
            if h_gaps[i] >= avg:
                h_exceeding.append(i)
        
        self.cham_gaps_2d = c_gaps
        self.tong_gaps_2d = t_gaps
        self.hieu_gaps_2d = h_gaps
        self.exceeding_cham_2d = c_exceeding
        self.exceeding_tong_2d = t_exceeding
        self.exceeding_hieu_2d = h_exceeding
    
    def _calculate_group_gaps(self, days_data):
        """Calculate gaps for Bộ, Kép, Zodiac groups"""
        if not days_data: return
        self.bo_gaps = {k: 0 for k in BO_DICT.keys()}
        self.kep_gaps = {k: 0 for k in KEP_DICT.keys()}
        self.zodiac_gaps = {k: 0 for k in ZODIAC_DICT.keys()}
        found_bo, found_kep, found_zodiac = set(), set(), set()
        for i, day in enumerate(days_data):
            # Extract 2D tails from 特别奖 (Standard for 2D analysis)
            tails = []
            if 'db_3so' in day:
                for val in day['db_3so']:
                    if len(val) >= 2: tails.append(val[-2:])
            elif 'db' in day:
                val = str(day['db'])
                if len(val) >= 2: tails.append(val[-2:])
            
            for num in tails:
                b_key = bo(num); k_key = kep(num); z_key = zodiac(num)
                if b_key in self.bo_gaps and b_key not in found_bo:
                    self.bo_gaps[b_key] = i; found_bo.add(b_key)
                if k_key in self.kep_gaps and k_key not in found_kep:
                    self.kep_gaps[k_key] = i; found_kep.add(k_key)
                if z_key in self.zodiac_gaps and z_key not in found_zodiac:
                    self.zodiac_gaps[z_key] = i; found_zodiac.add(z_key)
        m_g = len(days_data)
        for k in self.bo_gaps:
            if k not in found_bo: self.bo_gaps[k] = m_g
        for k in self.kep_gaps:
            if k not in found_kep: self.kep_gaps[k] = m_g
        for k in self.zodiac_gaps:
            if k not in found_zodiac: self.zodiac_gaps[k] = m_g
        self.top_khan_bo = sorted(self.bo_gaps.items(), key=lambda x: x[1], reverse=True)
        self.top_khan_kep = sorted(self.kep_gaps.items(), key=lambda x: x[1], reverse=True)
        self.top_khan_zodiac = sorted(self.zodiac_gaps.items(), key=lambda x: x[1], reverse=True)
        
    def _render_cham_table(self, days_data, header_bg, hit_bg, miss_bg, normal_bg, highlight_bg):
        """Render Chạm 3D table using Canvas for performance"""
        canvas = self.cham_canvas
        canvas.delete("all")
        
        # Dimensions
        CELL_W = 24
        CELL_H = 25
        DATE_W = 60
        COL_DATE = 0
        COL_START_NUM = DATE_W
        COL_MAX = DATE_W + 10 * CELL_W
        COL_DANG = COL_MAX + 35
        COL_DB = COL_DANG + 35
        TOTAL_W = COL_DB + 60
        
        FONT_HEADER = ('Segoe UI', 8, 'bold')
        FONT_NUM = ('Consolas', 9)
        FONT_NUM_BOLD = ('Consolas', 9, 'bold')
        
        # Helper to draw cell
        def draw_cell(x, y, w, h, text, bg, fg, font=FONT_NUM):
            canvas.create_rectangle(x, y, x + w, y + h, fill=bg, outline='#dcdcdc')
            canvas.create_text(x + w/2, y + h/2, text=text, fill=fg, font=font)

        # IMPORTANT: days_data comes newest-first from API, need to reverse for correct gan calculation
        days_data_ordered = list(reversed(days_data))  # Now oldest first
        
        # PRE-CALCULATION
        cham_gan = {i: 0 for i in range(10)}
        cham_max_gan = {i: 0 for i in range(10)}
        row_results = []
        
        for day in days_data_ordered:
            row_cham_hits_list = day['cham'] # This is now a LIST of sets
            
            # Combine all cham hits for this day into one big set for easy lookup
            combined_cham_hits = set()
            for s in row_cham_hits_list:
                combined_cham_hits.update(s)
            
            # Format DB display (join with newline if multiple)
            db_display = "\n".join(day['db_3so'])
            
            row_data = {
                'date': day['date'],
                'db_3so': db_display,
                'cham_hits': combined_cham_hits,
                'cells': {},
                'max_val': 0
            }
            
            for col in range(10):
                is_hit = col in combined_cham_hits
                
                if is_hit:
                    cell_text = str(cham_gan[col]) if cham_gan[col] > 0 else "0"
                    cell_bg = hit_bg
                    cham_gan[col] = 0
                else:
                    cham_gan[col] += 1
                    cell_bg = miss_bg if cham_gan[col] >= self.GAN_HIGH_THRESHOLD else normal_bg
                    cell_text = str(cham_gan[col])
                
                if cham_gan[col] > cham_max_gan[col]:
                    cham_max_gan[col] = cham_gan[col]
                
                fg_color = "white" if is_hit or cham_gan[col] >= self.GAN_HIGH_THRESHOLD else "#bdc3c7"
                row_data['cells'][col] = {'text': cell_text, 'bg': cell_bg, 'fg': fg_color, 'is_hit': is_hit}
            
            row_data['max_val'] = max(cham_gan.values())
            row_results.append(row_data)
        
        # Calculate Average Gan and Exceeding
        avg_gan = {}
        exceeding_avg = []
        for col in range(10):
            avg_gan[col] = cham_max_gan[col] / 2 if cham_max_gan[col] > 0 else 5
            if cham_gan[col] >= avg_gan[col]:
                exceeding_avg.append(col)
        
        # Sort by gan descending (highest gan first = most khan)
        self.cham_gaps_3d = cham_gan
        self.exceeding_cham = sorted(exceeding_avg, key=lambda x: cham_gan[x], reverse=True)

        # RENDERING
        current_y = 0
        
        # 1. Header Row
        draw_cell(COL_DATE, current_y, DATE_W, CELL_H, "Chạm 3D", header_bg, "white", FONT_HEADER)
        for i in range(10):
            draw_cell(COL_START_NUM + i*CELL_W, current_y, CELL_W, CELL_H, str(i), header_bg, "white", FONT_NUM_BOLD)
        draw_cell(COL_MAX, current_y, 35, CELL_H, "Max", "#8e44ad", "white", ('Segoe UI', 7, 'bold'))
        draw_cell(COL_DANG, current_y, 35, CELL_H, "Đang", "#e67e22", "white", ('Segoe UI', 7, 'bold'))
        draw_cell(COL_DB, current_y, 60, CELL_H, "ĐB", "#2980b9", "white", ('Segoe UI', 7, 'bold'))
        current_y += CELL_H
        
        # 2. Max Gan Row
        draw_cell(COL_DATE, current_y, DATE_W, CELL_H, "Max Gan", "#8e44ad", "white", ('Segoe UI', 7, 'bold'))
        for col in range(10):
            max_g = cham_max_gan[col]
            bg_col = "#e74c3c" if max_g >= self.GAN_VERY_HIGH_THRESHOLD else "#f39c12" if max_g >= self.GAN_HIGH_THRESHOLD else "#27ae60"
            draw_cell(COL_START_NUM + col*CELL_W, current_y, CELL_W, CELL_H, str(max_g), bg_col, "white", FONT_NUM_BOLD)
        draw_cell(COL_MAX, current_y, 35, CELL_H, "", "#d5dbdb", "black")
        draw_cell(COL_DANG, current_y, 35, CELL_H, "", "#fdf2e9", "black")
        draw_cell(COL_DB, current_y, 60, CELL_H, "", "#ebf5fb", "black")
        current_y += CELL_H
        
        # 3. Prediction Row ("Đang Gan")
        draw_cell(COL_DATE, current_y, DATE_W, CELL_H, "Đang Gan", "#16a085", "white", ('Segoe UI', 7, 'bold'))
        for col in range(10):
            curr_g = cham_gan[col]
            if col in exceeding_avg:
                bg_col = "#e74c3c"
                fg_col = "white"
            else:
                bg_col = "#ecf0f1"
                fg_col = "black"
            draw_cell(COL_START_NUM + col*CELL_W, current_y, CELL_W, CELL_H, str(curr_g), bg_col, fg_col, FONT_NUM_BOLD)
        
        # Prediction summary in "Đang" column
        pred_text = ",".join(map(str, exceeding_avg)) if exceeding_avg else "-"
        draw_cell(COL_MAX, current_y, 35, CELL_H, "Dự:", "#16a085", "white", ('Segoe UI', 7))
        draw_cell(COL_DANG, current_y, 35, CELL_H, pred_text, "#e74c3c", "white", ('Consolas', 8, 'bold'))
        draw_cell(COL_DB, current_y, 60, CELL_H, "", "#ebf5fb", "black")
        current_y += CELL_H
        
        # 4. Data Rows (Newest first)
        for idx, row_data in enumerate(reversed(row_results)):
            # Alternating background
            row_bg = "#2d2d2d" if idx % 2 == 0 else "#1e1e1e"
            
            # Date
            draw_cell(COL_DATE, current_y, DATE_W, CELL_H, row_data['date'][:5], row_bg, "#bdc3c7", ('Consolas', 7))
            
            # 0-9 Cells
            for col in range(10):
                cell = row_data['cells'][col]
                # If it's a miss (normal_bg), use row_bg for alternating effect
                c_bg = row_bg if cell['bg'] == normal_bg else cell['bg']
                draw_cell(COL_START_NUM + col*CELL_W, current_y, CELL_W, CELL_H, 
                          cell['text'], c_bg, cell['fg'], ('Consolas', 8))
            
            # Stats columns
            draw_cell(COL_MAX, current_y, 35, CELL_H, str(row_data['max_val']), "#34495e", "white", ('Consolas', 8, 'bold'))
            draw_cell(COL_DANG, current_y, 35, CELL_H, "", row_bg, "black")
            
            # ĐB
            draw_cell(COL_DB, current_y, 60, CELL_H, row_data['db_3so'], row_bg, "#f39c12", ('Consolas', 9, 'bold'))
            
            current_y += CELL_H

        # Update scrollregion
        canvas.configure(scrollregion=(0, 0, TOTAL_W, current_y))
        

        

        # Update scroll region
        self.cham_canvas.config(scrollregion=self.cham_canvas.bbox("all"))
        
        # Update stats
        current_gan = [f"{i}:{cham_gan[i]}" for i in range(10)]
        exceed_text = f" | Vượt TB: {','.join(map(str, exceeding_avg))}" if exceeding_avg else ""
        self.cham_stats_label.config(text=f"Gan hiện tại: {', '.join(current_gan)}{exceed_text}")
        
        # Count frequency of each 3-digit number in historical data
        number_freq = {}
        for day in days_data_ordered:
            db_3so_list = day['db_3so'] # Now a LIST
            for db_3so in db_3so_list:
                number_freq[db_3so] = number_freq.get(db_3so, 0) + 1
        
        # Update prediction text box with copyable numbers - SEPARATE each Chạm

        self.cham_pred_text.delete('1.0', tk.END)
        if exceeding_avg:
            # Sort by gan descending (highest gan first = most likely to hit)
            sorted_digits = sorted(exceeding_avg, key=lambda x: cham_gan[x], reverse=True)
            lines = []
            for digit in sorted_digits:
                numbers = CHAM_3CANG[digit]
                # Sort by frequency (most frequent first)
                sorted_nums = sorted(numbers, key=lambda x: number_freq.get(x, 0), reverse=True)
                num_str = ",".join(sorted_nums)
                lines.append(f"Chạm {digit} (gan:{cham_gan[digit]}): {num_str}")
            self.cham_pred_text.insert(tk.END, "\n".join(lines))
        else:
            self.cham_pred_text.insert(tk.END, "Không có số vượt trung bình")
        
            
    def _render_tong_table(self, days_data, header_bg, hit_bg, miss_bg, normal_bg, highlight_bg):
        """Render Tổng 3D table using Canvas for performance"""
        canvas = self.tong_canvas
        canvas.delete("all")
        
        # Dimensions
        CELL_W = 24
        CELL_H = 25
        DATE_W = 60
        COL_DATE = 0
        COL_START_NUM = DATE_W
        COL_MAX = DATE_W + 10 * CELL_W
        COL_TONG = COL_MAX + 35
        COL_DB = COL_TONG + 35
        TOTAL_W = COL_DB + 60
        
        FONT_HEADER = ('Segoe UI', 8, 'bold')
        FONT_NUM = ('Consolas', 9)
        FONT_NUM_BOLD = ('Consolas', 9, 'bold')
        
        # Helper to draw cell
        def draw_cell(x, y, w, h, text, bg, fg, font=FONT_NUM):
            canvas.create_rectangle(x, y, x + w, y + h, fill=bg, outline='#dcdcdc')
            canvas.create_text(x + w/2, y + h/2, text=text, fill=fg, font=font)

        # IMPORTANT: days_data comes newest-first from API, need to reverse for correct gan calculation
        days_data_ordered = list(reversed(days_data))  # Now oldest first
        
        # PRE-CALCULATION
        tong_gan = {i: 0 for i in range(10)}
        tong_max_gan = {i: 0 for i in range(10)}
        row_results = []
        
        for day in days_data_ordered:
            current_tong_list = day['tong'] # LIST of ints
            
            # Format DB display (join with newline if multiple)
            db_display = "\n".join(day['db_3so'])
            
            row_data = {
                'date': day['date'],
                'db_3so': db_display,
                'tong_val': current_tong_list,
                'cells': {},
                'max_val': 0
            }
            
            for col in range(10):
                # Is HIT if col is in the list of tongs for this day
                is_hit = col in current_tong_list
                
                if is_hit:
                    cell_text = str(tong_gan[col]) if tong_gan[col] > 0 else "0"
                    cell_bg = hit_bg
                    tong_gan[col] = 0
                else:
                    tong_gan[col] += 1
                    cell_bg = miss_bg if tong_gan[col] >= self.GAN_HIGH_THRESHOLD else normal_bg
                    cell_text = str(tong_gan[col])
                
                if tong_gan[col] > tong_max_gan[col]:
                    tong_max_gan[col] = tong_gan[col]
                    
                fg_color = "white" if is_hit or tong_gan[col] >= self.GAN_HIGH_THRESHOLD else "#bdc3c7"
                row_data['cells'][col] = {'text': cell_text, 'bg': cell_bg, 'fg': fg_color, 'is_hit': is_hit}
            
            row_data['max_val'] = max(tong_gan.values())
            row_results.append(row_data)
        
        # Calculate Average Gan and Exceeding
        avg_gan = {}
        exceeding_avg = []
        for col in range(10):
            avg_gan[col] = tong_max_gan[col] / 2 if tong_max_gan[col] > 0 else 5
            if tong_gan[col] >= avg_gan[col]:
                exceeding_avg.append(col)
                
        # Sort by gan descending (highest gan first = most khan)
        self.tong_gaps_3d = tong_gan
        self.exceeding_tong = sorted(exceeding_avg, key=lambda x: tong_gan[x], reverse=True)
        
        # RENDERING
        current_y = 0
        
        # 1. Header Row
        draw_cell(COL_DATE, current_y, DATE_W, CELL_H, "Tổng 3D", header_bg, "white", FONT_HEADER)
        for i in range(10):
            draw_cell(COL_START_NUM + i*CELL_W, current_y, CELL_W, CELL_H, str(i), header_bg, "white", FONT_NUM_BOLD)
        draw_cell(COL_MAX, current_y, 35, CELL_H, "Max", "#8e44ad", "white", ('Segoe UI', 7, 'bold'))
        draw_cell(COL_TONG, current_y, 35, CELL_H, "Tổng", "#e67e22", "white", ('Segoe UI', 7, 'bold'))
        draw_cell(COL_DB, current_y, 60, CELL_H, "ĐB", "#2980b9", "white", ('Segoe UI', 7, 'bold'))
        current_y += CELL_H
        
        # 2. Max Gan Row
        draw_cell(COL_DATE, current_y, DATE_W, CELL_H, "Max Gan", "#8e44ad", "white", ('Segoe UI', 7, 'bold'))
        for col in range(10):
            max_g = tong_max_gan[col]
            bg_col = "#e74c3c" if max_g >= self.GAN_VERY_HIGH_THRESHOLD else "#f39c12" if max_g >= self.GAN_HIGH_THRESHOLD else "#27ae60"
            draw_cell(COL_START_NUM + col*CELL_W, current_y, CELL_W, CELL_H, str(max_g), bg_col, "white", FONT_NUM_BOLD)
        draw_cell(COL_MAX, current_y, 35, CELL_H, "", "#d5dbdb", "black")
        draw_cell(COL_TONG, current_y, 35, CELL_H, "", "#fdf2e9", "black")
        draw_cell(COL_DB, current_y, 60, CELL_H, "", "#ebf5fb", "black")
        current_y += CELL_H
        
        # 3. Prediction Row ("Đang Gan")
        draw_cell(COL_DATE, current_y, DATE_W, CELL_H, "Đang Gan", "#16a085", "white", ('Segoe UI', 7, 'bold'))
        for col in range(10):
            curr_g = tong_gan[col]
            if col in exceeding_avg:
                bg_col = "#e74c3c"
                fg_col = "white"
            else:
                bg_col = "#ecf0f1"
                fg_col = "black"
            draw_cell(COL_START_NUM + col*CELL_W, current_y, CELL_W, CELL_H, str(curr_g), bg_col, fg_col, FONT_NUM_BOLD)
            
        # Prediction summary in "Tổng" column
        pred_text = ",".join(map(str, exceeding_avg)) if exceeding_avg else "-"
        draw_cell(COL_MAX, current_y, 35, CELL_H, "Dự:", "#16a085", "white", ('Segoe UI', 7))
        draw_cell(COL_TONG, current_y, 35, CELL_H, pred_text, "#e74c3c", "white", ('Consolas', 8, 'bold'))
        draw_cell(COL_DB, current_y, 60, CELL_H, "", "#ebf5fb", "black")
        current_y += CELL_H
        
        # 4. Data Rows (Newest first)
        for idx, row_data in enumerate(reversed(row_results)):
            # Alternating background
            row_bg = "#2d2d2d" if idx % 2 == 0 else "#1e1e1e"

            # Date
            draw_cell(COL_DATE, current_y, DATE_W, CELL_H, row_data['date'][:5], row_bg, "#bdc3c7", ('Consolas', 7))
            
            # 0-9 Cells
            for col in range(10):
                cell = row_data['cells'][col]
                # If it's a miss (normal_bg), use row_bg for alternating effect
                c_bg = row_bg if cell['bg'] == normal_bg else cell['bg']
                draw_cell(COL_START_NUM + col*CELL_W, current_y, CELL_W, CELL_H, 
                          cell['text'], c_bg, cell['fg'], ('Consolas', 8))
            
            # Stats columns
            draw_cell(COL_MAX, current_y, 35, CELL_H, str(row_data['max_val']), "#34495e", "white", ('Consolas', 8, 'bold'))
            draw_cell(COL_TONG, current_y, 35, CELL_H, str(row_data['tong_val']), row_bg, "#3498db", ('Consolas', 9, 'bold'))
            draw_cell(COL_DB, current_y, 60, CELL_H, row_data['db_3so'], row_bg, "#f39c12", ('Consolas', 9, 'bold'))
            
            current_y += CELL_H
            
        # Update scrollregion
        # Update scrollregion
        canvas.configure(scrollregion=(0, 0, TOTAL_W, current_y))
        
        # Update stats

        current_gan = [f"{i}:{tong_gan[i]}" for i in range(10)]
        exceed_text = f" | Vượt TB: {','.join(map(str, exceeding_avg))}" if exceeding_avg else ""
        self.tong_stats_label.config(text=f"Gan hiện tại: {', '.join(current_gan)}{exceed_text}")
        
        # Count frequency of each 3-digit number in historical data (ONLY PREDICTION)
        number_freq = {}
        for day in days_data_ordered:
            db_3so_list = day['db_3so']
            for db_3so in db_3so_list:
                number_freq[db_3so] = number_freq.get(db_3so, 0) + 1
        
        # Update prediction text box
        # Update prediction text box
        self.tong_pred_text.delete('1.0', tk.END)
        if exceeding_avg:
            sorted_tongs = sorted(exceeding_avg, key=lambda x: tong_gan[x], reverse=True)
            lines = []
            for tong_val in sorted_tongs:
                numbers = TONG_3CANG[tong_val]
                sorted_nums = sorted(numbers, key=lambda x: number_freq.get(x, 0), reverse=True)
                num_str = ",".join(sorted_nums)
                lines.append(f"Tổng {tong_val} (gan:{tong_gan[tong_val]}): {num_str}")
            self.tong_pred_text.insert(tk.END, "\n".join(lines))
        else:
            self.tong_pred_text.insert(tk.END, "Không có số vượt trung bình")
        

            


        
    def _calculate_3d_scores(self):
        """Calculate overlap scores for all 1000 numbers (000-999) based on current exceeding sets"""
        exceeding_cham = getattr(self, 'exceeding_cham', [])
        exceeding_tong = getattr(self, 'exceeding_tong', [])
        
        scores = [0] * 1000
        for digit in exceeding_cham:
            for num_str in CHAM_3CANG[digit]:
                scores[int(num_str)] += 1
                
        for val in exceeding_tong:
            for num_str in TONG_3CANG[val]:
                scores[int(num_str)] += 1
        return scores

    def _calculate_4d_scores(self):
        """Calculate overlap scores for all 10000 numbers (0000-9999) based on current exceeding sets"""
        exceeding_cham = getattr(self, 'exceeding_cham_4d', [])
        exceeding_tong = getattr(self, 'exceeding_tong_4d', [])
        
        scores = [0] * 10000
        for digit in exceeding_cham:
            for num_str in CHAM_4CANG[digit]:
                scores[int(num_str)] += 1
                
        for val in exceeding_tong:
            for num_str in TONG_4CANG[val]:
                scores[int(num_str)] += 1
        return scores


            



    def _render_rare_hundreds(self, days_data):
        """Analyze and display top 5 rare hundreds digits"""
        self.rare_hundreds_text.delete('1.0', tk.END)
        if not days_data:
            return
            
        # Get exceeding sets (Gan)
        exceeding_cham = getattr(self, 'exceeding_cham', [])
        exceeding_tong = getattr(self, 'exceeding_tong', [])
        
        # Pre-calculate Gan sets
        cham_gan_set = set()
        for c in exceeding_cham:
            cham_gan_set.update(CHAM_3CANG[c])
            
        tong_gan_set = set()
        for t in exceeding_tong:
            tong_gan_set.update(TONG_3CANG[t])

        # days_data comes newest first.
        # Logic: find the most recent appearance of each hundreds digit.
        
        last_appearance = {} # digit -> index (0 is newest)
        
        for idx, day in enumerate(days_data):
            db_3so_list = day['db_3so'] # List of strings "XYZ"
            
            # Check all results for this day (usually 1, but can be multiple)
            # Find hundreds digits appearing on this day
            digits_on_day = set()
            for val in db_3so_list:
                if len(val) == 3 and val.isdigit():
                    digits_on_day.add(int(val[0])) # 1st char of 3-digit string = Hundreds
            
            for d in digits_on_day:
                if d not in last_appearance:
                    last_appearance[d] = idx
            
            if len(last_appearance) == 10:
                break
        
        # Calculate gaps
        gaps = []
        max_days = len(days_data)
        for d in range(10):
            gap = last_appearance.get(d, max_days) # If not found, gap is at least total fetched days
            gaps.append((d, gap))
            
        # Sort by gap descending (Khan) or increasing (Moi/Recent)
        try:
            mode = self.rare_mode_3d.get()
        except:
            mode = "Khan (Lâu chưa ra)"
            
        is_recent_mode = "Mới" in mode
        
        # Sort
        gaps.sort(key=lambda x: x[1], reverse=not is_recent_mode)
        top_5 = gaps[:5]
        
        # Display
        total_filtered = []
        for rank, (digit, gap) in enumerate(top_5, 1):
            gap_str = str(gap) if gap < max_days else f">{max_days}"
            header_prefix = "Mới" if is_recent_mode else "Khan"
            header = f"Top {rank}: Đầu {digit} ({header_prefix} {gap_str})\n"
            self.rare_hundreds_text.insert(tk.END, header)
            
            # Generate set: X00-X99
            number_set = [f"{digit}{tail:02d}" for tail in range(100)]
            set_str = ",".join(number_set)
            
            self.rare_hundreds_text.insert(tk.END, f"{set_str}\n")
            
            # Filter overlap
            filtered_nums = []
            for num in number_set:
                if num in cham_gan_set and num in tong_gan_set:
                    filtered_nums.append(num)
            
            if filtered_nums:
                self.rare_hundreds_text.insert(tk.END, f"   -> Trùng 3 Dàn ({len(filtered_nums)} số): {','.join(filtered_nums)}\n\n", 'highlight')
                total_filtered.extend(filtered_nums)
            else:
                self.rare_hundreds_text.insert(tk.END, "\n")
                
        # Aggregate Summary
        gaps_dict = {d: g for d, g in gaps}
        exceeding_list = [d for d, g in top_5]
        hits_today = [d for d, g in gaps if g == 0]
        khan_data = (gaps_dict, exceeding_list, hits_today)
        self.update_rare_info_on_selector(khan_3d_str=khan_data, khan_4d_str=None)
        
        if total_filtered:
            total_filtered = sorted(list(set(total_filtered))) # Ensure unique and sorted
            self.rare_hundreds_text.insert(tk.END, f"🔥 TỔNG HỢP DÀN ÉP ({len(total_filtered)} số):\n", 'header')
            self.rare_hundreds_text.insert(tk.END, f"{','.join(total_filtered)}\n", 'highlight')
        else:
             self.rare_hundreds_text.insert(tk.END, "🔥 TỔNG HỢP DÀN ÉP: Không có số nào\n", 'header')


    def _get_column_stats(self, all_days_data):
        """Calculate gap and periodicity stats for columns N1-N28."""
        stats = {}
        for k in range(1, self.MAX_MATRIX_COLS + 1):
            hits = []
            for row_idx, day_data in enumerate(all_days_data):
                check_idx = row_idx - k + 1
                if 0 <= check_idx < len(all_days_data):
                    check_result = all_days_data[check_idx].get('check_result', [])
                    combos = day_data.get('combos', [])
                    if any(v in combos for v in check_result):
                        hits.append(row_idx)
            
            if not hits:
                stats[k] = {'current_gap': len(all_days_data), 'max_gap': len(all_days_data), 'avg_interval': len(all_days_data), 'std_dev': 99}
                continue

            current_gap = hits[0]
            intervals = []
            for i in range(len(hits) - 1):
                intervals.append(hits[i+1] - hits[i])
            
            max_gap = max(intervals) if intervals else current_gap
            avg_interval = sum(intervals) / len(intervals) if intervals else current_gap
            
            if len(intervals) > 1:
                variance = sum((x - avg_interval)**2 for x in intervals) / len(intervals)
                std_dev = variance ** 0.5
            else:
                std_dev = 99

            stats[k] = {
                'current_gap': current_gap,
                'max_gap': max_gap,
                'avg_interval': avg_interval,
                'std_dev': std_dev
            }
        return stats

    def render_smart_suggestions(self, all_days_data, target_widget, target_vars=None):
        """Analyze and display high-probability suggestions (V2 - Strict Performance & Anti-Just-Hit)."""
        if not all_days_data or len(all_days_data) < 20: return
        
        candidates_col = []
        candidates_row = []
        MAX_K = 28 # Analyze N1-N28
        
        def get_stats(hits_bool_list):
            if not hits_bool_list: return (0, 0, 0, 999)
            total_hits = sum(hits_bool_list)
            # Current Gap
            curr_gap = 0
            for is_hit in hits_bool_list:
                if is_hit: break
                curr_gap += 1
            # Intervals
            hit_indices = [i for i, x in enumerate(hits_bool_list) if x]
            if len(hit_indices) > 1:
                intervals = [hit_indices[i+1] - hit_indices[i] for i in range(len(hit_indices)-1)]
                avg = sum(intervals) / len(intervals)
                variance = sum((x - avg)**2 for x in intervals) / len(intervals)
                std_dev = variance ** 0.5
            else:
                avg = 15 if total_hits == 1 else 30
                std_dev = 999
            return (total_hits, avg, curr_gap, std_dev)

        def get_source_performance(src_idx):
            """Count total hits of SourceDate across all columns in history."""
            if src_idx >= len(all_days_data): return 0
            src_combos = set(all_days_data[src_idx]['combos'])
            hit_count = 0
            # Scan results from result(0) to result(src_idx-1)
            for i in range(min(src_idx, len(all_days_data))):
                res = all_days_data[i]['check_result']
                res_set = set(res) if isinstance(res, list) else {str(res)}
                if not res_set.isdisjoint(src_combos):
                    hit_count += 1
            return hit_count

        for k in range(1, MAX_K + 1):
            hits_history = []
            for i in range(len(all_days_data)):
                src_idx = i + k - 1
                if src_idx >= len(all_days_data): break
                try:
                    s_combos = set(all_days_data[src_idx]['combos'])
                    r_res = all_days_data[i]['check_result']
                    is_hit = not (set(r_res) if isinstance(r_res, list) else {str(r_res)}).isdisjoint(s_combos)
                    hits_history.append(is_hit)
                except Exception: break

            total_hits, avg_gap, curr_gap, std_dev = get_stats(hits_history)
            
            # Filter 1: Column Gap >= 2
            if curr_gap < 2: continue
            
            pred_idx = k - 2
            if 0 <= pred_idx < len(all_days_data):
                pred_combos = set(all_days_data[pred_idx]['combos'])
                
                # Filter 2: Anti-Just-Hit (Numbers appeared in Result(0..2))
                just_hit = False
                for d_offset in range(min(3, len(all_days_data))):
                    res = all_days_data[d_offset]['check_result']
                    res_set = set(res) if isinstance(res, list) else {str(res)}
                    if not res_set.isdisjoint(pred_combos):
                        just_hit = True; break
                if just_hit: continue

                common_data = {
                    'col': f"N{k}", 'k': k, 'hits': total_hits, 'avg': avg_gap, 'curr': curr_gap, 
                    'std': std_dev, 'combos': all_days_data[pred_idx]['combos'], 'date': all_days_data[pred_idx]['date']
                }
                
                # CỘT (N2-N28)
                if k >= 2 and total_hits >= 1 and avg_gap > 0:
                    diff = abs(curr_gap - avg_gap)
                    col_score = (total_hits / 5) + (1.0 / (1.0 + diff)) * 5
                    candidates_col.append({**common_data, 'score': col_score})
                
                # HÀNG (N7-N28)
                if k >= 7:
                    perf = get_source_performance(pred_idx)
                    # Relaxed: Always consider if k >= 7, but score higher for more hits
                    row_score = (10.0 / (1.0 + std_dev)) + (total_hits / 8) + (perf * 3)
                    candidates_row.append({**common_data, 'score': row_score, 'perf': perf})

        # Selection (Ensure 2 Col + 2 Row)
        candidates_col.sort(key=lambda x: x['score'], reverse=True)
        candidates_row.sort(key=lambda x: x['score'], reverse=True)
        
        selected = []
        seen_k = set()
        for c in candidates_col:
            selected.append({**c, 'tag': 'Cột'})
            seen_k.add(c['k'])
            if len(selected) >= 2: break
            
        row_count = 0
        for r in candidates_row:
            if r['k'] not in seen_k:
                selected.append({**r, 'tag': 'Hàng'})
                seen_k.add(r['k'])
                row_count += 1
            if row_count >= 2: break
            
        if selected:
            target_widget.insert(tk.END, "🚀 GỢI Ý THÔNG MINH (V2 - CỘT & HÀNG - ĐỦ 4 DÀN):\n", 'title')
            for i, cand in enumerate(selected, 1):
                if cand['tag'] == 'Cột':
                    note = f"Biên độ (Cột) - Hit {cand['hits']} - Gap {cand['curr']}/{cand['avg']:.1f}"
                else:
                    note = f"Nhịp (Hàng) - Đã nổ {cand['perf']} lần - Hit {cand['hits']}"
                target_widget.insert(tk.END, f" {i}. {cand['col']} - {note}\n", 'subtitle')
                target_widget.insert(tk.END, f"    {cand['date']} ({len(cand['combos'])}s)\n", 'numbers')
            
            if target_vars is not None:
                def auto_select():
                    count = 0
                    for cand in selected:
                        key = (cand['date'], tuple(cand['combos']))
                        if key in target_vars:
                            target_vars[key].set(True); count += 1
                    try:
                        top = tk.Toplevel(); top.overrideredirect(True); top.attributes("-topmost", True)
                        rw, rh = target_widget.winfo_toplevel().winfo_width(), target_widget.winfo_toplevel().winfo_height()
                        rx, ry = target_widget.winfo_toplevel().winfo_rootx(), target_widget.winfo_toplevel().winfo_rooty()
                        top.geometry(f"300x50+{rx + rw//2 - 150}+{ry + rh//2 - 25}")
                        tk.Label(top, text=f"✅ Đã chọn {count} dàn!", bg="#27ae60", fg="white", font=('Segoe UI', 10, 'bold'), relief="solid").pack(fill='both', expand=True)
                        top.after(1000, top.destroy)
                    except: pass
                b = tk.Button(target_widget, text="✅ Chọn 4 dàn này", command=auto_select, bg="#27ae60", fg="white", font=('Segoe UI', 9, 'bold'), cursor="hand2")
                target_widget.window_create(tk.END, window=b); target_widget.insert(tk.END, "\n")
            target_widget.insert(tk.END, "—"*24 + "\n\n")

    def create_tong_cham_4d_view(self):
        """Tạo view cho tab Tổng & Chạm 4 Càng - Clone từ 3D"""
        # Info label at top
        info_frame = tk.Frame(self.tong_cham_4d_frame, bg="#333333")  # Slightly different bg
        info_frame.pack(fill=tk.X)
        
        self.tong_cham_4d_info_label = tk.Label(info_frame, 
                                              text="📊 THEO DÕI TỔNG & CHẠM 4 CÀNG - Tải dữ liệu để hiển thị", 
                                              bg="#333333", fg="white", font=('Segoe UI', 12, 'bold'))
        self.tong_cham_4d_info_label.pack(pady=8)
        
        # Main container with 3 panels: CHẠM | TỔNG | DỰ ĐOÁN
        main_container = tk.Frame(self.tong_cham_4d_frame, bg="#1e1e1e")
        main_container.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # === LEFT SIDE: CHẠM 4D ===
        left_frame = tk.LabelFrame(main_container, text="CHẠM 4D", font=('Segoe UI', 10, 'bold'),
                                   bg="#1e1e1e", fg="#e74c3c")
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 2))
        
        # Canvas for scrollable Chạm table
        cham_canvas_frame = tk.Frame(left_frame, bg="#1e1e1e")
        cham_canvas_frame.pack(fill=tk.BOTH, expand=True)
        
        self.cham_4d_canvas = tk.Canvas(cham_canvas_frame, bg="#1e1e1e", highlightthickness=0)
        cham_v_scroll = tk.Scrollbar(cham_canvas_frame, orient=tk.VERTICAL, command=self.cham_4d_canvas.yview)
        cham_h_scroll = tk.Scrollbar(cham_canvas_frame, orient=tk.HORIZONTAL, command=self.cham_4d_canvas.xview)
        
        self.cham_4d_canvas.configure(yscrollcommand=cham_v_scroll.set, xscrollcommand=cham_h_scroll.set)
        
        cham_v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        cham_h_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        self.cham_4d_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._setup_canvas_mousewheel(self.cham_4d_canvas)
        
        # Chạm stats label
        self.cham_4d_stats_label = tk.Label(left_frame, text="", bg="#2d2d2d", 
                                          font=('Segoe UI', 8, 'bold'), fg="#ecf0f1")
        self.cham_4d_stats_label.pack(fill=tk.X, pady=1)
        
        # === MIDDLE: TỔNG 4D ===
        middle_frame = tk.LabelFrame(main_container, text="TỔNG 4D", font=('Segoe UI', 10, 'bold'),
                                    bg="#1e1e1e", fg="#3498db")
        middle_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=2)
        
        # Canvas for scrollable Tổng table
        tong_canvas_frame = tk.Frame(middle_frame, bg="#1e1e1e")
        tong_canvas_frame.pack(fill=tk.BOTH, expand=True)
        
        self.tong_4d_canvas = tk.Canvas(tong_canvas_frame, bg="#1e1e1e", highlightthickness=0)
        tong_v_scroll = tk.Scrollbar(tong_canvas_frame, orient=tk.VERTICAL, command=self.tong_4d_canvas.yview)
        tong_h_scroll = tk.Scrollbar(tong_canvas_frame, orient=tk.HORIZONTAL, command=self.tong_4d_canvas.xview)
        
        self.tong_4d_canvas.configure(yscrollcommand=tong_v_scroll.set, xscrollcommand=tong_h_scroll.set)
        
        tong_v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        tong_h_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        self.tong_4d_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._setup_canvas_mousewheel(self.tong_4d_canvas)
        
        # Tổng stats label
        self.tong_4d_stats_label = tk.Label(middle_frame, text="", bg="#2d2d2d",
                                          font=('Segoe UI', 8, 'bold'), fg="#ecf0f1")
        self.tong_4d_stats_label.pack(fill=tk.X, pady=1)
        
        # === RIGHT SIDE: DỰ ĐOÁN 4 CÀNG ===
        pred_frame = tk.LabelFrame(main_container, text="DỰ ĐOÁN 4 CÀNG", font=('Segoe UI', 10, 'bold'),
                                   bg="#333333", fg="white", width=500)
        pred_frame.pack(side=tk.RIGHT, fill=tk.BOTH, padx=(2, 0))
        pred_frame.pack_propagate(False)
        
        # Section 1: Dàn Chạm 4D
        sec1 = tk.Frame(pred_frame, bg="#333333")
        sec1.pack(fill=tk.BOTH, expand=True, pady=(0, 2))
        tk.Label(sec1, text="🎯 DÀN CHẠM 4D:", bg="#333333", fg="#f39c12",
                font=('Segoe UI', 9, 'bold')).pack(anchor='w', padx=5, pady=(2,0))
        self.cham_4d_pred_text = scrolledtext.ScrolledText(sec1, wrap=tk.WORD, height=1,
                                       bg="#1e1e1e", fg="white", font=('Consolas', 9),
                                       borderwidth=0, padx=5, pady=5)
        self.cham_4d_pred_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=2)
        
        # Section 2: Dàn Tổng 4D
        sec2 = tk.Frame(pred_frame, bg="#333333")
        sec2.pack(fill=tk.BOTH, expand=True, pady=2)
        tk.Label(sec2, text="🎯 DÀN TỔNG 4D:", bg="#333333", fg="#3498db",
                font=('Segoe UI', 9, 'bold')).pack(anchor='w', padx=5, pady=(2,0))
        self.tong_4d_pred_text = scrolledtext.ScrolledText(sec2, wrap=tk.WORD, height=1,
                                       bg="#1e1e1e", fg="white", font=('Consolas', 9),
                                       borderwidth=0, padx=5, pady=5)
        self.tong_4d_pred_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=2)
        
        # Section 3: Rare Thousands (4D)
        sec3 = tk.Frame(pred_frame, bg="#333333")
        sec3.pack(fill=tk.BOTH, expand=True, pady=(2, 0))
        rare_header = tk.Frame(sec3, bg="#333333")
        rare_header.pack(fill=tk.X, padx=5, pady=(2,0))
        tk.Label(rare_header, text="🎯 THỐNG KÊ NGHÌN (RARE):", bg="#333333", fg="#2ecc71",
                font=('Segoe UI', 9, 'bold')).pack(side=tk.LEFT)
                
        self.rare_mode_4d = tk.StringVar(value="Khan (Lâu chưa ra)")
        mode_cb = ttk.Combobox(rare_header, textvariable=self.rare_mode_4d, state='readonly', width=20, font=('Segoe UI', 8))
        mode_cb['values'] = ["Khan (Lâu chưa ra)", "Mới (Vừa ra)"]
        mode_cb.pack(side=tk.RIGHT)
        mode_cb.bind("<<ComboboxSelected>>", lambda e: self.render_tong_cham_4d())
        
        self.rare_thousands_text = scrolledtext.ScrolledText(sec3, wrap=tk.WORD, height=1,
                                           bg="#1e1e1e", fg="#bdc3c7", font=('Consolas', 8),
                                           borderwidth=0, padx=5, pady=5)
        self.rare_thousands_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=2)
        
        self.rare_thousands_text.tag_configure('header', foreground="#f1c40f", font=('Segoe UI', 9, 'bold'))
        self.rare_thousands_text.tag_configure('highlight', foreground="#e74c3c", font=('Consolas', 8, 'bold'))



    def create_cb_row(self, parent, label_text, var_list, color, labels=None, wrap_at=None, sugg_below=False, sugg_height=1):
        row = tk.Frame(parent, bg=self.secondary_bg)
        row.pack(fill=tk.X, padx=5, pady=1)
        # Label (Use NW anchor for multi-line support)
        tk.Label(row, text=label_text, bg=self.secondary_bg, fg=color, 
                 font=('Segoe UI', 9, 'bold'), width=12, anchor='nw').pack(side=tk.LEFT, pady=2)
        
        # CB Wrapper to handle wrapping
        cb_wrapper = tk.Frame(row, bg=self.secondary_bg)
        cb_wrapper.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        row1 = tk.Frame(cb_wrapper, bg=self.secondary_bg)
        row1.pack(side=tk.TOP, anchor='w', fill=tk.X, expand=True)
        
        row2 = None
        if wrap_at:
            row2 = tk.Frame(cb_wrapper, bg=self.secondary_bg)
            row2.pack(side=tk.TOP, anchor='w', fill=tk.X, expand=True)
        
        num_cbs = len(var_list)
        for i in range(num_cbs):
            txt = labels[i] if labels and i < len(labels) else str(i)
            target = row2 if wrap_at and i >= wrap_at else row1
            
            cb = tk.Checkbutton(target, text=txt, variable=var_list[i],
                                bg=self.secondary_bg, fg="white", selectcolor="#e67e22",
                                activebackground=self.secondary_bg, activeforeground="white",
                                font=('Segoe UI', 8))
            cb.pack(side=tk.LEFT, padx=1)
        
        # Suggestion Text - Font increased for better readability
        # Place in row1 if wrapped, otherwise in outer row
        if sugg_below:
            target_container = tk.Frame(cb_wrapper, bg=self.secondary_bg)
            target_container.pack(side=tk.TOP, anchor='w', fill=tk.X, expand=True)
        else:
            target_container = row1 if wrap_at else row
            
        s_font = ('Segoe UI', 8, 'italic')
        sugg_text = tk.Text(target_container, height=sugg_height, width=60, bg=self.secondary_bg, bd=0, 
                            highlightthickness=0, font=s_font, 
                            padx=5, state='disabled')
        sugg_text.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        sugg_text.tag_configure('top_3_dự', foreground="#e74c3c", font=('Segoe UI', 8, 'bold', 'italic'))
        sugg_text.tag_configure('top_3', foreground="#e74c3c", font=('Segoe UI', 8, 'italic'))
        sugg_text.tag_configure('dự', foreground="#2ecc71", font=('Segoe UI', 8, 'bold', 'italic'))
        sugg_text.tag_configure('hit', foreground="#f1c40f", font=('Segoe UI', 8, 'bold'))
        sugg_text.tag_configure('normal', foreground="#e67e22") # Changed to orange for better visibility
        sugg_text.tag_configure('rare', foreground="#e74c3c", font=('Segoe UI', 8, 'bold', 'italic')) # Added red for high gaps
        return sugg_text

    def create_selector_view(self):
        """Tạo tab Bộ Chọn Chạm/Tổng với 50 checkbox chia 2 cột và kết quả Hybrid"""
        self.tab_selector = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_selector, text="Bộ Chọn Chạm/Tổng")
        
        main_container = tk.Frame(self.tab_selector, bg=self.secondary_bg)
        main_container.pack(fill=tk.BOTH, expand=True)
        
        # --- TOP: CONTROL PANEL (2 COLUMNS SIDE-BY-SIDE) ---
        control_wrapper = tk.Frame(main_container, bg=self.secondary_bg)
        control_wrapper.pack(fill=tk.X, padx=10, pady=5)
        
        self.selector_vars_3d_cham = [tk.BooleanVar() for _ in range(10)]
        self.selector_vars_3d_tong = [tk.BooleanVar() for _ in range(10)]
        self.selector_vars_3d_tram = [tk.BooleanVar() for _ in range(10)]
        self.selector_vars_4d_cham = [tk.BooleanVar() for _ in range(10)]
        self.selector_vars_4d_tong = [tk.BooleanVar() for _ in range(10)]
        self.selector_vars_4d_ngan = [tk.BooleanVar() for _ in range(10)]
        # Add 2D Selectors
        self.selector_vars_2d_cham = [tk.BooleanVar() for _ in range(10)]
        self.selector_vars_2d_tong = [tk.BooleanVar() for _ in range(10)]
        self.selector_vars_2d_hieu = [tk.BooleanVar() for _ in range(10)]
        
        # Group Selectors for 2D (Bộ: 15, Kép: 4, Zodiac: 12)
        self.selector_vars_2d_bo = [tk.BooleanVar() for _ in range(15)]
        self.selector_vars_2d_kep = [tk.BooleanVar() for _ in range(5)]
        self.selector_vars_2d_zodiac = [tk.BooleanVar() for _ in range(12)]

        # New Multi-way Scan Selectors
        self.selector_vars_multi_cham = [tk.BooleanVar() for _ in range(10)]
        self.selector_vars_multi_tong = [tk.BooleanVar() for _ in range(10)]
        self.selector_vars_3d_kep = [tk.BooleanVar() for _ in range(9)]
        
        all_vars = self.selector_vars_3d_cham + self.selector_vars_3d_tong + self.selector_vars_3d_tram + \
                   self.selector_vars_4d_cham + self.selector_vars_4d_tong + \
                   self.selector_vars_4d_ngan + \
                   self.selector_vars_2d_cham + self.selector_vars_2d_tong + self.selector_vars_2d_hieu + \
                   self.selector_vars_2d_bo + self.selector_vars_2d_kep + self.selector_vars_2d_zodiac + \
                   self.selector_vars_multi_cham + self.selector_vars_multi_tong + self.selector_vars_3d_kep

        
        for var in all_vars:
            var.trace_add('write', lambda *args: self.update_selector_results())

        # Column 1: 3D Selectors
        col3d = tk.Frame(control_wrapper, bg=self.secondary_bg, bd=1, relief=tk.GROOVE)
        col3d.grid(row=0, column=0, sticky='nsew', padx=5)
        tk.Label(col3d, text="--- BỘ CHỌN 3D ---", bg=self.secondary_bg, fg="#f1c40f", 
                 font=('Segoe UI', 10, 'bold')).pack(pady=2)
        self.sugg_3d_cham = self.create_cb_row(col3d, "Chạm 3D:", self.selector_vars_3d_cham, "#f1c40f")
        self.sugg_3d_tong = self.create_cb_row(col3d, "Tổng 3D:", self.selector_vars_3d_tong, "#f1c40f")
        self.sugg_3d_tram = self.create_cb_row(col3d, "Hàng Trăm:", self.selector_vars_3d_tram, "#e67e22")
        
        # Level filter
        level_filter_row = tk.Frame(col3d, bg=self.secondary_bg)
        level_filter_row.pack(fill=tk.X, padx=5, pady=5)
        tk.Label(level_filter_row, text="Lọc Mức:", bg=self.secondary_bg, fg="#f1c40f", 
                 font=('Segoe UI', 9, 'bold')).pack(side=tk.LEFT, padx=5)
        self.level_2d_filter = ttk.Combobox(level_filter_row, values=["Tất cả"], 
                                      state="readonly", width=12, font=('Segoe UI', 8))
        self.level_2d_filter.set("Tất cả")
        self.level_2d_filter.pack(side=tk.LEFT, padx=5)
        
        def on_level_change(e):
            # Update selector results to refresh "⭐ DÀN 2D TỪ MỨC CAO"
            self.update_selector_results()
            # Re-render combined prediction without reloading data
        self.level_2d_filter.bind("<<ComboboxSelected>>", on_level_change)

        # Column 2: 4D Selectors
        col4d = tk.Frame(control_wrapper, bg=self.secondary_bg, bd=1, relief=tk.GROOVE)
        col4d.grid(row=0, column=1, sticky='nsew', padx=5)
        
        control_wrapper.columnconfigure(0, weight=1)
        control_wrapper.columnconfigure(1, weight=1)

        tk.Label(col4d, text="--- BỘ CHỌN 4D ---", bg=self.secondary_bg, fg="#3498db", 
                 font=('Segoe UI', 10, 'bold')).pack(pady=2)
        self.sugg_4d_cham = self.create_cb_row(col4d, "Chạm 4D:", self.selector_vars_4d_cham, "#3498db")
        self.sugg_4d_tong = self.create_cb_row(col4d, "Tổng 4D:", self.selector_vars_4d_tong, "#3498db")
        self.sugg_4d_ngan = self.create_cb_row(col4d, "Hàng Nghìn:", self.selector_vars_4d_ngan, "#e74c3c")
        
        # Action row
        action_frame = tk.Frame(main_container, bg=self.secondary_bg)
        action_frame.pack(fill=tk.X, pady=2)
        
        self.khan_3d_label = tk.Label(action_frame, text="Top Khan 3D: --", bg=self.secondary_bg, fg="#f1c40f", font=('Segoe UI', 8))
        self.khan_3d_label.pack(side=tk.LEFT, padx=10)
        
        tk.Button(action_frame, text="Xóa Tất Cả", command=self.clear_selector_checkboxes,
                  bg="#c0392b", fg="white", font=('Segoe UI', 8, 'bold'), padx=10, pady=1).pack(side=tk.LEFT, expand=True)
        
        self.khan_4d_label = tk.Label(action_frame, text="Top Khan 4D: --", bg=self.secondary_bg, fg="#e74c3c", font=('Segoe UI', 8))
        self.khan_4d_label.pack(side=tk.RIGHT, padx=10)
        
        # --- BOTTOM: DISPLAY AREA (SPLIT) ---
        display_container = tk.Frame(main_container, bg=self.bg_color)
        display_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        
        tk.Label(display_container, text="💡 Hướng dẫn: Nhấp đúp vào bất kỳ dãy số nào để copy nhanh.", 
                 bg=self.bg_color, fg="#bdc3c7", font=('Segoe UI', 8, 'italic')).pack(pady=(0, 5))
        
        
        # --- NEW: 2D SELECTORS ---
        mid_frame = tk.Frame(display_container, bg=self.bg_color)
        mid_frame.pack(fill=tk.X, pady=(0, 5))
        mid_frame.columnconfigure(0, weight=1) # 2D
        mid_frame.columnconfigure(1, weight=1) # Scan

        # 2D Selectors Frame - Spans 2 rows to match right side
        sel2d_frame = tk.LabelFrame(mid_frame, text="BỘ CHỌN 2D", bg=self.bg_color, fg="#2ecc71", font=('Segoe UI', 9, 'bold'))
        sel2d_frame.grid(row=0, column=0, rowspan=2, sticky='nsew', padx=5)
        
        self.sugg_2d_cham = self.create_cb_row(sel2d_frame, "Chạm 2D:", self.selector_vars_2d_cham, "#2ecc71")
        self.sugg_2d_tong = self.create_cb_row(sel2d_frame, "Tổng 2D:", self.selector_vars_2d_tong, "#2ecc71")
        self.sugg_2d_hieu = self.create_cb_row(sel2d_frame, "Hiệu 2D:", self.selector_vars_2d_hieu, "#2ecc71")
        
        # Add Group Tracking Rows (Bộ, Kép, Giáp) with actual names as labels
        bo_labels = list(BO_DICT.keys())
        kep_labels = ["K.Âm", "K.Bằng", "K.Lệch", "S.Kép", "Không kép"]
        zodiac_labels = list(ZODIAC_DICT.keys())
        
        self.sugg_2d_bo = self.create_cb_row(sel2d_frame, "Bộ:", self.selector_vars_2d_bo, "#e67e22", labels=bo_labels, wrap_at=5)
        self.sugg_2d_kep = self.create_cb_row(sel2d_frame, "Kép:", self.selector_vars_2d_kep, "#e67e22", labels=kep_labels)
        self.sugg_2d_zodiac = self.create_cb_row(sel2d_frame, "Giáp:", self.selector_vars_2d_zodiac, "#e67e22", labels=zodiac_labels, wrap_at=6)

        # MULTI-WAY SCAN SELECTOR
        mid_frame.rowconfigure(0, weight=1)
        mid_frame.rowconfigure(1, weight=1)
        
        sel_multi_frame = tk.LabelFrame(mid_frame, text="BỘ CHỌN 2-3-4D (Scan)", bg=self.bg_color, fg="#f1c40f", font=('Segoe UI', 9, 'bold'))
        sel_multi_frame.grid(row=0, column=1, sticky='nsew', padx=5, pady=(0, 2))
        
        # Add mode sync dropdown inside Scan frame
        scan_mode_row = tk.Frame(sel_multi_frame, bg=self.secondary_bg)
        scan_mode_row.pack(fill=tk.X, padx=5, pady=2)
        tk.Label(scan_mode_row, text="Chế độ:", bg=self.secondary_bg, fg="#f1c40f", font=('Segoe UI', 9, 'bold')).pack(side=tk.LEFT, padx=5)
        mode_sync_combo = ttk.Combobox(scan_mode_row, textvariable=self.scan_mode_var, values=["2D", "3D", "4D"], state='readonly', width=5)
        mode_sync_combo.pack(side=tk.LEFT, padx=5)
        # Trace handles update_selector_results and scan_two_way
        
        self.sugg_multi_cham = self.create_cb_row(sel_multi_frame, "C.Scan:", self.selector_vars_multi_cham, "#f1c40f")
        self.sugg_multi_tong = self.create_cb_row(sel_multi_frame, "T.Scan:", self.selector_vars_multi_tong, "#f1c40f")
        
        # 3D SPECIAL/KEP Frame - Placed under Scan
        sel_3d_special_frame = tk.LabelFrame(mid_frame, text="BỘ CHỌN 3D (Đặc Biệt/Kép)", bg=self.bg_color, fg="#f1c40f", font=('Segoe UI', 9, 'bold'))
        sel_3d_special_frame.grid(row=1, column=1, sticky='nsew', padx=5, pady=(2, 0))
        
        kep_3d_labels = ["AAB", "ABA", "BAA", "TH.Kép", "L.Bằng", "L.Lệch", "L.Am", "TH.Liên", "3D còn lại"]
        self.sugg_3d_kep = self.create_cb_row(sel_3d_special_frame, "3D Kép:", self.selector_vars_3d_kep, "#f1c40f", 
                                             labels=kep_3d_labels, wrap_at=4, sugg_below=True, sugg_height=2)
        
        # --- RESULT FRAMES ---
        result_area = tk.Frame(display_container, bg=self.bg_color)
        result_area.pack(fill=tk.BOTH, expand=True)
        result_area.columnconfigure(0, weight=1) # 2D
        result_area.columnconfigure(1, weight=2) # 3D
        result_area.columnconfigure(2, weight=2) # 4D
        result_area.rowconfigure(0, weight=1)

        # Left: 2D / Combined Results
        self.frame_2d = tk.LabelFrame(result_area, text="KẾT QUẢ 2D (Combined)", bg=self.bg_color, fg="#2ecc71", font=('Segoe UI', 9, 'bold'))
        self.frame_2d.grid(row=0, column=0, sticky='nsew', padx=(0, 5))
        self.selector_2d_text = scrolledtext.ScrolledText(self.frame_2d, bg=self.bg_color, fg="white", font=('Consolas', 9), wrap=tk.WORD)
        self.selector_2d_text.pack(fill=tk.BOTH, expand=True)
        
        # Center: 3D Results
        self.frame_3d = tk.LabelFrame(result_area, text="KẾT QUẢ 3D", bg=self.bg_color, fg="#f1c40f", font=('Segoe UI', 9, 'bold'))
        self.frame_3d.grid(row=0, column=1, sticky='nsew', padx=(0, 5))
        self.selector_3d_text = scrolledtext.ScrolledText(self.frame_3d, bg=self.bg_color, fg="white", font=('Consolas', 9), wrap=tk.WORD)
        self.selector_3d_text.pack(fill=tk.BOTH, expand=True)
        
        # Right: 4D Results
        self.frame_4d = tk.LabelFrame(result_area, text="KẾT QUẢ 4D", bg=self.bg_color, fg="#3498db", font=('Segoe UI', 9, 'bold'))
        self.frame_4d.grid(row=0, column=2, sticky='nsew', padx=(0, 0))
        self.selector_4d_text = scrolledtext.ScrolledText(self.frame_4d, bg=self.bg_color, fg="white", font=('Consolas', 9), wrap=tk.WORD)
        self.selector_4d_text.pack(fill=tk.BOTH, expand=True)
        
        # Tags for formatting
        for txt in [self.selector_3d_text, self.selector_4d_text, self.selector_2d_text]:
            txt.tag_configure('header', foreground="#3498db", font=('Segoe UI', 10, 'bold'))
            txt.tag_configure('level', foreground="#f1c40f", font=('Segoe UI', 9, 'bold'))
            txt.tag_configure('numbers', foreground="white")
            # Bind double click for silent copy
            txt.bind('<Double-Button-1>', self._on_selector_text_double_click)

    def _on_selector_text_double_click(self, event):
        """Silently copy the entire line/sequence of numbers on double click."""
        widget = event.widget
        index = widget.index(f"@{event.x},{event.y}")
        tags = widget.tag_names(index)
        
        if 'numbers' in tags:
            # Find the full range of the 'numbers' tag at this position
            ranges = widget.tag_ranges('numbers')
            for i in range(0, len(ranges), 2):
                start, end = ranges[i], ranges[i+1]
                if widget.compare(start, "<=", index) and widget.compare(index, "<=", end):
                    # Get the whole sequence string
                    text_to_copy = widget.get(start, end).strip()
                    if text_to_copy:
                        self.root.clipboard_clear()
                        self.root.clipboard_append(text_to_copy)
                    break

    # ========================================================================
    # TAB 5: PHÂN TÍCH 2 CHIỀU
    # ========================================================================
    def create_two_way_analysis_view(self):
        tab = self.two_way_frame
        
        header = ttk.Label(tab, text="Phân tích Đa Chiều (MB ↔ MN/MT)", style='Title.TLabel')
        header.pack(pady=10)
        
        frame_dir = tk.LabelFrame(tab, text="Chọn chiều phân tích", bg=self.bg_color, fg=self.accent_color, font=('Segoe UI', 10, 'bold'))
        frame_dir.pack(fill='x', padx=10, pady=5)
        
        ttk.Radiobutton(frame_dir, text="MB ➔ MN/MT", variable=self.two_way_direction, value="MB", command=self.update_two_way_ui).pack(side='left', padx=20)
        ttk.Radiobutton(frame_dir, text="MN/MT ➔ MB", variable=self.two_way_direction, value="MN", command=self.update_two_way_ui).pack(side='left', padx=20)
        
        # Scan Mode: 2D/3D/4D
        ttk.Label(frame_dir, text="Chế độ:", foreground=self.accent_color).pack(side='left', padx=(30, 5))
        mode_combo = ttk.Combobox(frame_dir, textvariable=self.scan_mode_var, values=["2D", "3D", "4D"], state='readonly', width=5)
        mode_combo.pack(side='left', padx=5)
        # Trace handles scan_two_way and update_selector_results
        
        # Nhị hợp vs Chọn lọc
        ttk.Label(frame_dir, text="Tạo dàn:", foreground=self.accent_color).pack(side='left', padx=(20, 5))
        ttk.Radiobutton(frame_dir, text="Nhị hợp", variable=self.scan_gen_mode, value="Nhị hợp").pack(side='left', padx=5)
        ttk.Radiobutton(frame_dir, text="Chọn lọc", variable=self.scan_gen_mode, value="Chọn lọc").pack(side='left', padx=5)
        
        self.two_way_scan_btn = ttk.Button(frame_dir, text="🚀 Quét", command=self.scan_two_way, width=10)
        self.two_way_scan_btn.pack(side='left', padx=20)
        self.two_way_info = tk.Label(tab, text="", fg=self.accent_color, bg=self.bg_color, wraplength=1000)
        self.two_way_info.pack(fill='x', padx=10, pady=5)
        
        self.two_way_controls = ttk.Frame(tab)
        self.two_way_controls.pack(fill='x', padx=10, pady=5)
        
        self.two_way_progress = ttk.Progressbar(tab, mode='determinate')
        self.two_way_progress.pack(fill='x', padx=10, pady=(0, 5))
        
        # Split layout: Left (Treeview), Right (Analysis)
        pane = ttk.PanedWindow(tab, orient=tk.HORIZONTAL)
        pane.pack(fill='both', expand=True, padx=10, pady=5)
        
        frame_tree = ttk.Frame(pane)
        pane.add(frame_tree, weight=2)
        
        # New Scrollable Table Implementation
        self.two_way_canvas = tk.Canvas(frame_tree, bg=self.bg_color, highlightthickness=0)
        scroll_y = ttk.Scrollbar(frame_tree, orient='vertical', command=self.two_way_canvas.yview)
        scroll_x = ttk.Scrollbar(frame_tree, orient='horizontal', command=self.two_way_canvas.xview)
        
        self.two_way_table_frame = tk.Frame(self.two_way_canvas, bg=self.bg_color)
        self.two_way_canvas.create_window((0, 0), window=self.two_way_table_frame, anchor='nw')
        
        self.two_way_canvas.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        
        scroll_y.pack(side='right', fill='y')
        scroll_x.pack(side='bottom', fill='x')
        self.two_way_canvas.pack(side='left', fill='both', expand=True)
        
        # Support mousewheel
        self._setup_canvas_mousewheel(self.two_way_canvas)
        
        # When frame size changes, update canvas scrollregion
        self.two_way_table_frame.bind("<Configure>", lambda e: self.two_way_canvas.configure(scrollregion=self.two_way_canvas.bbox("all")))

        frame_ana = ttk.Frame(pane)
        pane.add(frame_ana, weight=1)
        
        header_ana = ttk.Frame(frame_ana)
        header_ana.pack(fill='x')
        ttk.Label(header_ana, text="💡 GỢI Ý & BỘ SỐ:", font=('Segoe UI', 10, 'bold'), foreground=self.accent_color).pack(side='left', pady=5)
        
        self.tab3_source = tk.StringVar(value="tab2")
        ttk.Radiobutton(header_ana, text="Tự động", variable=self.tab3_source, value="tab2").pack(side='right', padx=5)
        ttk.Radiobutton(header_ana, text="Nhập tay", variable=self.tab3_source, value="manual").pack(side='right', padx=5)

        self.tab3_input = scrolledtext.ScrolledText(frame_ana, height=4, wrap='word', bg=self.secondary_bg, fg=self.fg_color, font=('Consolas', 10))
        self.tab3_input.pack(fill='x', pady=5)
        self.tab3_input.insert("1.0", "Nhập dàn số ở đây nếu chọn 'Nhập tay'...")
        
        # Action Buttons for Quick Selection
        btn_frame = ttk.Frame(frame_ana)
        btn_frame.pack(fill='x', pady=2)
        ttk.Button(btn_frame, text="📊 Phân tích", command=self.analyze_frequency_tab, width=12).pack(side='left', padx=2)
        ttk.Button(btn_frame, text="✅ Áp dụng Chạm HOT", command=lambda: self.apply_hot_to_selector('cham'), width=18).pack(side='left', padx=2)
        ttk.Button(btn_frame, text="✅ Áp dụng Tổng HOT", command=lambda: self.apply_hot_to_selector('tong'), width=18).pack(side='left', padx=2)

        self.tab3_suggest = scrolledtext.ScrolledText(frame_ana, wrap='word', bg=self.secondary_bg, fg=self.fg_color, font=('Consolas', 10))
        self.tab3_suggest.pack(fill='both', expand=True)
        
        self.update_two_way_ui()

    def update_two_way_ui(self):
        for widget in self.two_way_controls.winfo_children(): widget.destroy()
        direction = self.two_way_direction.get()
        
        if direction == "MB":
            self.two_way_info.config(text="🎯 Lấy kết quả MB ghép dàn, quét nổ MN/MT trong 7 ngày tới.")
            
            f_cfg = ttk.Frame(self.two_way_controls)
            f_cfg.pack(fill='x')
            
            ttk.Label(f_cfg, text="Ngày MB:").pack(side='left', padx=5)
            self.two_way_date_mb = ttk.Combobox(f_cfg, state='readonly', width=15)
            self.two_way_date_mb.pack(side='left', padx=5)
            self.two_way_date_mb.bind('<<ComboboxSelected>>', lambda e: self.scan_two_way())
            
            ttk.Label(f_cfg, text="Miền Soi:").pack(side='left', padx=5)
            self.two_way_region_mb = ttk.Combobox(f_cfg, values=["Miền Nam", "Miền Trung", "Cả 2 Miền"], state='readonly', width=10)
            self.two_way_region_mb.current(0)
            self.two_way_region_mb.pack(side='left', padx=5)
            self.two_way_region_mb.bind('<<ComboboxSelected>>', lambda e: self.scan_two_way())
            
            ttk.Label(f_cfg, text="Phạm vi:").pack(side='left', padx=5)
            self.two_way_scope_mb = ttk.Combobox(f_cfg, values=["ĐB", "ĐB + G8", "G8", "G1", "Tất cả"], state='readonly', width=10)
            self.two_way_scope_mb.current(0)
            self.two_way_scope_mb.pack(side='left', padx=5)
            self.two_way_scope_mb.bind('<<ComboboxSelected>>', lambda e: self.scan_two_way())
            
            self.populate_two_way_mb_dates()
        else:
            self.two_way_info.config(text="🎯 Mục tiêu: Lấy kết quả MN/MT (ĐB, G8...) ghép dàn, sau đó quét xem dàn đó có nổ ở MB (ĐB/G1...) trong 7 ngày tới không.")
            
            f_cfg = ttk.Frame(self.two_way_controls)
            f_cfg.pack(fill='x')
            
            ttk.Label(f_cfg, text="Miền Gốc:").pack(side='left', padx=5)
            self.two_way_source_region_mnmt = ttk.Combobox(f_cfg, values=["Miền Nam", "Miền Trung", "Cả 2 Miền"], state='readonly', width=12)
            self.two_way_source_region_mnmt.current(0)
            self.two_way_source_region_mnmt.pack(side='left', padx=5)
            self.two_way_source_region_mnmt.bind('<<ComboboxSelected>>', lambda e: self.populate_two_way_mnmt_dates())

            ttk.Label(f_cfg, text="Thứ:").pack(side='left', padx=5)
            self.two_way_weekday = ttk.Combobox(f_cfg, values=self.WEEKDAYS, state='readonly', width=10)
            self.two_way_weekday.current(5) # Thứ 7
            self.two_way_weekday.bind('<<ComboboxSelected>>', lambda e: self.populate_two_way_mnmt_dates())
            self.two_way_weekday.pack(side='left', padx=5)
            
            ttk.Label(f_cfg, text="Ngày:").pack(side='left', padx=5)
            self.two_way_date_mnmt = ttk.Combobox(f_cfg, state='readonly', width=15)
            self.two_way_date_mnmt.pack(side='left', padx=5)
            self.two_way_date_mnmt.bind('<<ComboboxSelected>>', lambda e: self.scan_two_way())
            
            ttk.Label(f_cfg, text="Phạm vi soi MB:").pack(side='left', padx=5)
            self.two_way_scope_mb_target = ttk.Combobox(f_cfg, values=["ĐB", "Đầu ĐB", "G1", "Đầu G1"], state='readonly', width=10)
            self.two_way_scope_mb_target.current(0)
            self.two_way_scope_mb_target.pack(side='left', padx=5)
            self.two_way_scope_mb_target.bind('<<ComboboxSelected>>', lambda e: self.scan_two_way())
            self.populate_two_way_mnmt_dates()

    def create_frequency_tab_view(self):
        """Create the Frequency (Tần Suất) tab UI."""
        # Top control frame
        controls = ttk.Frame(self.freq_frame)
        controls.pack(fill=tk.X, padx=10, pady=5)
        
        ttk.Label(controls, text="Nguồn dữ liệu:", font=('Segoe UI', 10, 'bold')).pack(side=tk.LEFT, padx=5)
        
        self.freq_source_var = tk.StringVar(value="BOTH")
        ttk.Radiobutton(controls, text="Cả 2 (ĐT + TT)", variable=self.freq_source_var, value="BOTH", 
                        command=self.render_frequency_tables).pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(controls, text="Chỉ Điện Toán", variable=self.freq_source_var, value="DT", 
                        command=self.render_frequency_tables).pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(controls, text="Chỉ Thần Tài", variable=self.freq_source_var, value="TT", 
                        command=self.render_frequency_tables).pack(side=tk.LEFT, padx=10)
        
        ttk.Label(controls, text="Chu kỳ: Rolling 7 ngày", font=('Segoe UI', 9)).pack(side=tk.RIGHT, padx=20)

        # Paned Window for Digit and Pair tables
        pane = ttk.PanedWindow(self.freq_frame, orient=tk.VERTICAL)
        pane.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Digit Frequency Table
        digit_frame = ttk.LabelFrame(pane, text="📊 Tần Suất Chạm (0-9)")
        pane.add(digit_frame, weight=1)
        
        self.digit_freq_canvas = tk.Canvas(digit_frame, bg="#1a1a1a", highlightthickness=0)
        digit_v_scroll = ttk.Scrollbar(digit_frame, orient=tk.VERTICAL, command=self.digit_freq_canvas.yview)
        digit_h_scroll = ttk.Scrollbar(digit_frame, orient=tk.HORIZONTAL, command=self.digit_freq_canvas.xview)
        self.digit_freq_canvas.configure(yscrollcommand=digit_v_scroll.set, xscrollcommand=digit_h_scroll.set)
        
        digit_v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        digit_h_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        self.digit_freq_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Pair Frequency Table
        pair_frame = ttk.LabelFrame(pane, text="📊 Tần Suất Cặp (00-99)")
        pane.add(pair_frame, weight=1)
        
        self.pair_freq_canvas = tk.Canvas(pair_frame, bg="#1a1a1a", highlightthickness=0)
        pair_v_scroll = ttk.Scrollbar(pair_frame, orient=tk.VERTICAL, command=self.pair_freq_canvas.yview)
        pair_h_scroll = ttk.Scrollbar(pair_frame, orient=tk.HORIZONTAL, command=self.pair_freq_canvas.xview)
        self.pair_freq_canvas.configure(yscrollcommand=pair_v_scroll.set, xscrollcommand=pair_h_scroll.set)
        
        pair_v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        pair_h_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        self.pair_freq_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Mouse wheel and selection bindings for both
        for cv in [self.digit_freq_canvas, self.pair_freq_canvas]:
            self._setup_canvas_mousewheel(cv)
            self._setup_canvas_selection(cv)
            cv.bind('<Double-1>', lambda e, c=cv: self._on_canvas_double_click_copy(e, c))
            # Right-click menu
            cv.bind('<Button-3>', lambda e, c=cv: self._show_canvas_context_menu(e, c))

    def _show_canvas_context_menu(self, event, canvas):
        """Show a right-click menu for copying."""
        menu = tk.Menu(self.root, tearoff=0)
        
        # Determine what's under the cursor
        item = canvas.find_closest(canvas.canvasx(event.x), canvas.canvasy(event.y))
        tags = canvas.gettags(item)
        cell_text = ""
        for t in tags:
            if t.startswith("copy_"):
                cell_text = t[5:].replace("__NL__", "\n")
                break
        
        if cell_text:
            menu.add_command(label=f"Sao chép ô: {cell_text[:20]}...", command=lambda: self._copy_text_to_clipboard(cell_text, canvas, item))
            menu.add_separator()
            
        # Check if there's a multi-selection
        sel_items = canvas.find_withtag("selected_item")
        if sel_items:
            menu.add_command(label=f"Sao chép vùng đã chọn ({len(sel_items)} ô)", command=lambda: self._copy_selected_area(canvas))
        
        menu.tk_popup(event.x_root, event.y_root)

    def _copy_text_to_clipboard(self, text, canvas=None, item=None):
        """Copy text to clipboard and show visual feedback."""
        if not text: return
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        
        if canvas and item:
            # Feedback on the specific cell
            old_width = canvas.itemcget(item, "width")
            canvas.itemconfig(item, outline='red', width=2)
            self.root.after(300, lambda: canvas.itemconfig(item, outline='#dcdcdc', width=old_width))
            
            # Floating "Copied!" message
            cx, cy = canvas.canvasx(canvas.winfo_pointerx() - canvas.winfo_rootx()), canvas.canvasy(canvas.winfo_pointery() - canvas.winfo_rooty())
            fid = canvas.create_text(cx, cy - 20, text="Đã copy! ✅", fill="#2ecc71", font=('Segoe UI', 10, 'bold'), tags="feedback")
            self.root.after(800, lambda: canvas.delete(fid))

    def _copy_selected_area(self, canvas):
        """Aggregate and copy text from all selected items."""
        items = canvas.find_withtag("selected_item")
        if not items: return
        
        items_pos = []
        for it in items:
            coords = canvas.coords(it)
            items_pos.append((coords[1], coords[0], it))
        items_pos.sort()
        
        txts = []
        for _, _, it in items_pos:
            val = canvas.itemcget(it, "text")
            if val and val != "-": txts.append(val)
            
        if txts:
            self._copy_text_to_clipboard(" ".join(txts), canvas, items[0])

    def _on_canvas_double_click_copy(self, event, canvas):
        """Silently copy cell text content to clipboard on double click."""
        item = canvas.find_closest(canvas.canvasx(event.x), canvas.canvasy(event.y))
        tags = canvas.gettags(item)
        for t in tags:
            if t.startswith("copy_"):
                content = t[5:].replace("__NL__", "\n")
                self._copy_text_to_clipboard(content, canvas, item)
                break

    def _setup_canvas_selection(self, canvas):
        """Setup drag-to-select and Ctrl+C copy logic."""
        canvas.sel_start = None
        canvas.sel_rect = None
        
        def _start_sel(event):
            canvas.delete("sel_overlay")
            # Clear previous selection tags from items
            for item in canvas.find_withtag("selected_item"):
                canvas.dtag(item, "selected_item")
                
            canvas.sel_start = (canvas.canvasx(event.x), canvas.canvasy(event.y))
            canvas.sel_rect = canvas.create_rectangle(
                canvas.sel_start[0], canvas.sel_start[1], 
                canvas.sel_start[0], canvas.sel_start[1],
                fill="#3498db", stipple="gray25", outline="#2980b9", tags="sel_overlay"
            )

        def _drag_sel(event):
            if not canvas.sel_start: return
            x2, y2 = canvas.canvasx(event.x), canvas.canvasy(event.y)
            canvas.coords(canvas.sel_rect, canvas.sel_start[0], canvas.sel_start[1], x2, y2)

        def _end_sel(event):
            if not canvas.sel_rect: return
            coords = canvas.coords(canvas.sel_rect)
            # Find all text items overlapping the selection rectangle
            overlapping = canvas.find_overlapping(*coords)
            for item in overlapping:
                if canvas.type(item) == "text":
                    canvas.addtag_withtag("selected_item", item)
            
        def _copy_sel(event):
            selected_texts = []
            # Gather text from items tagged as selected
            items = canvas.find_withtag("selected_item")
            # Sort by Y then X to maintain reading order
            items_with_pos = []
            for item in items:
                pos = canvas.coords(item)
                items_with_pos.append((pos[1], pos[0], item))
            items_with_pos.sort()
            
            for _, _, item in items_with_pos:
                txt = canvas.itemcget(item, "text")
                if txt and txt != "-":
                    selected_texts.append(txt)
            
            if selected_texts:
                content = " ".join(selected_texts) # Or comma/newline? Space is safest for lottery numbers
                self.root.clipboard_clear()
                self.root.clipboard_append(content)
                
        canvas.bind("<Button-1>", _start_sel, add="+")
        canvas.bind("<B1-Motion>", _drag_sel, add="+")
        canvas.bind("<ButtonRelease-1>", _end_sel, add="+")
        # Global bind for Ctrl+C when mouse is over canvas
        canvas.bind("<Enter>", lambda _: canvas.bind_all("<Control-c>", _copy_sel))
        canvas.bind("<Leave>", lambda _: canvas.unbind_all("<Control-c>"))

    def _setup_canvas_mousewheel(self, canvas):
        """Setup mousewheel scrolling for a canvas (Windows/MacOS/Linux compatible)."""
        def _on_mousewheel(event):
            # Vertical scroll
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
            
        def _on_shift_mousewheel(event):
            # Horizontal scroll
            canvas.xview_scroll(int(-1 * (event.delta / 120)), "units")

        canvas.bind('<Enter>', lambda _: (
            canvas.bind_all("<MouseWheel>", _on_mousewheel),
            canvas.bind_all("<Shift-MouseWheel>", _on_shift_mousewheel)
        ))
        canvas.bind('<Leave>', lambda _: (
            canvas.unbind_all("<MouseWheel>"),
            canvas.unbind_all("<Shift-MouseWheel>")
        ))

    def _extract_numbers_from_data(self, row, source_type):
        """Extract digits and overlapping pairs from row data based on source selection."""
        digits = []
        pairs = []
        
        if source_type in ["DT", "BOTH"]:
            # DT123: [673, 41, 1] -> join -> 673411
            dt_nums = "".join(row.get('dt_numbers', []))
            if dt_nums:
                digits.extend(list(dt_nums))
                for i in range(len(dt_nums) - 1):
                    # Overlapping pairs
                    pairs.append(dt_nums[i:i+2])
                    
        if source_type in ["TT", "BOTH"]:
            # TT4: 1234 -> 12, 23, 34
            tt_num = row.get('tt_number', '')
            if tt_num and len(tt_num) >= 2:
                digits.extend(list(tt_num))
                for i in range(len(tt_num) - 1):
                    # Overlapping pairs
                    pairs.append(tt_num[i:i+2])
                    
        return digits, pairs

    def _calculate_rolling_frequency_stats(self, source_type="BOTH", window_size=7):
        """Calculate statistics over a sliding window (every day, window=7 days)."""
        region = self.region_var.get()
        data_source = self.master_data if region == "Miền Bắc" else self.station_data
        
        if not data_source:
            return []
            
        try: offset = self.backtest_var.get()
        except: offset = 0
        
        # Apply offset to skip newest days for backtesting
        data_to_use = data_source[offset:]
        
        results = []
        # Step by 1 to make it a sliding window ("gối đầu")
        for i in range(len(data_to_use)):
            window = data_to_use[i : i + window_size]
            if len(window) < window_size:
                break # Only full 7-day windows
                
            head_row = window[0] # The date for which we are showing the stats
            
            total_digits = []
            total_pairs = []
            for row in window:
                d, p = self._extract_numbers_from_data(row, source_type)
                total_digits.extend(d)
                total_pairs.extend(p)
                
            digit_counts = Counter(total_digits)
            pair_counts = Counter(total_pairs)
            
            # --- Digit Ranking by Levels ---
            digit_stats = {str(d): digit_counts.get(str(d), 0) for d in range(10)}
            # Group digits by their counts
            counts_to_digits = defaultdict(list)
            for d, c in digit_stats.items():
                if c > 0: counts_to_digits[c].append(d)
            # Sort unique counts in descending order
            unique_counts_d = sorted(counts_to_digits.keys(), reverse=True)
            # top_digits_levels will be a list of lists: [[level1_digits], [level2_digits], [level3_digits]]
            top_digits_levels = [sorted(counts_to_digits[count]) for count in unique_counts_d[:3]]
            
            # --- Pair Ranking by Levels ---
            pair_stats = {f"{p:02d}": pair_counts.get(f"{p:02d}", 0) for p in range(100)}
            # Group pairs by their counts
            counts_to_pairs = defaultdict(list)
            for p, c in pair_stats.items():
                if c > 0: counts_to_pairs[c].append(p)
            unique_counts_p = sorted(counts_to_pairs.keys(), reverse=True)
            # top_pairs_levels will be a list of lists
            top_pairs_levels = [sorted(counts_to_pairs[count]) for count in unique_counts_p[:2]]
            
            results.append({
                'date': head_row['date'],
                'dt_kq': "".join(head_row.get('dt_numbers', [])),
                'tt_kq': head_row.get('tt_number', ''),
                'digit_stats': digit_stats,
                'top_digits_levels': top_digits_levels, # [[L1], [L2], [L3]]
                'pair_stats': pair_stats,
                'top_pairs_levels': top_pairs_levels   # [[L1], [L2]]
            })
            
        return results

    def _draw_frequency_cell(self, canvas, x, y, w, h, text, bg="#1e1e1e", fg="white", font=None, tags=None):
        """Refactored cell drawing for Cyber Dark style."""
        if font is None: font = ('Consolas', 9)
        
        # Dim color for zero values
        if str(text).strip() == "0":
            fg = "#444444"
            
        # Draw background and subtle grid border
        rect_id = canvas.create_rectangle(x, y, x + w, y + h, fill=bg, outline='#333333', tags=tags)
        
        lines = str(text).split('\n')
        line_h = 13
        total_h = len(lines) * line_h
        start_y = y + (h - total_h) / 2 + 5
        
        for i, line in enumerate(lines):
            canvas.create_text(x + w/2, start_y + i*line_h, text=line, fill=fg, font=font, anchor='center', tags=tags)
        return rect_id

    def render_frequency_tables(self):
        """Main entry point to update Frequency tables."""
        if not self.master_data: return
        source_type = self.freq_source_var.get()
        stats = self._calculate_rolling_frequency_stats(source_type=source_type)
        if not stats: return

        self._render_digit_freq_canvas(stats)
        self._render_pair_freq_canvas(stats)

    def _render_digit_freq_canvas(self, stats):
        canvas = self.digit_freq_canvas
        canvas.delete("all")
        
        # Value-based levels: STT, Date, Res, Count 0 to Count 17+, Top 2 Summary
        STT_W, DATE_W, RES_W = 40, 75, 120
        COUNT_W = 35 
        TOP_W = 150 # Width for Top 2 digits summary
        NUM_COUNTS = 18 # 0 to 17
        COL_STT = 0
        COL_DATE = STT_W
        COL_RES = COL_DATE + DATE_W
        COL_START_COUNT = COL_RES + RES_W
        COL_TOP = COL_START_COUNT + (NUM_COUNTS * COUNT_W)
        TOTAL_W = COL_TOP + TOP_W + 10
        CELL_H = 40
        
        # Header - Cyber Dark Matrix Green
        h_bg = "#27ae60"
        self._draw_frequency_cell(canvas, COL_STT, 0, STT_W, CELL_H, "STT", h_bg, "white", ('Segoe UI', 9, 'bold'))
        self._draw_frequency_cell(canvas, COL_DATE, 0, DATE_W, CELL_H, "Ngày", h_bg, "white", ('Segoe UI', 9, 'bold'))
        self._draw_frequency_cell(canvas, COL_RES, 0, RES_W, CELL_H, "Kết Quả\n(ĐT/TT)", h_bg, "white", ('Segoe UI', 9, 'bold'))
        for c in range(NUM_COUNTS):
            header_text = str(c) if c < 17 else "17+"
            self._draw_frequency_cell(canvas, COL_START_COUNT + c*COUNT_W, 0, COUNT_W, CELL_H, header_text, h_bg, "white", ('Segoe UI', 10, 'bold'))
        self._draw_frequency_cell(canvas, COL_TOP, 0, TOP_W, CELL_H, "Top 2 Mức", h_bg, "white", ('Segoe UI', 9, 'bold'))
        
        y = CELL_H
        for idx, entry in enumerate(stats):
            digit_stats = entry['digit_stats']
            # Group digits BY their count
            count_to_digits = defaultdict(list)
            for d, count in digit_stats.items():
                count_to_digits[count].append(d)
            
            # Identify Top 3 levels for highlighting
            unique_counts = sorted(count_to_digits.keys(), reverse=True)
            top1_c = unique_counts[0] if len(unique_counts) > 0 else -1
            top2_c = unique_counts[1] if len(unique_counts) > 1 else -1
            top3_c = unique_counts[2] if len(unique_counts) > 2 else -1

            # STT, Date, Result columns use #2d2d2d separation
            sep_bg = "#2d2d2d"
            self._draw_frequency_cell(canvas, COL_STT, y, STT_W, CELL_H, str(idx+1), sep_bg, "#bdc3c7")
            self._draw_frequency_cell(canvas, COL_DATE, y, DATE_W, CELL_H, entry['date'][:5], sep_bg, "#bdc3c7", ('Segoe UI', 8))
            
            res_str = f"ĐT:{entry['dt_kq']}\nTT:{entry['tt_kq']}"
            res_copy = f"copy_{res_str.replace('\n', '__NL__')}"
            self._draw_frequency_cell(canvas, COL_RES, y, RES_W, CELL_H, res_str, sep_bg, "#c0392b", ('Consolas', 8), tags=(res_copy,))
            
            for c in range(NUM_COUNTS):
                digits = count_to_digits.get(c, [])
                if c == 17: # Handle 17+
                    for over_c in count_to_digits:
                        if over_c > 17: digits.extend(count_to_digits[over_c])
                    digits = sorted(list(set(digits)))
                
                bg, fg = "#1e1e1e", "white"
                # Level highlighters
                if c == top1_c: bg, fg = "#c0392b", "white" # Top 1: Dark Red
                elif c == top2_c and c != -1: bg, fg = "#d35400", "white" # M2: Dark Orange
                elif c == top3_c and c != -1: bg, fg = "#2980b9", "white" # M3: Blue
                
                text = ",".join(digits) if digits else ""
                tag = f"copy_{text}" if text else None
                self._draw_frequency_cell(canvas, COL_START_COUNT + c*COUNT_W, y, COUNT_W, CELL_H, text, bg, fg, ('Consolas', 9, 'bold'), tags=(tag,) if tag else None)
            
            # --- Top 2 Digits Summary ---
            l1_digits = sorted(count_to_digits.get(top1_c, []))
            l2_digits = sorted(count_to_digits.get(top2_c, []))
            l1_str = ",".join(l1_digits)
            l2_str = ",".join(l2_digits)
            top_text = l1_str + ((" | " + l2_str) if l2_str else "")
            
            copy_tag = f"copy_{top_text}"
            self._draw_frequency_cell(canvas, COL_TOP, y, TOP_W, CELL_H, top_text, "#5b2c6f", "white", ('Consolas', 10, 'bold'), tags=(copy_tag,))
            
            y += CELL_H
            
        canvas.configure(scrollregion=(0, 0, TOTAL_W, y))

    def _render_pair_freq_canvas(self, stats):
        canvas = self.pair_freq_canvas
        canvas.delete("all")
        
        # Value-based counts for pairs: STT, Date, Res, Count 0 to 5+, Top 2 Summary
        STT_W, DATE_W, RES_W = 40, 75, 120
        COUNT_W = 180 # Wider for list of pairs
        TOP_W = 250
        NUM_COUNTS = 6 # 0, 1, 2, 3, 4, 5+
        COL_STT = 0
        COL_DATE = STT_W
        COL_RES = COL_DATE + DATE_W
        COL_START_COUNT = COL_RES + RES_W
        COL_TOP = COL_START_COUNT + (NUM_COUNTS * COUNT_W)
        TOTAL_W = COL_TOP + TOP_W + 10
        CELL_H = 60
        
        # Header - Matrix Green
        h_bg = "#27ae60"
        self._draw_frequency_cell(canvas, COL_STT, 0, STT_W, CELL_H, "STT", h_bg, "white", ('Segoe UI', 9, 'bold'))
        self._draw_frequency_cell(canvas, COL_DATE, 0, DATE_W, CELL_H, "Ngày", h_bg, "white", ('Segoe UI', 9, 'bold'))
        self._draw_frequency_cell(canvas, COL_RES, 0, RES_W, CELL_H, "Kết Quả\n(ĐT/TT)", h_bg, "white", ('Segoe UI', 9, 'bold'))
        for c in range(NUM_COUNTS):
            header_text = f"Về {c} lần" if c < 5 else "Về 5+ lần"
            self._draw_frequency_cell(canvas, COL_START_COUNT + c*COUNT_W, 0, COUNT_W, CELL_H, header_text, h_bg, "white", ('Segoe UI', 9, 'bold'))
        self._draw_frequency_cell(canvas, COL_TOP, 0, TOP_W, CELL_H, "Top 2 Mức", h_bg, "white", ('Segoe UI', 9, 'bold'))
        
        y = CELL_H
        for idx, entry in enumerate(stats):
            pair_stats = entry['pair_stats']
            
            # Identify Top frequencies
            count_to_pairs = defaultdict(list)
            for p, count in pair_stats.items():
                count_to_pairs[count].append(p)
                
            unique_counts = sorted(count_to_pairs.keys(), reverse=True)
            top1_c = unique_counts[0] if len(unique_counts) > 0 else -1
            top2_c = unique_counts[1] if len(unique_counts) > 1 else -1

            sep_bg = "#2d2d2d"
            self._draw_frequency_cell(canvas, COL_STT, y, STT_W, CELL_H, str(idx+1), sep_bg, "#bdc3c7")
            self._draw_frequency_cell(canvas, COL_DATE, y, DATE_W, CELL_H, entry['date'][:5], sep_bg, "#bdc3c7", ('Segoe UI', 8))
            
            res_str = f"ĐT:{entry['dt_kq']}\nTT:{entry['tt_kq']}"
            res_copy = f"copy_{res_str.replace('\n', '__NL__')}"
            self._draw_frequency_cell(canvas, COL_RES, y, RES_W, CELL_H, res_str, sep_bg, "#c0392b", ('Consolas', 8), tags=(res_copy,))
            
            def fmt_pairs(plist, wrap=10):
                if not plist: return ""
                plist = sorted(plist)
                chunks = [plist[i:i+wrap] for i in range(0, len(plist), wrap)]
                return "\n".join([",".join(chunk) for chunk in chunks])

            for c in range(NUM_COUNTS):
                plist = count_to_pairs.get(c, [])
                if c == 5: # Handle 5+
                    for over_c in count_to_pairs:
                        if over_c > 5: plist.extend(count_to_pairs[over_c])
                    plist = sorted(list(set(plist)))
                
                bg, fg = "#1e1e1e", "white"
                if c == top1_c: bg, fg = "#c0392b", "white"
                elif c == top2_c and c != -1: bg, fg = "#d35400", "white"
                
                text = fmt_pairs(plist)
                tag = f"copy_{text.replace('\n', '__NL__')}" if text else None
                self._draw_frequency_cell(canvas, COL_START_COUNT + c*COUNT_W, y, COUNT_W, CELL_H, text, bg, fg, ('Consolas', 8), tags=(tag,) if tag else None)

            # Summary Top 2
            l1_pairs = sorted(count_to_pairs.get(top1_c, []))
            l2_pairs = sorted(count_to_pairs.get(top2_c, []))
            
            # Combine into a single list with a separator element to wrap naturally
            combined = l1_pairs + [" | "] + l2_pairs if l2_pairs else l1_pairs
            
            def wrap_combined(items, wrap=12):
                if not items: return ""
                # Special handling for " | " separator during join/wrap
                lines = []
                current_line = []
                for item in items:
                    current_line.append(item)
                    if len(current_line) >= wrap:
                        line_str = ",".join(current_line).replace(", | ,", " | ").replace(", | ", " | ").replace(" | ,", " | ")
                        lines.append(line_str)
                        current_line = []
                if current_line:
                    line_str = ",".join(current_line).replace(", | ,", " | ").replace(", | ", " | ").replace(" | ,", " | ")
                    lines.append(line_str)
                return "\n".join(lines)

            top_summary = wrap_combined(combined, wrap=14)
            copy_tag = f"copy_{top_summary}"
            self._draw_frequency_cell(canvas, COL_TOP, y, TOP_W, CELL_H, top_summary, "#5b2c6f", "white", ('Consolas', 9, 'bold'), tags=(copy_tag,))
            
            y += CELL_H
            
        canvas.configure(scrollregion=(0, 0, TOTAL_W, y))

    def populate_two_way_mb_dates(self):
        if not hasattr(self, 'two_way_date_mb') or self.two_way_date_mb is None: return
        if self.master_data:
            import datetime
            # Filter for Saturdays only (weekday() == 5)
            saturday_dates = []
            for d in self.master_data:
                try:
                    date_obj = datetime.datetime.strptime(d['date'], "%d/%m/%Y")
                    if date_obj.weekday() == 5:  # Saturday
                        saturday_dates.append(d['date'])
                except:
                    pass
            
            if saturday_dates:
                self.two_way_date_mb.config(values=saturday_dates)
                self.two_way_date_mb.current(0)  # Most recent Saturday (first in list)
            else:
                # Fallback to all dates if no Saturdays found
                dates = [d['date'] for d in self.master_data]
                self.two_way_date_mb.config(values=dates)
                if dates: self.two_way_date_mb.current(0)

    def populate_two_way_mnmt_dates(self):
        if not hasattr(self, 'two_way_date_mnmt') or self.two_way_date_mnmt is None: return
        # Fetch a sample station to get dates for that weekday
        region_opt = self.two_way_source_region_mnmt.get()
        region = "Miền Nam" if region_opt == "Miền Nam" else ("Miền Trung" if region_opt == "Miền Trung" else "Miền Nam")
        day = self.two_way_weekday.get()
        stations = get_stations_by_day(region, day)
        if not stations: return
        
        def _fetch_dates():
            try:
                rows = fetch_station_data(stations[0], total_days=60)
                valid_dates = [r['date'] for r in rows]
                self.root.after(0, lambda: self.two_way_date_mnmt.config(values=valid_dates))
                if valid_dates:
                    self.root.after(0, lambda: self.two_way_date_mnmt.current(0))
                    self.root.after(500, self._check_auto_scan)
            except: pass
        threading.Thread(target=_fetch_dates, daemon=True).start()

    def _check_auto_scan(self):
        """Automatically trigger scan if it's the first time and data is ready."""
        if self._initial_scan_done: return
        
        # We need master_data (MB) and either two_way_date_mb or two_way_date_mnmt 
        # based on current direction
        direction = self.two_way_direction.get()
        if direction == "MN": # MN/MT -> MB
            if self.master_data and hasattr(self, 'two_way_date_mnmt') and self.two_way_date_mnmt.get():
                self._initial_scan_done = True
                self.scan_two_way()
        else: # MB -> MN/MT
            if self.master_data and hasattr(self, 'two_way_date_mb') and self.two_way_date_mb.get():
                self._initial_scan_done = True
                self.scan_two_way()

    def scan_two_way(self):
        """Perform Multi-way Scan automatically."""
        if not hasattr(self, 'two_way_direction'): return
        direction = self.two_way_direction.get()
        if direction == "MB": self.scan_mb_to_mnmt_ui()
        else: self.scan_mnmt_to_mb_ui()

    def _on_scan_mode_changed(self):
        """Automatically trigger scan and update results when mode changes."""
        # Prevent recursion or redundant calls
        if getattr(self, '_is_syncing_scan', False): return
        self._is_syncing_scan = True
        try:
            # Only trigger if we have data
            if self.master_data or self.station_data:
                self.scan_two_way()
                self.update_selector_results()
        finally:
            self._is_syncing_scan = False

    def scan_mb_to_mnmt_ui(self):
        sel_date = self.two_way_date_mb.get()
        if not sel_date: return
        
        region_opt = self.two_way_region_mb.get()
        region_filter = "MN" if region_opt == "Miền Nam" else ("MT" if region_opt == "Miền Trung" else "BOTH")
        self.last_region_filter = region_filter

        mn_scope = self.two_way_scope_mb.get()
        self.last_mn_scope = mn_scope
        scope_map = {"Tất cả": list(range(18)), "ĐB": [0], "ĐB + G8": [0, 17], "G8": [17], "G1": [1]}
        target_indices = scope_map.get(mn_scope, [0])

        def _scan():
            self.root.after(0, lambda: self.two_way_progress.config(value=10))
            self.last_scan_direction = "MB_TO_MNMT"
            
            # Get prizes for MB date
            mb_row = next((d for d in self.master_data if d['date'] == sel_date), None)
            if not mb_row: return self.root.after(0, lambda: messagebox.showerror("Lỗi", "Không tìm thấy dữ liệu MB"))
            
            # Extract first 10 prizes: ĐB, G1, G2 (2), G3 (6)
            all_prizes = mb_row.get('all_prizes', [])
            if not all_prizes:
                return self.root.after(0, lambda: messagebox.showerror("Lỗi", "Dữ liệu MB chưa đầy đủ, vui lòng tải lại dữ liệu"))
            
            prizes_mb = all_prizes[:10]  # ĐB to G3.6 (indices 0-9)
            # Pad to at least 10
            prizes_mb = (prizes_mb + [""] * 10)[:10]

            if sel_date not in self.mnmt_cache:
                fetched, err = fetch_mnmt_data_7days(sel_date)
                if err: return self.root.after(0, lambda: messagebox.showerror("Lỗi", err))
                self.mnmt_cache[sel_date] = fetched
            
            mode = self.scan_mode_var.get()
            gen_mode = self.scan_gen_mode.get()
            res, err = scan_mb_to_mnmt(sel_date, prizes_mb, mn_target_indices=target_indices, fetched_data=self.mnmt_cache[sel_date], region_filter=region_filter, mode=mode, mode_gen=gen_mode)
            
            if res:
                all_nums = []
                for r in res:
                    # Collect ALL numbers from generated sets, not just hits
                    for x in r["Dàn số"].split(","):
                        if x.strip(): all_nums.append(x.strip())
                self.mn_mt_results = all_nums
                self.mn_mt_date = sel_date
                self.root.after(0, lambda: self.display_two_way_results(res))
                self.root.after(100, self.analyze_frequency_tab) # Auto-analyze
            self.root.after(0, lambda: self.two_way_progress.config(value=100))
            self.root.after(1000, lambda: self.two_way_progress.config(value=0))

        threading.Thread(target=_scan, daemon=True).start()

    def scan_mnmt_to_mb_ui(self):
        sel_date = self.two_way_date_mnmt.get()
        if not sel_date: return
        weekday = self.two_way_weekday.get()
        region_opt = self.two_way_source_region_mnmt.get()
        region_code = "MN" if region_opt == "Miền Nam" else ("MT" if region_opt == "Miền Trung" else "BOTH")
        mb_scope = self.two_way_scope_mb_target.get()
        self.last_mb_scope = mb_scope

        def _scan():
            self.last_scan_direction = "MNMT_TO_MB"
            # Map master_data to mb_dict for logic using all_prizes field
            mb_dict = {d['date']: d.get('all_prizes', [""]*27) for d in self.master_data}
            
            def progress(p, msg): self.root.after(0, lambda: self.two_way_progress.config(value=p*100))
            
            mode = self.scan_mode_var.get()
            gen_mode = self.scan_gen_mode.get()
            results = scan_multistation_subset(region_code, sel_date, mb_dict, weekday, mb_scope=mb_scope, progress_callback=progress, mode=mode, mode_gen=gen_mode)
            
            if results:
                all_nums = []
                for r in results:
                    # Collect ALL numbers from generated sets, not just hits
                    for x in r["Dàn số"].split(","):
                        if x.strip(): all_nums.append(x.strip())
                self.mn_mt_results = all_nums
                self.mn_mt_date = sel_date
                self.root.after(0, lambda: self.display_two_way_results(results))
                self.root.after(100, self.analyze_frequency_tab) # Auto-analyze
            self.root.after(1000, lambda: self.two_way_progress.config(value=0))

        threading.Thread(target=_scan, daemon=True).start()

    def display_two_way_results(self, results):
        # Clear previous results
        for widget in self.two_way_table_frame.winfo_children():
            widget.destroy()
            
        if not results:
            tk.Label(self.two_way_table_frame, text="Không có kết quả nào.", 
                     bg=self.bg_color, fg=self.fg_color, font=('Segoe UI', 10)).pack(pady=20)
            return
            
        # Direction
        direction = self.two_way_direction.get()
        
        # Sort results: Prioritize "Not Hit" (has_hit=False) first, then sort by "Số lượng" descending
        results.sort(key=lambda x: (x.get('has_hit', False), -(x.get('Số lượng', 0))))

        header_bg = self.accent_color
        hit_color = "#27ae60"
        miss_color = "#c0392b"
        wait_color = "#6b7280"
        
        # Headers Configuration
        if direction == "MB":
            headers = ["STT", "Tổ hợp MB", "Giá trị", "Dàn số", "SL", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "Chi tiết"]
            col_keys = ["Tổ hợp MB", "Giá trị giải", "Dàn số", "Số lượng", "Checklist", "Chi tiết"]
        else:
            headers = ["STT", "Đài", "Giá trị", "Giải ghép", "Dàn số", "SL", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "Ghi chú"]
            col_keys = ["Đài", "Giá trị giải", "Giải ghép", "Dàn số", "Số lượng", "T7 -> T6", "Ghi chú"]

        # Render Headers
        for i, h in enumerate(headers):
            # Base width settings
            if h == "STT": w = 5
            elif h in ["Dàn số", "Chi tiết", "Ghi chú"]: w = 30
            elif h in ["Giá trị", "Tổ hợp MB", "Giải ghép", "Đài"]: w = 15
            elif h == "SL": w = 6
            elif h.startswith("D"): w = 4 # Day columns
            else: w = 10
            
            lbl = tk.Label(self.two_way_table_frame, text=h, bg=header_bg, fg="white",
                          font=('Segoe UI', 9, 'bold'), width=w, relief='flat', pady=5)
            lbl.grid(row=0, column=i, sticky='nsew', padx=1, pady=1)

        # Render Rows
        for r_idx, r in enumerate(results, 1):
            row_bg = self.secondary_bg if r_idx % 2 == 0 else self.bg_color
            
            # STT
            tk.Label(self.two_way_table_frame, text=str(r_idx), bg=row_bg, fg="white",
                    font=('Consolas', 9), width=5).grid(row=r_idx, column=0, sticky='nsew', padx=1, pady=1)
            
            curr_col = 1
            checklist_key = "Checklist" if direction == "MB" else "T7 -> T6"
            
            for key in col_keys:
                val = r.get(key, "")
                
                if key == checklist_key:
                    # Split checklist into 7 columns
                    # Format is usually "✅ ❌ ⏳" or similar space-separated
                    icons = str(val).split()
                    # Ensure we have 7 icons
                    icons = (icons + ["⏳"] * 7)[:7]
                    
                    for day_idx, icon in enumerate(icons):
                        fg = "white"
                        bg_cell = row_bg
                        text_icon = icon
                        
                        if icon == "✅": bg_cell = hit_color
                        elif icon == "❌": bg_cell = miss_color
                        elif icon == "⏳": bg_cell = row_bg # Wait stays row bg
                        
                        # Center the icon
                        cell = tk.Label(self.two_way_table_frame, text=text_icon, bg=bg_cell, fg=fg,
                                       font=('Segoe UI', 10, 'bold'), width=4)
                        cell.grid(row=r_idx, column=curr_col + day_idx, sticky='nsew', padx=1, pady=1)
                    
                    curr_col += 7
                else:
                    # Normal text column
                    w_text = 30 if key in ["Dàn số", "Chi tiết", "Ghi chú"] else (15 if key in ["Giá trị giải", "Tổ hợp MB", "Giải ghép", "Đài"] else 6)
                    
                    # Truncate Dàn số if too long, or just let it expand
                    display_val = str(val)
                    if key == "Dàn số" and len(display_val) > 100:
                        display_val = display_val[:97] + "..."
                        
                    txt_lbl = tk.Label(self.two_way_table_frame, text=display_val, bg=row_bg, fg=self.fg_color,
                                     font=('Consolas', 9), width=w_text, anchor='w' if w_text > 6 else 'center', padx=5)
                    txt_lbl.grid(row=r_idx, column=curr_col, sticky='nsew', padx=1, pady=1)
                    
                    # Bind double click to copy
                    txt_lbl.bind("<Double-1>", lambda e, v=val: self.copy_to_clipboard(str(v)))
                    
                    curr_col += 1


    def copy_to_clipboard(self, text):
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        # Removed messagebox notification as per user request

    # (Legacy frequency UI method removed - merged into Phân Tích Đa Chiều)

        


    def analyze_frequency_tab(self):
        raw_nums = []
        mode = self.scan_mode_var.get()
        n_digits = int(mode[0]) if mode[0].isdigit() else 2
        
        # Store current scan mode for selector to use
        self.current_scan_mode = n_digits
        
        if self.tab3_source.get() == "manual":
            txt = self.tab3_input.get("1.0", "end-1c")
            for x in txt.replace(",", " ").replace(".", " ").split():
                if x.strip().isdigit() and len(x.strip()) <= n_digits: raw_nums.append(x.strip().zfill(n_digits))
        else: raw_nums = self.mn_mt_results
            
        # Check if we have data from scan results
        if not raw_nums:
            return messagebox.showwarning("Cảnh báo", "Chưa có dữ liệu. Vui lòng chạy quét (Scan) trước để tạo dàn số.")

        # Logic check nổ real-time
        def check_hit(type_check, val):
            direction = getattr(self, 'last_scan_direction', "MB_TO_MNMT")
            check_date_obj = None
            if self.mn_mt_date:
                try: check_date_obj = datetime.strptime(self.mn_mt_date, "%d/%m/%Y")
                except: pass
            
            if not check_date_obj: return ""
            
            # Get today's date for limiting range
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            
            if direction == "MB_TO_MNMT" and self.mn_mt_date in self.mnmt_cache:
                fetched = self.mnmt_cache[self.mn_mt_date]
                mn_scope = getattr(self, 'last_mn_scope', "ĐB")
                target_idx = [0] if mn_scope == "ĐB" else ([17] if mn_scope == "G8" else range(18))
                
                # We check from check_date_obj + 1 to check_date_obj + 7, capped at today
                max_check_date = min(check_date_obj + timedelta(days=7), today)
                
                hits = []
                for st, rows in fetched.items():
                    for r in rows:
                        row_date_obj = r.get('ObjDate')
                        # CRITICAL: Only check dates AFTER the scan date and NOT in the future
                        if not row_date_obj or not (check_date_obj < row_date_obj <= max_check_date):
                            continue
                            
                        prizes = r['Prizes']
                        for idx in target_idx:
                            if idx < len(prizes):
                                ln = get_lastN(prizes[idx], n_digits)
                                if not ln: continue
                                d_val, u_val = -1, -1
                                if n_digits == 2:
                                    d_val, u_val = int(ln[0]), int(ln[1])
                                v_int = int(val)
                                if (type_check == "exact" and val == ln) or \
                                   (n_digits == 2 and type_check == "cham" and (d_val == v_int or u_val == v_int)) or \
                                   (n_digits == 2 and type_check == "tong" and (d_val + u_val) % 10 == v_int):
                                    hits.append(f"{STATION_ABBREVIATIONS.get(st, st)} {r['Date'][:5]}")
                if hits: return f" [✅ {', '.join(sorted(list(set(hits)))[:2])}]"
            elif direction == "MNMT_TO_MB" and check_date_obj:
                mb_scope = getattr(self, 'last_mb_scope', "ĐB")
                # Limit loop to today
                for i in range(1, 8):
                    check_d = check_date_obj + timedelta(days=i)
                    if check_d > today: break
                    
                    d_str = check_d.strftime("%d/%m/%Y")
                    mb_row = next((d for d in self.master_data if d['date'] == d_str), None)
                    if mb_row:
                        target = get_lastN(mb_row['xsmb_full'], n_digits) if mb_scope == "ĐB" else get_lastN(mb_row['g1_full'], n_digits)
                        if not target: continue
                        d_val, u_val = -1, -1
                        if n_digits == 2:
                            d_val, u_val = int(target[0]), int(target[1])
                        v_int = int(val)
                        if (type_check == "exact" and val == target) or \
                           (n_digits == 2 and type_check == "cham" and (d_val == v_int or u_val == v_int)) or \
                           (n_digits == 2 and type_check == "tong" and (d_val + u_val) % 10 == v_int):
                            return f" [✅ {d_str[:5]}]"
            return ""

        counter = Counter(raw_nums)
        max_c = max(counter.values()) if counter else 0
        res_text = f"=== PHÂN TÍCH {len(raw_nums)} SỐ ===\n\n1. MỨC SỐ:\n"
        
        for m in range(max_c, 0, -1):
            grp = sorted([n for n, c in counter.items() if c == m])
            if grp:
                parts = []
                for num in grp:
                    h = check_hit("exact", num)
                    parts.append(f"{num}{h}")
                res_text += f"Mức {m} ({len(grp)} số): {', '.join(parts)}\n"
        
        if n_digits == 2:
            all_nums = set(f"{i:02d}" for i in range(100))
            missing = sorted(list(all_nums - set(raw_nums)))
            if missing:
                parts = []
                for num in missing:
                    h = check_hit("exact", num)
                    parts.append(f"{num}{h}")
                res_text += f"\nTrượt (Mức 0) ({len(missing)} số): {', '.join(parts)}\n"
        else:
            res_text += f"\n(Phân tích Trượt/Chạm/Tổng chỉ hỗ trợ chế độ 2D)\n"
        
        # 2. CHẠM/TỔNG HOT
        ana = analyze_aggregated_frequency(raw_nums, n_digits=n_digits)
        if ana:
            res_text += f"\n2. CHẠM & TỔNG {mode} HOT (FULL 0-9):\n"
            
            cham_stats, cham_top_list = ana['cham']
            tong_stats, tong_top_list = ana['tong']
            
            cham_display = []
            for i in range(10):
                h = check_hit('cham', str(i))
                count = cham_stats.get(i, 0)
                cham_display.append(f"{i}({count}){h}")
            res_text += f"🔥 CHẠM {mode}: " + ", ".join(cham_display) + "\n"
            
            tong_display = []
            for i in range(10):
                h = check_hit('tong', str(i))
                count = tong_stats.get(i, 0)
                tong_display.append(f"{i}({count}){h}")
            res_text += f"🔥 TỔNG {mode}: " + ", ".join(tong_display) + "\n"
            
            # Gợi ý bộ số (chỉ 2D)
            if n_digits == 2:
                res_text += "\n💡 GỢI Ý:\n"
                chams = [x[0] for x in ana['top_cham'][:3]]
                tongs = [x[0] for x in ana['top_tong'][:3]]
                dan_goi_y = [f"{i:02d}" for i in range(100) if int(f"{i:02d}"[0]) in chams or int(f"{i:02d}"[1]) in chams or (int(f"{i:02d}"[0])+int(f"{i:02d}"[1]))%10 in tongs]
                res_text += f"Dàn Gợi Ý ({len(dan_goi_y)} số): {', '.join(dan_goi_y)}\n"
            
            # Update Selector tab with rich suggestion data
            # IMPORTANT: Only update multi-way (Scan) suggestions here. 
            # Standard 2-3-4D suggestions should keep their historical Khan data.
            # Identify which digits/sums hit in the MB result for coloring
            cham_hits = [i for i in range(10) if check_hit("cham", str(i))]
            tong_hits = [i for i in range(10) if check_hit("tong", str(i))]
            
            # Pass (stats_dict, sorted_list, hits_today_list)
            self.root.after(10, lambda: self.update_rare_info_on_selector(
                multi_cham=(ana['cham'][0], ana['cham'][1], cham_hits),
                multi_tong=(ana['tong'][0], ana['tong'][1], tong_hits)
            ))
            
            # Store for Apply buttons
            self.last_ana_results = ana

        self.tab3_suggest.delete("1.0", "end")
        self.tab3_suggest.insert("1.0", res_text)
        
        # Trigger selector results update to display scan results in correct boxes
        self.root.after(50, self.update_selector_results)
        
    def update_rare_info_on_selector(self, khan_3d_str=None, khan_4d_str=None, 
                                     cham_3d=None, tong_3d=None, cham_4d=None, tong_4d=None,
                                     cham_2d=None, tong_2d=None, hieu_2d=None,
                                     multi_cham=None, multi_tong=None):

        """Cập nhật thông tin Khan và Gợi ý lên tab Bộ chọn"""
        # Note: All params can be tuples (gaps_dict, exceeding_list)
        
        if hasattr(self, 'khan_3d_label') and khan_3d_str is not None:
            display_str = khan_3d_str
            if isinstance(khan_3d_str, tuple) and len(khan_3d_str) >= 2:
                gaps_dict = khan_3d_str[0]
                top_digits = khan_3d_str[1]
                # Format: "2(15), 5(12), 6(10)..."
                display_parts = [f"{d}({gaps_dict[d]})" for d in top_digits]
                display_str = ", ".join(display_parts)
                # Store exceeding list for top 5 hundreds calculation
                self.exceeding_tram_3d = top_digits
            self.khan_3d_label.config(text=f"Top Khan 3D (Trăm): {display_str}")
            if hasattr(self, 'matrix_khan_3d_label'):
                self.matrix_khan_3d_label.config(text=f"Khan 3D: {display_str}")
            if hasattr(self, 'sugg_3d_tram'):
                self._update_rich_suggestion(self.sugg_3d_tram, khan_3d_str)

        if hasattr(self, 'khan_4d_label') and khan_4d_str is not None:
            display_str = khan_4d_str
            if isinstance(khan_4d_str, tuple) and len(khan_4d_str) >= 2:
                gaps_dict = khan_4d_str[0]
                top_digits = khan_4d_str[1]
                # Format: "4(20), 5(18), 7(15)..."
                display_parts = [f"{d}({gaps_dict[d]})" for d in top_digits]
                display_str = ", ".join(display_parts)
            self.khan_4d_label.config(text=f"Top Khan 4D (Nghìn): {display_str}")
            if hasattr(self, 'matrix_khan_4d_label'):
                self.matrix_khan_4d_label.config(text=f"Khan 4D: {display_str}")
            if hasattr(self, 'sugg_4d_ngan'):
                 self._update_rich_suggestion(self.sugg_4d_ngan, khan_4d_str)

        if hasattr(self, 'sugg_3d_cham') and cham_3d: self._update_rich_suggestion(self.sugg_3d_cham, cham_3d)
        if hasattr(self, 'sugg_3d_tong') and tong_3d: self._update_rich_suggestion(self.sugg_3d_tong, tong_3d)
        if hasattr(self, 'sugg_4d_cham') and cham_4d: self._update_rich_suggestion(self.sugg_4d_cham, cham_4d)
        if hasattr(self, 'sugg_4d_tong') and tong_4d: self._update_rich_suggestion(self.sugg_4d_tong, tong_4d)
        # 2D Suggestions
        if hasattr(self, 'sugg_2d_cham'): self._update_rich_suggestion(self.sugg_2d_cham, cham_2d)
        if hasattr(self, 'sugg_2d_tong'): self._update_rich_suggestion(self.sugg_2d_tong, tong_2d)
        if hasattr(self, 'sugg_2d_hieu'): self._update_rich_suggestion(self.sugg_2d_hieu, hieu_2d)
        
        # Scan Multi-way Suggestions
        if hasattr(self, 'sugg_multi_cham'): 
            self._update_rich_suggestion(self.sugg_multi_cham, multi_cham, show_hits=True)
        
        if hasattr(self, 'sugg_multi_tong'): 
            self._update_rich_suggestion(self.sugg_multi_tong, multi_tong, show_hits=True)

    def _update_rich_suggestion(self, text_widget, data, show_hits=False):
        """Update a tk.Text suggestion widget with 10 digits, colored and sorted."""
        if data is None: return
        
        text_widget.config(state='normal')
        text_widget.delete('1.0', tk.END)
        text_widget.insert(tk.END, "(Dự: ")
        
        try:
            if isinstance(data, tuple) and len(data) >= 2:
                gaps_dict = data[0]
                exceeding_list = data[1]
                hits_today = data[2] if len(data) > 2 else []
                
                # Sort all 10 digits by gap descending - Ensure keys are integers
                sorted_digits = sorted(range(10), key=lambda x: gaps_dict.get(int(x), 0), reverse=True)
                top_3 = sorted_digits[:3]
                
                exc_list = list(exceeding_list) if exceeding_list else []
                hit_today_list = list(hits_today) if hits_today else []
                
                for idx, d in enumerate(sorted_digits):
                    count = gaps_dict.get(int(d), 0)
                    is_today = int(d) in hit_today_list
                    
                    if int(d) in top_3:
                        tag = 'top_3_dự' if (int(d) in exc_list or is_today) else 'top_3'
                    elif count > 0 and not is_today:
                        tag = 'hit' # Not today, but appeared recently (Yellow)
                    else:
                        # Appeared today OR freq=0
                        tag = 'dự' if (int(d) in exc_list and not is_today) else 'normal'
                    
                    text_widget.insert(tk.END, str(d), tag)
                    if idx < 9: text_widget.insert(tk.END, ",", 'normal')
            elif isinstance(data, (str, list)):
                # Fallback for simple strings or lists
                text_widget.insert(tk.END, str(data), 'dự')
        except Exception as e:
            # Emergency fallback: just show the number 0-9 to avoid empty brackets
            text_widget.insert(tk.END, "0,1,2,3,4,5,6,7,8,9", 'normal')
            
        text_widget.insert(tk.END, ")")
        
        # Display list of hits (already appeared digits) - ONLY for scan module
        try:
            if show_hits and isinstance(data, tuple) and len(data) > 2:
                hits_today = data[2]
                if hits_today:
                    text_widget.insert(tk.END, " Ra: ", 'normal')
                    # Ensure they are unique and sorted
                    h_list = sorted(list(set(str(x) for x in hits_today)))
                    for i, h_str in enumerate(h_list):
                        text_widget.insert(tk.END, h_str, 'hit')
                        if i < len(h_list) - 1:
                            text_widget.insert(tk.END, ",", 'normal')
        except:
            pass

        text_widget.config(state='disabled')

    def apply_hot_to_selector(self, type_pick):
        """Transfer Top 3 HOT digits from scan tab to Selector checkboxes."""
        if not hasattr(self, 'last_ana_results') or not self.last_ana_results:
            messagebox.showwarning("Cảnh báo", "Hãy bấm 'Phân tích' ở tab Đa Chiều trước!")
            return
        
        ana = self.last_ana_results
        mode = self.scan_mode_var.get()
        n_digits = int(mode[0]) if mode[0].isdigit() else 2
        
        target_vars = []
        source_data = []
        
        if type_pick == 'cham':
            source_data = ana['cham'][1][:3] # Top 3 digits
            target_vars = self.selector_vars_multi_cham # Target the NEW Scan selector
        else: # tong
            source_data = ana['tong'][1][:3] 
            target_vars = self.selector_vars_multi_tong
            
        if target_vars and source_data:
            # Clear previous and set new
            for v in target_vars: v.set(False)
            for d in source_data:
                if 0 <= d < 10:
                    target_vars[d].set(True)
            self.update_selector_results()
            messagebox.showinfo("Thành công", f"Đã áp dụng Top 3 {type_pick.upper()} vào Bộ Chọn 2-3-4D Scan!")

    def clear_selector_checkboxes(self):
        all_vars = self.selector_vars_3d_cham + self.selector_vars_3d_tong + self.selector_vars_3d_tram + \
                   self.selector_vars_4d_cham + self.selector_vars_4d_tong + \
                   self.selector_vars_4d_ngan + \
                   self.selector_vars_2d_cham + self.selector_vars_2d_tong + self.selector_vars_2d_hieu + \
                   self.selector_vars_2d_bo + self.selector_vars_2d_kep + self.selector_vars_2d_zodiac + \
                   self.selector_vars_multi_cham + self.selector_vars_multi_tong + self.selector_vars_3d_kep
        for var in all_vars:
            var.set(False)
        self.selector_3d_text.delete('1.0', tk.END)
        self.selector_4d_text.delete('1.0', tk.END)
        self.selector_2d_text.delete('1.0', tk.END)

    def _calculate_3d_scores(self):
        """Tính toán điểm overlap cho 3D dựa trên Chạm, Tổng và Trạm."""
        sel_c = [i for i, v in enumerate(self.selector_vars_3d_cham) if v.get()]
        sel_t = [i for i, v in enumerate(self.selector_vars_3d_tong) if v.get()]
        sel_tr = [i for i, v in enumerate(self.selector_vars_3d_tram) if v.get()]
        
        scores = [0] * 1000
        for c in sel_c:
            for n in CHAM_3CANG[c]: scores[int(n)] += 1
        for t in sel_t:
            for n in TONG_3CANG[t]: scores[int(n)] += 1
        for tr in sel_tr:
            for tail in range(100): scores[tr * 100 + tail] += 1
        return scores

    def _calculate_4d_scores(self):
        """Tính toán điểm overlap cho 4D dựa trên Chạm, Tổng và Ngàn."""
        sel_c = [i for i, v in enumerate(self.selector_vars_4d_cham) if v.get()]
        sel_t = [i for i, v in enumerate(self.selector_vars_4d_tong) if v.get()]
        sel_n = [i for i, v in enumerate(self.selector_vars_4d_ngan) if v.get()]
        
        scores = [0] * 10000
        for c in sel_c:
            for n in CHAM_4CANG[c]: scores[int(n)] += 1
        for t in sel_t:
            for n in TONG_4CANG[t]: scores[int(n)] += 1
        for n in sel_n:
            for tail in range(1000): scores[n * 1000 + tail] += 1
        return scores

    def update_selector_results(self):
        """Tính toán kết quả theo Mức (Accumulation) cho 2D/3D/4D và Chế độ Lai."""
        region = self.region_var.get()
        prize = self.prize_var.get()

        # Constraints
        is_3d_blocked = (region == "Miền Bắc" and prize == "Giải 7") or (region != "Miền Bắc" and prize == "Giải 8")
        is_4d_blocked = (region == "Miền Bắc" and prize in ["Giải 6", "Giải 7"]) or (region != "Miền Bắc" and prize in ["Giải 7", "Giải 8"])

        # 0. Get Checkbox Values
        sel_2d_c = [i for i, v in enumerate(self.selector_vars_2d_cham) if v.get()]
        sel_2d_t = [i for i, v in enumerate(self.selector_vars_2d_tong) if v.get()]
        sel_2d_h = [i for i, v in enumerate(self.selector_vars_2d_hieu) if v.get()]
        
        sel_multi_c = [i for i, v in enumerate(self.selector_vars_multi_cham) if v.get()]
        sel_multi_t = [i for i, v in enumerate(self.selector_vars_multi_tong) if v.get()]
        sel_multi_k = [i for i, v in enumerate(self.selector_vars_3d_kep) if v.get()]
        
        sel_3d_c = [i for i, v in enumerate(self.selector_vars_3d_cham) if v.get()] if not is_3d_blocked else []
        sel_3d_t = [i for i, v in enumerate(self.selector_vars_3d_tong) if v.get()] if not is_3d_blocked else []
        sel_3d_tr = [i for i, v in enumerate(self.selector_vars_3d_tram) if v.get()] if not is_3d_blocked else []
        
        sel_4d_c = [i for i, v in enumerate(self.selector_vars_4d_cham) if v.get()] if not is_4d_blocked else []
        sel_4d_t = [i for i, v in enumerate(self.selector_vars_4d_tong) if v.get()] if not is_4d_blocked else []
        sel_4d_n = [i for i, v in enumerate(self.selector_vars_4d_ngan) if v.get()] if not is_4d_blocked else []

        # 2D Group Selectors (Indices map to names from BO_DICT, KEP_DICT, ZODIAC_DICT)
        bo_keys = list(BO_DICT.keys())
        kep_keys = ["K.AM", "K.BANG", "K.LECH", "S.KEP", "K.KHONG"]
        zodiac_keys = list(ZODIAC_DICT.keys())
        
        sel_2d_bo = [bo_keys[i] for i, v in enumerate(self.selector_vars_2d_bo) if v.get()]
        sel_2d_kep = [kep_keys[i] for i, v in enumerate(self.selector_vars_2d_kep) if v.get()]
        sel_2d_zodiac = [zodiac_keys[i] for i, v in enumerate(self.selector_vars_2d_zodiac) if v.get()]

        # Clear All
        self.selector_2d_text.delete('1.0', tk.END)
        self.selector_3d_text.delete('1.0', tk.END)
        self.selector_4d_text.delete('1.0', tk.END)

        if is_3d_blocked: self.selector_3d_text.insert(tk.END, f"⚠️ KHÔNG HỖ TRỢ 3D CHO {prize.upper()}\n\n", 'header')
        if is_4d_blocked: self.selector_4d_text.insert(tk.END, f"⚠️ KHÔNG HỖ TRỢ 4D CHO {prize.upper()}\n\n", 'header')

        # Current level filter
        level_filter = self.level_2d_filter.get() if hasattr(self, 'level_2d_filter') else "Tất cả"
        min_level = 0
        if "≥ Mức" in level_filter: min_level = int(level_filter.split("Mức ")[1])

        # 1. --- MODULE: BỘ CHỌN 2D (Accumulation + Linked) ---
        hits_2d = Counter()
        # Chạm/Tổng/Hiệu 2D
        if sel_2d_c or sel_2d_t or sel_2d_h:
            for c in sel_2d_c:
                for s in CHAM_2CANG[c]: hits_2d[s] += 1
            for t in sel_2d_t:
                for s in TONG_2CANG[t]: hits_2d[s] += 1
            for h in sel_2d_h:
                for s in HIEU_2CANG[h]: hits_2d[s] += 1
        
        # Add 2D Groups (Always check even if no Chạm/Tổng is selected)
        for b_key in sel_2d_bo:
            for s in BO_DICT.get(b_key, []): hits_2d[s] += 1
        for k_key in sel_2d_kep:
            for s in KEP_DICT.get(k_key, []): hits_2d[s] += 1
        for z_key in sel_2d_zodiac:
            for s in ZODIAC_DICT.get(z_key, []): hits_2d[s] += 1

        # 2. --- MODULE: BỘ CHỌN SCAN (Full Generation) ---
        # Check current scan mode to determine where to display results
        # Update from dropdown if available
        if hasattr(self, 'scan_mode_var'):
            mode = self.scan_mode_var.get()
            n_digits = int(mode[0]) if mode and mode[0].isdigit() else 2
            self.current_scan_mode = n_digits
        
        current_mode = getattr(self, 'current_scan_mode', 2)  # Default to 2D
        
        scan_hits_2d = Counter()
        scan_hits_3d = Counter()
        scan_hits_4d = Counter()
        
        if sel_multi_c or sel_multi_t:
            # Generate based on current scan mode
            if current_mode == 2:
                # Generate 2D
                for c in sel_multi_c:
                    for s in CHAM_2CANG[c]: scan_hits_2d[s] += 1
                for t in sel_multi_t:
                    for s in TONG_2CANG[t]: scan_hits_2d[s] += 1
            elif current_mode == 3:
                # Generate 3D
                for c in sel_multi_c:
                    for num in CHAM_3CANG[c]: scan_hits_3d[num] += 1
                for t in sel_multi_t:
                    for num in TONG_3CANG[t]: scan_hits_3d[num] += 1
            elif current_mode == 4:
                # Generate 4D
                for c in sel_multi_c:
                    for num in CHAM_4CANG[c]: scan_hits_4d[num] += 1
                for t in sel_multi_t:
                    for num in TONG_4CANG[t]: scan_hits_4d[num] += 1
        
        # COMBINE 2D results from both modules (similar to 3D logic)
        combined_2d = Counter(hits_2d)
        if scan_hits_2d:
            for num, score in scan_hits_2d.items():
                combined_2d[num] += score
        
        # Display combined 2D results
        if combined_2d:
            # Basic header
            import datetime
            header_info = f"📍 {region} - {datetime.datetime.now().strftime('%d/%m/%Y')}"
            self.selector_2d_text.insert(tk.END, f"{header_info}\n\n", 'level')
            self._render_selector_hits(combined_2d, self.selector_2d_text, show_detail=True)
            
            # Linked 3D/4D logic
            linked_3d = []
            if sel_3d_tr:
                scores_3d = self._calculate_3d_scores()
                filtered_2d = [num for num, score in combined_2d.items() if score >= max(1, min_level)] if min_level > 0 else list(combined_2d.keys())
                for tr in sel_3d_tr:
                    for tail in filtered_2d:
                        num3d = f"{tr}{tail}"
                        if scores_3d[int(num3d)] >= min_level: linked_3d.append(num3d)
            
            if linked_3d:
                self.selector_2d_text.insert(tk.END, f"\n--- 🔗 GHÉP VỚI TRĂM ({len(linked_3d)} số) ---\n", 'header')
                self.selector_2d_text.insert(tk.END, ", ".join(sorted(linked_3d)) + "\n", 'numbers')
                # Display linked 3D results in 3D box only if 3D selectors are active
                if sel_3d_c or sel_3d_t or sel_3d_tr:
                    self.selector_3d_text.insert(tk.END, f"--- 🔗 GHÉP VỚI 2D ({len(linked_3d)} số) ---\n", 'header')
                    self.selector_3d_text.insert(tk.END, ", ".join(sorted(linked_3d)) + "\n\n", 'numbers')

            linked_4d = []
            if sel_4d_n and linked_3d:
                scores_4d = self._calculate_4d_scores()
                for n in sel_4d_n:
                    for tail3d in linked_3d:
                        num4d = f"{n}{tail3d}"
                        if scores_4d[int(num4d)] >= min_level: linked_4d.append(num4d)
            
            if linked_4d:
                self.selector_2d_text.insert(tk.END, f"\n--- 🔗 GHÉP VỚI NGHÌN ({len(linked_4d)} số) ---\n", 'header')
                self.selector_2d_text.insert(tk.END, ", ".join(sorted(linked_4d)) + "\n", 'numbers')
                # Display linked 4D results in 4D box only if 4D selectors are active
                if sel_4d_c or sel_4d_t or sel_4d_n:
                    self.selector_4d_text.insert(tk.END, f"--- 🔗 GHÉP VỚI 3D ({len(linked_4d)} số) ---\n", 'header')
                    self.selector_4d_text.insert(tk.END, ", ".join(sorted(linked_4d)) + "\n\n", 'numbers')

        # 2. --- MODULE: BỘ CHỌN SCAN (Full Generation) ---
        # scan_hits_3d and scan_hits_4d are already populated above (lines 5086-5110)
        
        if sel_multi_k:
            # 3D Kép Scan (independent of current_scan_mode)
            kep_keys = ["AAB", "ABA", "BAA", "KEP_TH", "L_BANG", "L_LECH", "L_AM", "L_TH", "CON_LAI"]
            for idx in sel_multi_k:
                k_key = kep_keys[idx]
                for num in KEP_PATTERNS_3D[k_key]:
                    scan_hits_3d[num] += 1
                    # Also add to 4D (each 3D number becomes 10 4D numbers)
                    for head in range(10):
                        scan_hits_4d[f"{head}{num}"] += 1
            
            # Scan results are now combined with BỘ CHỌN results above
            # No separate display needed here

        # 3. --- MODULE: 3D, 4D (Cumulative) ---
        hits_3d_base = Counter()
        if not is_3d_blocked:
            for c in sel_3d_c:
                for num in CHAM_3CANG[c]: hits_3d_base[num] += 1
            for t in sel_3d_t:
                for num in TONG_3CANG[t]: hits_3d_base[num] += 1
            for tr in sel_3d_tr:
                for tail in range(100): hits_3d_base[f"{tr}{tail:02d}"] += 1
        
        hits_4d_base = Counter()
        if not is_4d_blocked:
            for c in sel_4d_c:
                for num in CHAM_4CANG[c]: hits_4d_base[num] += 1
            for t in sel_4d_t:
                for num in TONG_4CANG[t]: hits_4d_base[num] += 1
            for n in sel_4d_n:
                for tail in range(1000): hits_4d_base[f"{n}{tail:03d}"] += 1

        # Propagation logic
        # 1. Start with base hits and scan hits
        combined_3d = Counter(hits_3d_base)
        if scan_hits_3d:
            for num, score in scan_hits_3d.items():
                combined_3d[num] += score
        
        combined_4d = Counter(hits_4d_base)
        if scan_hits_4d:
            for num, score in scan_hits_4d.items():
                combined_4d[num] += score

        # 2. Propagate 2D scores to 3D and 4D
        for tail2d, score in combined_2d.items():
            # To 3D
            for head in range(10):
                combined_3d[f"{head}{tail2d}"] += score
            # To 4D
            for head in range(100):
                combined_4d[f"{head:02d}{tail2d}"] += score
                
        # 3. Propagate 3D base scores to 4D
        # We propagate 3D base (selections + scan) to avoid double counting 2D
        combined_3d_base = Counter(hits_3d_base)
        if scan_hits_3d:
            for num, score in scan_hits_3d.items():
                combined_3d_base[num] += score
                
        for tail3d, score in combined_3d_base.items():
            for head in range(10):
                combined_4d[f"{head}{tail3d}"] += score

        self.last_3d_hits = combined_3d
        self.last_4d_hits = combined_4d

        # INDEPENDENT DISPLAY (All 3 Dimensions now always include cumulative results)
        if combined_3d:
            self.selector_3d_text.insert(tk.END, "📋 KẾT QUẢ 3D (Cộng dồn 2D) ---\n", 'header')
            self._render_selector_hits(combined_3d, self.selector_3d_text)
        if combined_4d:
            self.selector_4d_text.insert(tk.END, "📋 KẾT QUẢ 4D (Cộng dồn 3D & 2D) ---\n", 'header')
            self._render_selector_hits(combined_4d, self.selector_4d_text, is_4d=True)

        self.update_level_filter_options()
        self.update_3d_kep_stats()

    def _render_selector_hits(self, counter, text_widget, is_4d=False, show_detail=True):
        """Hiển thị các số theo Mức, kèm dàn 2D mức cao lên đầu (Giống Backup)."""
        levels = defaultdict(list)
        for num, score in counter.items():
            levels[score].append(num)
            
        sorted_lv = sorted(levels.keys(), reverse=True)
        if not sorted_lv: return

        # 1. Trích xuất dàn 2D từ mức được chọn trong dropdown
        level_filter = self.level_2d_filter.get() if hasattr(self, 'level_2d_filter') else "Tất cả"
        min_level = 1
        if "≥ Mức" in level_filter: min_level = int(level_filter.split("Mức ")[1])
        elif level_filter == "Tất cả": min_level = 0
        
        dan_2d = set()
        for lv in sorted_lv:
            if lv >= min_level:
                for num in levels[lv]:
                    if len(num) >= 2: dan_2d.add(num[-2:])
        
        if dan_2d and min_level > 0:
            sorted_2d = sorted(list(dan_2d))
            title = f"⭐ DÀN 2D TỪ MỨC {min_level} TRỞ LÊN"
            text_widget.insert(tk.END, f"{title} ({len(sorted_2d)} số):\n", 'level')
            text_widget.insert(tk.END, ", ".join(sorted_2d) + "\n\n", 'numbers')
            if show_detail: text_widget.insert(tk.END, "--- 📋 CHI TIẾT THEO MỨC ---\n", 'header')
        elif show_detail:
            text_widget.insert(tk.END, "--- 📋 CHI TIẾT THEO MỨC ---\n", 'header')

        # 2. Hiển thị chi tiết theo mức (Nếu show_detail=True)
        if show_detail:
            for lv in sorted_lv:
                nums = sorted(levels[lv])
                text_widget.insert(tk.END, f"Mức {lv}: ({len(nums)} số)\n", 'level')
                text_widget.insert(tk.END, ", ".join(nums) + "\n", 'numbers')


    def render_tong_cham_4d(self, days_data=None):
        """Render Tổng và Chạm 4 Càng tracking tables"""
        region = self.region_var.get()
        prize = self.prize_var.get()
        
        if region == "Miền Bắc":
            data_source = self.master_data
            station_name = f"XSMB - {prize}"
        else:
            data_source = self.station_data
            s_name = self.station_var.get()
            if s_name == "Tất cả":
                day = self.day_var.get()
                station_name = f"{day} - TẤT CẢ {region} - {prize}"
            else:
                station_name = f"{s_name} - {prize}"
        
        if not data_source: return
        
        # BLOCK 4D for small prizes
        is_blocked = False
        if region == "Miền Bắc" and (prize == "Giải 6" or prize == "Giải 7"):
            is_blocked = True
        elif region != "Miền Bắc" and (prize == "Giải 7" or prize == "Giải 8"):
            is_blocked = True
            
        if is_blocked:
            self.tong_cham_4d_info_label.config(text=f"⚠️ KHÔNG HỖ TRỢ CHẾ ĐỘ 4D CHO {prize.upper()} {region.upper()}", foreground="red")
            self.cham_4d_canvas.delete("all")
            self.tong_4d_canvas.delete("all")
            self.cham_4d_pred_text.delete('1.0', tk.END)
            self.tong_4d_pred_text.delete('1.0', tk.END)
            self.rare_thousands_text.delete('1.0', tk.END)
            return

        # Update info
        self.tong_cham_4d_info_label.config(text=f"📊 THEO DÕI TỔNG & CHẠM 4 CÀNG - {station_name} - {len(data_source)} ngày")

        # Use provided days_data if available (for backtesting), otherwise process from data_source
        if days_data is None:
            processed_days_data = []
            for row in data_source:
                db_4so_list = []
                if 'items' in row:
                    for item in row['items']:
                        db_4so_list.extend(self.get_prize_numbers(item, width=4))
                else:
                    db_4so_list = self.get_prize_numbers(row, width=4)
                
                if not db_4so_list: continue
                
                # Normalize and validate 4 digits
                valid_4so = [v[-4:] for v in db_4so_list if len(v) >= 4] # Strictly 4-digit tails
                if not valid_4so: 
                    # Fallback for small prizes if width=4 requested
                    valid_4so = [v.zfill(4) for v in db_4so_list if len(v) >= 2]
                
                if not valid_4so: continue

                tongs = [sum(int(d) for d in num) % 10 for num in valid_4so]
                chams = [set(int(d) for d in num) for num in valid_4so]
                
                processed_days_data.append({
                    'date': row['date'],
                    'db_4so': valid_4so,
                    'tong': tongs,
                    'cham': chams
                })
            days_data = processed_days_data # Assign to days_data for further processing
    
        # ========== BACKTEST LOGIC ==========
        actual_res = None
        try:
            backtest_days = self.backtest_var.get()
        except:
            backtest_days = 0
            
        check_msg = ""
        if backtest_days > 0 and days_data:
            if backtest_days < len(days_data):
                # Get the result of the "Next Day" relative to the backtest date
                actual_res = days_data[backtest_days - 1]
                
                # Slice the data to simulate being in the past
                days_data = days_data[backtest_days:]
                
                # Formulate the Result Check message for 4D
                res_val = ", ".join(actual_res['db_4so'])
                check_msg = f" | 🔙 BACKTEST (Đến {days_data[0]['date']}) -> KQ THỰC TẾ {actual_res['date']}: {res_val}"
                
                # Highlight this message
                self.tong_cham_4d_info_label.config(text=f"📊 THEO DÕI TỔNG & CHẠM 4 CÀNG {check_msg}", foreground="#ff4b4b")
            else:
                self.tong_cham_4d_info_label.config(text=f"📊 BACKTEST: Không đủ dữ liệu để lùi {backtest_days} ngày", foreground="red")
                return
        else:
            # Restore normal label
            self.tong_cham_4d_info_label.config(text=f"📊 THEO DÕI TỔNG & CHẠM 4 CÀNG - {station_name} - {len(data_source)} ngày", foreground="white")

        if not days_data: return

        # Render - Professional Dark Theme
        header_bg = "#333333"
        hit_bg = "#27ae60"
        miss_bg = "#c0392b"
        normal_bg = "#1e1e1e"
        highlight_bg = "#2d2d2d"

        self._render_cham_table_4d(days_data, header_bg, hit_bg, miss_bg, normal_bg, highlight_bg)
        self._render_tong_table_4d(days_data, header_bg, hit_bg, miss_bg, normal_bg, highlight_bg)
        self._render_rare_thousands(days_data)

        # Calculate 2D predictions for suggestion updates
        self._calculate_2d_predictions(days_data)

        # Update selector tab suggestions for 4D
        cham_data = (self.cham_gaps_4d, self.exceeding_cham_4d) if hasattr(self, 'cham_gaps_4d') else None
        tong_data = (self.tong_gaps_4d, self.exceeding_tong_4d) if hasattr(self, 'tong_gaps_4d') else None
        
        # Calculate Top Khan for Hàng Nghìn (thousands digit)
        ngan_gaps = {i: 0 for i in range(10)}
        for day in reversed(days_data):
            # Increment gap for all digits first
            for digit in range(10):
                ngan_gaps[digit] += 1
            # Reset gap to 0 for digits that appeared
            for num in day['db_4so']:
                if len(num) >= 4:
                    thousands_digit = int(num[-4])
                    ngan_gaps[thousands_digit] = 0
        
        # Get top 5 khan (highest gaps)
        sorted_ngan = sorted(ngan_gaps.items(), key=lambda x: x[1], reverse=True)
        top_khan_ngan = tuple([d for d, g in sorted_ngan[:5]])
        
        # Prepare data with hits of today (gap 0) for 4D
        c4_hits = [i for i, g in self.cham_gaps_4d.items() if g == 0] if hasattr(self, 'cham_gaps_4d') else []
        t4_hits = [i for i, g in self.tong_gaps_4d.items() if g == 0] if hasattr(self, 'tong_gaps_4d') else []
        ng_hits = [i for i, g in ngan_gaps.items() if g == 0]
        
        # Also need 3D and 2D hit contexts for full refresh
        c3_hits = [i for i, g in self.cham_gaps.items() if g == 0] if hasattr(self, 'cham_gaps') else []
        t3_hits = [i for i, g in self.tong_gaps.items() if g == 0] if hasattr(self, 'tong_gaps') else []
        c2_hits = [i for i, g in self.cham_gaps_2d.items() if g == 0] if hasattr(self, 'cham_gaps_2d') else []
        t2_hits = [i for i, g in self.tong_gaps_2d.items() if g == 0] if hasattr(self, 'tong_gaps_2d') else []
        h2_hits = [i for i, g in self.hieu_gaps_2d.items() if g == 0] if hasattr(self, 'hieu_gaps_2d') else []

        self.update_rare_info_on_selector(
            khan_4d_str=(ngan_gaps, top_khan_ngan, ng_hits),
            cham_4d=(self.cham_gaps_4d, self.exceeding_cham_4d, c4_hits) if hasattr(self, 'cham_gaps_4d') else None,
            tong_4d=(self.tong_gaps_4d, self.exceeding_tong_4d, t4_hits) if hasattr(self, 'tong_gaps_4d') else None,
            cham_3d=(self.cham_gaps, self.exceeding_cham, c3_hits) if hasattr(self, 'cham_gaps') else None,
            tong_3d=(self.tong_gaps, self.exceeding_tong, t3_hits) if hasattr(self, 'tong_gaps') else None,
            cham_2d=(self.cham_gaps_2d, self.exceeding_cham_2d, c2_hits) if hasattr(self, 'cham_gaps_2d') else None,
            tong_2d=(self.tong_gaps_2d, self.exceeding_tong_2d, t2_hits) if hasattr(self, 'tong_gaps_2d') else None,
            hieu_2d=(self.hieu_gaps_2d, self.exceeding_hieu_2d, h2_hits) if hasattr(self, 'hieu_gaps_2d') else None
        )

    def _render_cham_table_4d(self, days_data, header_bg, hit_bg, miss_bg, normal_bg, highlight_bg):
        # Implementation similar to _render_cham_table but targeting cham_4d_canvas
        canvas = self.cham_4d_canvas
        canvas.delete("all")
        
        CELL_W = 24
        CELL_H = 25
        DATE_W = 60
        COL_DATE = 0
        COL_START_NUM = DATE_W
        COL_MAX = DATE_W + 10 * CELL_W
        COL_DANG = COL_MAX + 35
        COL_DB = COL_DANG + 35
        TOTAL_W = COL_DB + 60
        
        FONT_HEADER = ('Segoe UI', 8, 'bold')
        FONT_NUM = ('Consolas', 9)
        FONT_NUM_BOLD = ('Consolas', 9, 'bold')
        
        def draw_cell(x, y, w, h, text, bg, fg, font=FONT_NUM):
            canvas.create_rectangle(x, y, x + w, y + h, fill=bg, outline='#dcdcdc')
            canvas.create_text(x + w/2, y + h/2, text=text, fill=fg, font=font)

        days_data_ordered = list(reversed(days_data))
        cham_gan = {i: 0 for i in range(10)}
        cham_max_gan = {i: 0 for i in range(10)}
        row_results = []
        
        for day in days_data_ordered:
            combined_cham_hits = set()
            for s in day['cham']: combined_cham_hits.update(s)
            
            db_display = "\n".join(day['db_4so'])
            row_data = {'date': day['date'], 'db_4so': db_display, 'cells': {}, 'max_val': 0}
            
            for col in range(10):
                is_hit = col in combined_cham_hits
                if is_hit:
                    cell_text = str(cham_gan[col]) if cham_gan[col] > 0 else "0"
                    cell_bg = hit_bg
                    cham_gan[col] = 0
                else:
                    cham_gan[col] += 1
                    cell_bg = miss_bg if cham_gan[col] >= self.GAN_HIGH_THRESHOLD else normal_bg
                    cell_text = str(cham_gan[col])
                
                if cham_gan[col] > cham_max_gan[col]: cham_max_gan[col] = cham_gan[col]
                fg_color = "white" if is_hit or cham_gan[col] >= self.GAN_HIGH_THRESHOLD else "#bdc3c7"
                row_data['cells'][col] = {'text': cell_text, 'bg': cell_bg, 'fg': fg_color}
            
            row_data['max_val'] = max(cham_gan.values())
            row_results.append(row_data)
            
        avg_gan = {}
        exceeding_avg = []
        for col in range(10):
            avg_gan[col] = cham_max_gan[col] / 2 if cham_max_gan[col] > 0 else 5
            if cham_gan[col] >= avg_gan[col]: exceeding_avg.append(col)
        # Sort by gan descending (highest gan first = most khan)
        self.cham_gaps_4d = cham_gan
        self.exceeding_cham_4d = sorted(exceeding_avg, key=lambda x: cham_gan[x], reverse=True)

        current_y = 0
        # Header
        draw_cell(COL_DATE, current_y, DATE_W, CELL_H, "Chạm 4D", header_bg, "white", FONT_HEADER)
        for i in range(10): draw_cell(COL_START_NUM + i*CELL_W, current_y, CELL_W, CELL_H, str(i), header_bg, "white", FONT_NUM_BOLD)
        draw_cell(COL_MAX, current_y, 35, CELL_H, "Max", "#8e44ad", "white", ('Segoe UI', 7, 'bold'))
        draw_cell(COL_DANG, current_y, 35, CELL_H, "Đang", "#e67e22", "white", ('Segoe UI', 7, 'bold'))
        draw_cell(COL_DB, current_y, 60, CELL_H, "ĐB", "#2980b9", "white", ('Segoe UI', 7, 'bold'))
        current_y += CELL_H
        
        # Max Gan
        draw_cell(COL_DATE, current_y, DATE_W, CELL_H, "Max Gan", "#8e44ad", "white", ('Segoe UI', 7, 'bold'))
        for col in range(10):
            max_g = cham_max_gan[col]
            bg_col = "#e74c3c" if max_g >= self.GAN_VERY_HIGH_THRESHOLD else "#f39c12" if max_g >= self.GAN_HIGH_THRESHOLD else "#27ae60"
            draw_cell(COL_START_NUM + col*CELL_W, current_y, CELL_W, CELL_H, str(max_g), bg_col, "white", FONT_NUM_BOLD)
        draw_cell(COL_MAX, current_y, 35, CELL_H, "", "#34495e", "white")
        draw_cell(COL_DANG, current_y, 35, CELL_H, "", "#2d2d2d", "white")
        draw_cell(COL_DB, current_y, 60, CELL_H, "", "#2d2d2d", "white")
        current_y += CELL_H
        
        # Đang Gan
        draw_cell(COL_DATE, current_y, DATE_W, CELL_H, "Đang Gan", "#16a085", "white", ('Segoe UI', 7, 'bold'))
        for col in range(10):
            bg_col = "#e74c3c" if col in exceeding_avg else "#1e1e1e"
            fg_col = "white" if col in exceeding_avg else "#bdc3c7"
            draw_cell(COL_START_NUM + col*CELL_W, current_y, CELL_W, CELL_H, str(cham_gan[col]), bg_col, fg_col, FONT_NUM_BOLD)
        pred_text = ",".join(map(str, exceeding_avg)) if exceeding_avg else "-"
        draw_cell(COL_MAX, current_y, 35, CELL_H, "Dự:", "#16a085", "white", ('Segoe UI', 7))
        draw_cell(COL_DANG, current_y, 35, CELL_H, pred_text, "#e74c3c", "white", ('Consolas', 8, 'bold'))
        draw_cell(COL_DB, current_y, 60, CELL_H, "", "#2d2d2d", "white")
        current_y += CELL_H
        
        # Data Rows
        for idx, row_data in enumerate(reversed(row_results)):
            row_bg = "#2d2d2d" if idx % 2 == 0 else "#1e1e1e"
            draw_cell(COL_DATE, current_y, DATE_W, CELL_H, row_data['date'][:5], row_bg, "#bdc3c7", ('Consolas', 7))
            for col in range(10):
                 c = row_data['cells'][col]
                 # If it's a miss (normal_bg), use row_bg for alternating effect
                 cell_bg = row_bg if c['bg'] == normal_bg else c['bg']
                 draw_cell(COL_START_NUM + col*CELL_W, current_y, CELL_W, CELL_H, c['text'], cell_bg, c['fg'], ('Consolas', 8))
            draw_cell(COL_MAX, current_y, 35, CELL_H, str(row_data['max_val']), "#34495e", "white", ('Consolas', 8, 'bold'))
            draw_cell(COL_DANG, current_y, 35, CELL_H, "", row_bg, "white")
            draw_cell(COL_DB, current_y, 60, CELL_H, row_data['db_4so'], row_bg, "#f39c12", ('Consolas', 9, 'bold'))
            current_y += CELL_H
            
        canvas.configure(scrollregion=(0, 0, TOTAL_W, current_y))
        
        # Stats
        current_gan = [f"{i}:{cham_gan[i]}" for i in range(10)]
        exceed_text = f" | Vượt TB: {','.join(map(str, exceeding_avg))}" if exceeding_avg else ""
        self.cham_4d_stats_label.config(text=f"Gan hiện tại: {', '.join(current_gan)}{exceed_text}")
        
        # Prediction Text
        number_freq = {}
        for day in days_data_ordered:
            for db in day['db_4so']: number_freq[db] = number_freq.get(db, 0) + 1
            
        self.cham_4d_pred_text.delete('1.0', tk.END)
        if exceeding_avg:
            sorted_digits = sorted(exceeding_avg, key=lambda x: cham_gan[x], reverse=True)
            lines = []
            for digit in sorted_digits:
                numbers = CHAM_4CANG[digit]
                sorted_nums = sorted(numbers, key=lambda x: number_freq.get(x, 0), reverse=True)
                # Show all numbers as requested
                num_str = ",".join(sorted_nums)
                lines.append(f"Chạm {digit} (gan:{cham_gan[digit]}): {num_str}")
            self.cham_4d_pred_text.insert(tk.END, "\n".join(lines))
        else:
            self.cham_4d_pred_text.insert(tk.END, "Không có số vượt trung bình")

    def _render_tong_table_4d(self, days_data, header_bg, hit_bg, miss_bg, normal_bg, highlight_bg):
        # Implementation similar to _render_tong_table but for 4D
        canvas = self.tong_4d_canvas
        canvas.delete("all")
        
        CELL_W = 24
        CELL_H = 25
        DATE_W = 60
        COL_DATE = 0
        COL_START_NUM = DATE_W
        COL_MAX = DATE_W + 10 * CELL_W
        COL_TONG = COL_MAX + 35
        COL_DB = COL_TONG + 35
        TOTAL_W = COL_DB + 60
        
        FONT_HEADER = ('Segoe UI', 8, 'bold')
        FONT_NUM = ('Consolas', 9)
        FONT_NUM_BOLD = ('Consolas', 9, 'bold')
        
        def draw_cell(x, y, w, h, text, bg, fg, font=FONT_NUM):
            canvas.create_rectangle(x, y, x + w, y + h, fill=bg, outline='#dcdcdc')
            canvas.create_text(x + w/2, y + h/2, text=text, fill=fg, font=font)

        days_data_ordered = list(reversed(days_data))
        tong_gan = {i: 0 for i in range(10)}
        tong_max_gan = {i: 0 for i in range(10)}
        row_results = []
        
        for day in days_data_ordered:
            current_tong_list = day['tong']
            db_display = "\n".join(day['db_4so'])
            row_data = {'date': day['date'], 'db_4so': db_display, 'tong_val': current_tong_list, 'cells': {}, 'max_val': 0}
            
            for col in range(10):
                is_hit = col in current_tong_list
                if is_hit:
                    cell_text = str(tong_gan[col]) if tong_gan[col] > 0 else "0"
                    cell_bg = hit_bg
                    tong_gan[col] = 0
                else:
                    tong_gan[col] += 1
                    cell_bg = miss_bg if tong_gan[col] >= self.GAN_HIGH_THRESHOLD else normal_bg
                    cell_text = str(tong_gan[col])
                
                if tong_gan[col] > tong_max_gan[col]: tong_max_gan[col] = tong_gan[col]
                fg_color = "white" if is_hit or tong_gan[col] >= self.GAN_HIGH_THRESHOLD else "#bdc3c7"
                row_data['cells'][col] = {'text': cell_text, 'bg': cell_bg, 'fg': fg_color}
            
            row_data['max_val'] = max(tong_gan.values())
            row_results.append(row_data)

        avg_gan = {}
        exceeding_avg = []
        for col in range(10):
            avg_gan[col] = tong_max_gan[col] / 2 if tong_max_gan[col] > 0 else 5
            if tong_gan[col] >= avg_gan[col]: exceeding_avg.append(col)
        # Sort by gan descending (highest gan first = most khan)
        self.tong_gaps_4d = tong_gan
        self.exceeding_tong_4d = sorted(exceeding_avg, key=lambda x: tong_gan[x], reverse=True)
        
        current_y = 0
        draw_cell(COL_DATE, current_y, DATE_W, CELL_H, "Tổng 4D", header_bg, "white", FONT_HEADER)
        for i in range(10): draw_cell(COL_START_NUM + i*CELL_W, current_y, CELL_W, CELL_H, str(i), header_bg, "white", FONT_NUM_BOLD)
        draw_cell(COL_MAX, current_y, 35, CELL_H, "Max", "#8e44ad", "white", ('Segoe UI', 7, 'bold'))
        draw_cell(COL_TONG, current_y, 35, CELL_H, "Tổng", "#e67e22", "white", ('Segoe UI', 7, 'bold'))
        draw_cell(COL_DB, current_y, 60, CELL_H, "ĐB", "#2980b9", "white", ('Segoe UI', 7, 'bold'))
        current_y += CELL_H
        
        draw_cell(COL_DATE, current_y, DATE_W, CELL_H, "Max Gan", "#8e44ad", "white", ('Segoe UI', 7, 'bold'))
        for col in range(10):
            max_g = tong_max_gan[col]
            bg_col = "#e74c3c" if max_g >= self.GAN_VERY_HIGH_THRESHOLD else "#f39c12" if max_g >= self.GAN_HIGH_THRESHOLD else "#27ae60"
            draw_cell(COL_START_NUM + col*CELL_W, current_y, CELL_W, CELL_H, str(max_g), bg_col, "white", FONT_NUM_BOLD)
        draw_cell(COL_MAX, current_y, 35, CELL_H, "", "#34495e", "white")
        draw_cell(COL_TONG, current_y, 35, CELL_H, "", "#2d2d2d", "white")
        draw_cell(COL_DB, current_y, 60, CELL_H, "", "#2d2d2d", "white")
        current_y += CELL_H
        
        draw_cell(COL_DATE, current_y, DATE_W, CELL_H, "Đang Gan", "#16a085", "white", ('Segoe UI', 7, 'bold'))
        for col in range(10):
            bg_col = "#e74c3c" if col in exceeding_avg else "#1e1e1e"
            fg_col = "white" if col in exceeding_avg else "#bdc3c7"
            draw_cell(COL_START_NUM + col*CELL_W, current_y, CELL_W, CELL_H, str(tong_gan[col]), bg_col, fg_col, FONT_NUM_BOLD)
        pred_text = ",".join(map(str, exceeding_avg)) if exceeding_avg else "-"
        draw_cell(COL_MAX, current_y, 35, CELL_H, "Dự:", "#16a085", "white", ('Segoe UI', 7))
        draw_cell(COL_TONG, current_y, 35, CELL_H, pred_text, "#e74c3c", "white", ('Consolas', 8, 'bold'))
        draw_cell(COL_DB, current_y, 60, CELL_H, "", "#2d2d2d", "white")
        current_y += CELL_H
        
        for idx, row_data in enumerate(reversed(row_results)):
            row_bg = "#2d2d2d" if idx % 2 == 0 else "#1e1e1e"
            draw_cell(COL_DATE, current_y, DATE_W, CELL_H, row_data['date'][:5], row_bg, "#bdc3c7", ('Consolas', 7))
            for col in range(10):
                 c = row_data['cells'][col]
                 cell_bg = row_bg if c['bg'] == normal_bg else c['bg']
                 draw_cell(COL_START_NUM + col*CELL_W, current_y, CELL_W, CELL_H, c['text'], cell_bg, c['fg'], ('Consolas', 8))
            draw_cell(COL_MAX, current_y, 35, CELL_H, str(row_data['max_val']), "#34495e", "white", ('Consolas', 8, 'bold'))
            draw_cell(COL_TONG, current_y, 35, CELL_H, str(row_data['tong_val']), row_bg, "#3498db", ('Consolas', 9, 'bold'))
            draw_cell(COL_DB, current_y, 60, CELL_H, row_data['db_4so'], row_bg, "#f39c12", ('Consolas', 9, 'bold'))
            current_y += CELL_H
            
        canvas.configure(scrollregion=(0, 0, TOTAL_W, current_y))
        
        current_gan = [f"{i}:{tong_gan[i]}" for i in range(10)]
        exceed_text = f" | Vượt TB: {','.join(map(str, exceeding_avg))}" if exceeding_avg else ""
        self.tong_4d_stats_label.config(text=f"Gan hiện tại: {', '.join(current_gan)}{exceed_text}")
        
        number_freq = {}
        for day in days_data_ordered:
            for db in day['db_4so']: number_freq[db] = number_freq.get(db, 0) + 1
            
        self.tong_4d_pred_text.delete('1.0', tk.END)
        if exceeding_avg:
            sorted_tongs = sorted(exceeding_avg, key=lambda x: tong_gan[x], reverse=True)
            lines = []
            for tong_val in sorted_tongs:
                numbers = TONG_4CANG[tong_val]
                sorted_nums = sorted(numbers, key=lambda x: number_freq.get(x, 0), reverse=True)
                num_str = ",".join(sorted_nums)
                lines.append(f"Tổng {tong_val} (gan:{tong_gan[tong_val]}): {num_str}")
            self.tong_4d_pred_text.insert(tk.END, "\n".join(lines))
        else:
            self.tong_4d_pred_text.insert(tk.END, "Không có số vượt trung bình")

    def _render_combined_prediction_4d(self, days_data, actual_res=None):
        exceeding_cham = getattr(self, 'exceeding_cham_4d', [])
        exceeding_tong = getattr(self, 'exceeding_tong_4d', [])
        
        # Calculate overlap scores using helper
        scores = self._calculate_4d_scores()
            
        # Group numbers by score (Level)
        freq_groups = defaultdict(list)
        for i in range(10000):
            score = scores[i]
            num_str = f"{i:04d}"
            freq_groups[score].append(num_str)
            
        combined_numbers = [f"{i:04d}" for i in range(10000)]
            
        self.tamhop_4d_freq_text.delete('1.0', tk.END)
        total_days = len(days_data)
        
        if not combined_numbers:
             self.tamhop_4d_freq_text.insert(tk.END, "Không có Dàn Gan vượt trung bình.\n", 'header')
             return

        # Check actual result level if provided (Backtest)
        if actual_res:
            res_list = actual_res.get('db_4so', [])
            if res_list:
                # Check ALL results for that day (Multi-station)
                hits = []
                misses = []
                
                for val_full in res_list:
                    val = val_full[-4:] # Ensure 4 digits
                    found_level = -1
                    for lvl, numbers in freq_groups.items():
                        if val in numbers:
                            found_level = lvl
                            break
                    
                    if found_level != -1:
                        hits.append(f"{val}(M{found_level})")
                    else:
                        misses.append(val)
                
                if hits:
                    self.tamhop_4d_freq_text.insert(tk.END, f"🎯 KẾT QUẢ 4D: {', '.join(hits)}\n", 'high')
                if misses:
                    self.tamhop_4d_freq_text.insert(tk.END, f"⚠️ TRƯỢT: {', '.join(misses)}\n", 'header')
                self.tamhop_4d_freq_text.insert(tk.END, "\n")

        self.tamhop_4d_freq_text.insert(tk.END, f"📊 TỔNG HỢP {len(combined_numbers)} SỐ 4D\n", 'header')
        self.tamhop_4d_freq_text.insert(tk.END, f"  (Chạm: {exceeding_cham} | Tổng: {exceeding_tong})\n\n", 'header')
        
        # Extract 2D tails from the HIGHEST Level only
        tail_2d_set = set()
        max_score = 0
        if freq_groups:
            max_score = max(freq_groups.keys())
            
        if max_score > 0:
            for num in freq_groups[max_score]:
                if len(num) >= 2:
                    tail_2d_set.add(num[-2:])
        
        # Display 2D tails if any
        if tail_2d_set:
            sorted_tails = sorted(list(tail_2d_set))
            self.tamhop_4d_freq_text.insert(tk.END, f"🔥 DÀN ĐỀ 4D (TỪ MỨC {max_score}) [{len(sorted_tails)} số]:\n", 'high')
            self.tamhop_4d_freq_text.insert(tk.END, f"{','.join(sorted_tails)}\n\n", 'header')

        # Count only filtered numbers (Mức >= 1)
        filtered_count = sum(len(nums) for level, nums in freq_groups.items() if level > 0)
        if filtered_count > 0:
            self.tamhop_4d_freq_text.insert(tk.END, f"📊 Tổng: {filtered_count} số (chỉ hiển thị Mức ≥ 1)\n\n", 'med')
        
        for freq in sorted(freq_groups.keys(), reverse=True):
            # Skip Mức 0 to show only meaningful results
            if freq == 0:
                continue
                
            nums = sorted(freq_groups[freq])
            # Show all numbers as requested
            count = len(nums)
            display_nums = nums
            more_indicator = ""
            
            if freq >= (total_days * 0.1): tag = 'high'
            else: tag = 'med'
            
            self.tamhop_4d_freq_text.insert(tk.END, f"Mức {freq}: ({count} số)\n", tag)
            self.tamhop_4d_freq_text.insert(tk.END, f"{','.join(display_nums)}{more_indicator}\n", tag)


    def _render_rare_thousands(self, days_data):
        """Analyze and display top 5 rare Thousands digits (Hàng Nghìn)"""
        self.rare_thousands_text.delete('1.0', tk.END)
        if not days_data: return
            
        exceeding_cham = getattr(self, 'exceeding_cham_4d', [])
        exceeding_tong = getattr(self, 'exceeding_tong_4d', [])
        
        cham_gan_set = set()
        for c in exceeding_cham: cham_gan_set.update(CHAM_4CANG[c])
        tong_gan_set = set()
        for t in exceeding_tong: tong_gan_set.update(TONG_4CANG[t])

        last_appearance = {} 
        for idx, day in enumerate(days_data): # Newest first
            digits_on_day = set()
            for val in day['db_4so']:
                if len(val) >= 4 and val.isdigit():
                    # Take thousands digit (4th from right)
                    thousands_digit = int(val[-4])
                    digits_on_day.add(thousands_digit)
            for d in digits_on_day:
                if d not in last_appearance: last_appearance[d] = idx
            if len(last_appearance) == 10: break
        
        gaps = []
        max_days = len(days_data)
        for d in range(10):
            gap = last_appearance.get(d, max_days)
            gaps.append((d, gap))
            
        # Sort by gap descending (Khan) or increasing (Moi/Recent)
        try:
            mode = self.rare_mode_4d.get()
        except:
            mode = "Khan (Lâu chưa ra)"
            
        is_recent_mode = "Mới" in mode
        
        # Sort
        gaps.sort(key=lambda x: x[1], reverse=not is_recent_mode)
        top_5 = gaps[:5]
        
        for rank, (digit, gap) in enumerate(top_5, 1):
            gap_str = str(gap) if gap < max_days else f">{max_days}"
            header_prefix = "Mới" if is_recent_mode else "Khan"
            header = f"Top {rank}: Đầu {digit} ({header_prefix} {gap_str})\n"
            self.rare_thousands_text.insert(tk.END, header)
            
            # Generate set: X000-X999
            number_set = [f"{digit}{tail:03d}" for tail in range(1000)]
            
            # Filter overlap
            filtered_nums = []
            for num in number_set:
                if num in cham_gan_set and num in tong_gan_set:
                    filtered_nums.append(num)
            
            if filtered_nums:
                # Show all numbers as requested
                display_str = ",".join(filtered_nums)
                self.rare_thousands_text.insert(tk.END, f"   -> Trùng 3 Dàn ({len(filtered_nums)} số): {display_str}\n\n", 'highlight')
            else:
                self.rare_thousands_text.insert(tk.END, "\n")
            
        # Update selector tab
        gaps_dict = {d: g for d, g in gaps}
        exceeding_list = [d for d, g in top_5]
        hits_today = [d for d, g in gaps if g == 0]
        khan_data = (gaps_dict, exceeding_list, hits_today)
        self.update_rare_info_on_selector(khan_3d_str=None, khan_4d_str=khan_data)

    def update_3d_kep_stats(self, custom_data=None):
        """Tính Gap (Gan) cho các dàn 3D Kép và hiển thị vào sugg_3d_kep."""
        if custom_data is not None:
            self._last_processed_3d_data = custom_data

        data_to_use = getattr(self, '_last_processed_3d_data', None)
        if not data_to_use:
            return

        region = self.region_var.get()
        # Account for backtest offset
        try: offset = self.backtest_var.get()
        except: offset = 0
        
        # Handle offset if needed (data_to_use might already be sliced in backtest)
        # But for safety, we assume it's the full data_source if passed from render_tong_cham
        sliced_data = data_to_use[offset:] if offset < len(data_to_use) else data_to_use
        
        kep_keys = ["AAB", "ABA", "BAA", "KEP_TH", "L_BANG", "L_LECH", "L_AM", "L_TH", "CON_LAI"]
        gaps = {k: 99 for k in kep_keys}
        
        for idx, day in enumerate(sliced_data):
            if 'db_3so' in day:
                nums_on_day = set(day['db_3so'])
            else:
                db_3so_list = self.get_prize_numbers(day, width=3)
                nums_on_day = set(db_3so_list)
            
            for k_key in kep_keys:
                if gaps[k_key] == 99: # Only update if not found yet
                    pattern_nums = set(KEP_PATTERNS_3D[k_key])
                    if nums_on_day & pattern_nums:
                        gaps[k_key] = idx
            
            if all(g < 99 for g in gaps.values()): break

        self.sugg_3d_kep.config(state='normal')
        self.sugg_3d_kep.delete('1.0', tk.END)
        
        # Split into two lines as requested (matching wrap_at=4)
        kep_keys_L1 = ["AAB", "ABA", "BAA", "KEP_TH"]
        kep_keys_L2 = ["L_BANG", "L_LECH", "L_AM", "L_TH"]
        
        # Line 1
        self.sugg_3d_kep.insert(tk.END, "(Dự 1: ", 'normal')
        for i, k_key in enumerate(kep_keys_L1):
            gap = gaps[k_key]
            tag = 'rare' if gap >= 10 else 'normal'
            self.sugg_3d_kep.insert(tk.END, f"{k_key}({gap})", tag)
            if i < len(kep_keys_L1) - 1:
                self.sugg_3d_kep.insert(tk.END, ", ", 'normal')
        self.sugg_3d_kep.insert(tk.END, ")\n", 'normal')

        # Line 2
        self.sugg_3d_kep.insert(tk.END, "(Dự 2: ", 'normal')
        for i, k_key in enumerate(kep_keys_L2):
            gap = gaps[k_key]
            tag = 'rare' if gap >= 10 else 'normal'
            label_display = k_key.replace("L_", "L.") # Match UI labels
            self.sugg_3d_kep.insert(tk.END, f"{label_display}({gap})", tag)
            if i < len(kep_keys_L2) - 1:
                self.sugg_3d_kep.insert(tk.END, ", ", 'normal')
        self.sugg_3d_kep.insert(tk.END, ")", 'normal')
        
        self.sugg_3d_kep.config(state='disabled')

    def update_level_filter_options(self):
        """Update dropdown to show only available levels from current results"""
        if not hasattr(self, 'level_2d_filter'):
            return
        
        # Detect available levels
        available_levels = set()
        if hasattr(self, 'last_3d_hits') and self.last_3d_hits:
            available_levels.update(self.last_3d_hits.values())
        if hasattr(self, 'last_4d_hits') and self.last_4d_hits:
            available_levels.update(self.last_4d_hits.values())
        
        # Build dropdown values
        values = ["Tất cả"]
        if available_levels:
            max_level = max(available_levels)
            for i in range(1, max_level + 1):
                values.append(f"≥ Mức {i}")
        
        # Update dropdown
        current_value = self.level_2d_filter.get()
        self.level_2d_filter['values'] = values
        # Restore previous selection if still valid
        if current_value in values:
            self.level_2d_filter.set(current_value)
        else:
            self.level_2d_filter.set("Tất cả")

    def toggle_auto_scan(self):
        """Toggle auto-scan mode for level filter"""
        if self.auto_scan_active:
            # Stop scanning
            self.auto_scan_active = False
            self.auto_scan_btn.config(text="▶ Tự động", bg="#27ae60")
            if hasattr(self, 'auto_scan_job'):
                self.root.after_cancel(self.auto_scan_job)
        else:
            # Start scanning
            self.auto_scan_active = True
            self.auto_scan_btn.config(text="⏸ Dừng", bg="#e74c3c")
            self.current_scan_level = 1
            self.auto_scan_next_level()
    
    def auto_scan_next_level(self):
        """Cycle to next level in auto-scan mode"""
        if not self.auto_scan_active:
            return
        
        # Detect max level from current results
        max_level = 1
        if hasattr(self, 'last_3d_hits') and self.last_3d_hits:
            max_level = max(max_level, max(self.last_3d_hits.values(), default=1))
        if hasattr(self, 'last_4d_hits') and self.last_4d_hits:
            max_level = max(max_level, max(self.last_4d_hits.values(), default=1))
        
        # Set filter to current level
        if self.current_scan_level <= max_level:
            self.level_2d_filter.set(f"≥ Mức {self.current_scan_level}")
            # Trigger update
            self.update_selector_results()
            if hasattr(self, 'current_days_data') and self.current_days_data:
                self._render_combined_prediction(self.current_days_data, None)
            
            # Move to next level
            self.current_scan_level += 1
        else:
            # Reset to level 1
            self.current_scan_level = 1
        
        # Schedule next update (2 seconds delay)
        self.auto_scan_job = self.root.after(2000, self.auto_scan_next_level)


    # ====================================

    # =============================================================================
    # METHODS FOR SOI CẦU PHOI TAB (Integrated from tkinter_app.py)
    # =============================================================================

    def create_skp_view(self):
        """Create the UI for Soi Cầu Phoi tab."""
        # Split into Sidebar and Main area
        paned = ttk.PanedWindow(self.skp_tab_frame, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True)
        
        # Sidebar
        sidebar_frame = ttk.Frame(paned, padding=10, style='TFrame')
        paned.add(sidebar_frame, weight=1)
        
        # Main Content
        main_frame = ttk.Frame(paned, padding=10, style='TFrame')
        paned.add(main_frame, weight=4)
        
        # --- Sidebar Content ---
        ttk.Label(sidebar_frame, text="⚙️ ĐIỀU KHIỂN", font=("Segoe UI", 14, "bold"), foreground=self.accent_color).pack(anchor="w", pady=(0, 10))
        
        # Data Mode
        ttk.Label(sidebar_frame, text="Chế độ dữ liệu:", style='TLabel').pack(anchor="w")
        ttk.Radiobutton(sidebar_frame, text="Tháng", variable=self.skp_data_mode_var, value="Tháng").pack(anchor="w")
        ttk.Radiobutton(sidebar_frame, text="Năm", variable=self.skp_data_mode_var, value="Năm").pack(anchor="w")
        
        ttk.Button(sidebar_frame, text="🔄 Tải dữ liệu mới", command=self.skp_force_reload).pack(fill="x", pady=10)
        
        ttk.Separator(sidebar_frame, orient="horizontal").pack(fill="x", pady=10)
        ttk.Label(sidebar_frame, text="Cấu hình tìm cầu", font=("Segoe UI", 12, "bold")).pack(anchor="w", pady=(0, 5))
        
        # Day Selection
        ttk.Label(sidebar_frame, text="Chọn ngày:", style='TLabel').pack(anchor="w")
        self.skp_day_combo = ttk.Combobox(sidebar_frame, textvariable=self.skp_selected_day_var, state="readonly")
        self.skp_day_combo.pack(fill="x", pady=(0, 5))
        self.skp_day_combo.bind("<<ComboboxSelected>>", self.skp_update_patterns)
        
        # Month Selection (Hidden by default)
        self.skp_month_frame = ttk.Frame(sidebar_frame, style='TFrame')
        self.skp_month_frame.pack(fill="x", pady=(0, 5))
        ttk.Label(self.skp_month_frame, text="Chọn cột tháng:", style='TLabel').pack(anchor="w")
        self.skp_month_combo = ttk.Combobox(self.skp_month_frame, textvariable=self.skp_selected_month_var, state="readonly")
        self.skp_month_combo.pack(fill="x")
        self.skp_month_combo.bind("<<ComboboxSelected>>", self.skp_update_patterns)
        
        # Num Patterns
        ttk.Label(sidebar_frame, text="Số ngày chạy cầu:", style='TLabel').pack(anchor="w")
        tk.Spinbox(sidebar_frame, from_=1, to=5, textvariable=self.skp_num_patterns_var, command=self.skp_update_patterns, bg=self.secondary_bg, fg=self.fg_color).pack(fill="x", pady=(0, 5))
        
        # Checkboxes
        ttk.Label(sidebar_frame, text="Kiểu so khớp:", style='TLabel').pack(anchor="w")
        ttk.Checkbutton(sidebar_frame, text="Chính xác mẫu", variable=self.skp_exact_match_var).pack(anchor="w")
        ttk.Checkbutton(sidebar_frame, text="Có chứa (Kép 1, Lệch 2)", variable=self.skp_contains_both_var).pack(anchor="w")
        ttk.Checkbutton(sidebar_frame, text="🔄 Tìm đảo", variable=self.skp_allow_reverse_var).pack(anchor="w")
        
        # Search Direction
        ttk.Separator(sidebar_frame, orient="horizontal").pack(fill="x", pady=10)
        ttk.Label(sidebar_frame, text="Hướng tìm cầu:", font=("Segoe UI", 10, "bold")).pack(anchor="w")
        ttk.Radiobutton(sidebar_frame, text="↕ Lên Xuống (theo ngày)", variable=self.skp_search_direction_var, value="Lên Xuống").pack(anchor="w")
        ttk.Radiobutton(sidebar_frame, text="↔ Trái Phải (theo cột)", variable=self.skp_search_direction_var, value="Trái Phải").pack(anchor="w")
        ttk.Radiobutton(sidebar_frame, text="🔄 Đồng thời cả hai", variable=self.skp_search_direction_var, value="Đồng thời").pack(anchor="w")
        
        # Pattern Display
        ttk.Label(sidebar_frame, text="Mẫu hiện tại:", font=("Segoe UI", 10, "bold"), foreground="#f1c40f").pack(anchor="w", pady=(10, 5))
        self.skp_pattern_text = tk.Text(sidebar_frame, height=5, width=30, state="disabled", bg=self.secondary_bg, fg=self.fg_color, font=('Consolas', 9))
        self.skp_pattern_text.pack(fill="x")
        
        # --- Main Content ---
        self.skp_notebook = ttk.Notebook(main_frame)
        self.skp_notebook.pack(fill=tk.BOTH, expand=True)
        
        # Tab 1: Data & Cau
        self.skp_page1 = ttk.Frame(self.skp_notebook, style='TFrame')
        self.skp_notebook.add(self.skp_page1, text="📊 Dữ liệu & Cầu")
        self._build_skp_tab1()
        
        # Tab 2: Statistics
        self.skp_page2 = ttk.Frame(self.skp_notebook, style='TFrame')
        self.skp_notebook.add(self.skp_page2, text="📈 Thống kê mức số")
        self._build_skp_tab2()
        
        # Tab 3: Check Number
        self.skp_page3 = ttk.Frame(self.skp_notebook, style='TFrame')
        self.skp_notebook.add(self.skp_page3, text="🔍 Kiểm tra số")
        self._build_skp_tab3()
        
        self.skp_notebook.bind("<<NotebookTabChanged>>", self.skp_on_internal_tab_change)

    def _build_skp_tab1(self):
        ctrl_frame = ttk.Frame(self.skp_page1, style='TFrame')
        ctrl_frame.pack(fill="x", pady=5)
        
        ttk.Label(ctrl_frame, text="Chọn cách soi:", style='TLabel').pack(side="left", padx=5)
        step_options = ["Tất cả các cách"] + [f"Cách {i}" for i in range(6)]
        self.skp_step_combo = ttk.Combobox(ctrl_frame, values=step_options, textvariable=self.skp_selected_step_var, state="readonly", width=15)
        self.skp_step_combo.pack(side="left", padx=5)
        self.skp_step_combo.bind("<<ComboboxSelected>>", lambda e: self.skp_refresh_tab1())
        
        ttk.Label(ctrl_frame, text="Chế độ xem:", style='TLabel').pack(side="left", padx=(20, 5))
        ttk.Combobox(ctrl_frame, values=["Highlight Cầu", "Dữ liệu gốc"], textvariable=self.skp_view_mode_var, state="readonly", width=15).pack(side="left", padx=5)
        
        self.skp_clear_hl_btn = ttk.Button(ctrl_frame, text="❌ Xóa Highlight", command=self.skp_clear_highlight)
        self.skp_clear_hl_btn.pack(side="left", padx=20)
        self.skp_clear_hl_btn.state(['disabled'])

        self.skp_pred_label = tk.Label(self.skp_page1, text="", bg=self.secondary_bg, fg=self.accent_color, font=("Segoe UI", 12, "bold"), anchor="w", padx=10, pady=5)
        self.skp_pred_label.pack(fill="x", pady=5)
        
        # Grid Container
        grid_container = tk.Frame(self.skp_page1, bg=self.bg_color)
        grid_container.pack(fill="both", expand=True)
        
        self.skp_canvas = tk.Canvas(grid_container, bg=self.bg_color, highlightthickness=0)
        v_scroll = ttk.Scrollbar(grid_container, orient="vertical", command=self.skp_canvas.yview)
        h_scroll = ttk.Scrollbar(grid_container, orient="horizontal", command=self.skp_canvas.xview)
        
        self.skp_canvas.configure(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)
        v_scroll.pack(side="right", fill="y")
        h_scroll.pack(side="bottom", fill="x")
        self.skp_canvas.pack(side="left", fill="both", expand=True)
        
        self.skp_grid_frame = tk.Frame(self.skp_canvas, bg=self.bg_color)
        self.skp_canvas.create_window((0, 0), window=self.skp_grid_frame, anchor="nw")
        self.skp_grid_frame.bind("<Configure>", lambda e: self.skp_canvas.configure(scrollregion=self.skp_canvas.bbox("all")))
        self._setup_canvas_mousewheel(self.skp_canvas)

    def _build_skp_tab2(self):
        paned = ttk.PanedWindow(self.skp_page2, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # Left Panel: Scrollable Detailed List
        left_f = ttk.Frame(paned, style='TFrame')
        paned.add(left_f, weight=1)
        
        self.skp_tab2_canvas = tk.Canvas(left_f, bg=self.bg_color, highlightthickness=0)
        v_scroll = ttk.Scrollbar(left_f, orient="vertical", command=self.skp_tab2_canvas.yview)
        self.skp_tab2_canvas.configure(yscrollcommand=v_scroll.set)
        
        v_scroll.pack(side="right", fill="y")
        self.skp_tab2_canvas.pack(side="left", fill="both", expand=True)
        
        self.skp_tab2_scroll_frame = tk.Frame(self.skp_tab2_canvas, bg=self.bg_color)
        self.skp_tab2_canvas.create_window((0, 0), window=self.skp_tab2_scroll_frame, anchor="nw")
        
        self.skp_tab2_scroll_frame.bind("<Configure>", lambda e: self.skp_tab2_canvas.configure(scrollregion=self.skp_tab2_canvas.bbox("all")))
        self._setup_canvas_mousewheel(self.skp_tab2_canvas)
        self.skp_tab2_left = self.skp_tab2_scroll_frame
        
        # Right Panel: Summary
        right_f = ttk.Frame(paned, style='TFrame')
        paned.add(right_f, weight=1)
        self.skp_tab2_right = right_f
        
        ttk.Label(right_f, text="TỔNG HỢP MỨC SỐ", font=("Segoe UI", 12, "bold"), foreground=self.accent_color).pack(anchor="w")
        self.skp_summary_text = tk.Text(right_f, height=20, bg=self.secondary_bg, fg=self.fg_color, font=('Consolas', 10))
        self.skp_summary_text.pack(fill="both", expand=True, pady=5)

    def _build_skp_tab3(self):
        input_frame = ttk.Frame(self.skp_page3, style='TFrame')
        input_frame.pack(fill="x", pady=10)
        
        ttk.Label(input_frame, text="Nhập số (2 chữ số):", style='TLabel').pack(side="left", padx=5)
        entry = ttk.Entry(input_frame, textvariable=self.skp_check_num_var, width=5)
        entry.pack(side="left", padx=5)
        ttk.Button(input_frame, text="Kiểm tra", command=self.skp_check_number).pack(side="left", padx=5)
        
        cols = ("Cách", "Mức (↓)", "Mức (↑)")
        self.skp_check_tree = ttk.Treeview(self.skp_page3, columns=cols, show="headings", style='Cyber.Treeview')
        for col in cols:
            self.skp_check_tree.heading(col, text=col)
            self.skp_check_tree.column(col, width=100, anchor="center")
        self.skp_check_tree.pack(fill="both", expand=True, pady=10)

    # --- Handlers ---

    def skp_on_mode_change(self):
        self.skp_load_data()

    def skp_on_exact_change(self):
        if self.skp_exact_match_var.get():
            # Temporarily disable trace to avoid recursion if needed, 
            # but simple set() should be fine as they shouldn't both be true.
            if self.skp_contains_both_var.get():
                self.skp_contains_both_var.set(False)
        self.skp_update_patterns()

    def skp_on_contains_change(self):
        if self.skp_contains_both_var.get():
            if self.skp_exact_match_var.get():
                self.skp_exact_match_var.set(False)
        self.skp_update_patterns()

    def skp_force_reload(self):
        """Force reload data from web and clear cache."""
        mode = "month" if self.skp_data_mode_var.get() == "Tháng" else "year"
        if mode in self.skp_cache: del self.skp_cache[mode]
        
        # Check cache first
        if mode in self.skp_cache:
            self.skp_on_data_loaded(self.skp_cache[mode])
            return

        self.root.config(cursor="watch")
        def task():
            df = skp_fetch_data(mode)
            self.root.after(0, lambda: self.skp_on_data_loaded(df))
        threading.Thread(target=task, daemon=True).start()

    def skp_on_data_loaded(self, df):
        self.root.config(cursor="")
        if df is None:
            messagebox.showerror("Lỗi", "Không thể tải dữ liệu Phoi.")
            return
        
        mode = "month" if self.skp_data_mode_var.get() == "Tháng" else "year"
        self.skp_cache[mode] = df
        self.skp_df = df
        days = [str(i) for i in range(1, len(df) + 1)]
        self.skp_day_combo['values'] = days
        
        now = datetime.now()
        cur_d = now.day
        if now.hour > 18 or (now.hour == 18 and now.minute >= 30): def_d = cur_d + 1
        else: def_d = cur_d
        if def_d > len(days): def_d = len(days)
        
        if str(def_d) in days: self.skp_day_combo.set(str(def_d))
        elif str(cur_d) in days: self.skp_day_combo.set(str(cur_d))
        else: self.skp_day_combo.current(len(days)-1)

        if self.skp_data_mode_var.get() == "Năm":
            self.skp_month_frame.pack(fill="x", pady=(0, 5))
            months = [c for c in df.columns if c.startswith("TH")]
            self.skp_month_combo['values'] = months
            cur_m_idx = now.month - 1
            def_m = months[cur_m_idx] if cur_m_idx < len(months) else months[0]
            self.skp_month_combo.set(def_m)
        else:
            self.skp_month_frame.pack_forget()
            self.skp_selected_month_var.set("")
        self.skp_update_patterns()

    def skp_update_patterns(self, event=None):
        if self.skp_df is None: return
        day_str = self.skp_selected_day_var.get()
        if not day_str or not day_str.isdigit(): return
        row_idx = int(day_str) - 1
        is_year = (self.skp_data_mode_var.get() == "Năm")
        col_source = self.skp_selected_month_var.get() if is_year else str(datetime.now().year)
        num_p = self.skp_num_patterns_var.get()
        patterns, _ = skp_get_patterns(self.skp_df, is_year, row_idx, col_source, num_p)
        self.skp_pattern_text.config(state="normal")
        self.skp_pattern_text.delete("1.0", tk.END)
        rev = self.skp_allow_reverse_var.get()
        for i, p in enumerate(patterns):
            txt = f"Mẫu {i+1}: {p} hoặc {p[::-1]}\n" if rev and p and p[0] != p[1] else f"Mẫu {i+1}: {p}\n"
            self.skp_pattern_text.insert(tk.END, txt)
        self.skp_pattern_text.config(state="disabled")
        idx = self.skp_notebook.index(self.skp_notebook.select())
        if idx == 0: self.skp_refresh_tab1()
        elif idx == 1: self.skp_refresh_tab2()

    def skp_on_internal_tab_change(self, event):
        idx = self.skp_notebook.index(self.skp_notebook.select())
        if idx == 0: self.skp_refresh_tab1()
        elif idx == 1: self.skp_refresh_tab2()

    def skp_get_params(self):
        ds = self.skp_selected_day_var.get()
        if not ds or not ds.isdigit(): return [], 0, False, set(), None, 0
        r_idx = int(ds) - 1
        is_y = (self.skp_data_mode_var.get() == "Năm")
        col_s = self.skp_selected_month_var.get() if is_y else str(datetime.now().year)
        num_p = self.skp_num_patterns_var.get()
        patterns, p_months = skp_get_patterns(self.skp_df, is_y, r_idx, col_s, num_p)
        return patterns, num_p, is_y, p_months, col_s, r_idx

    def skp_refresh_tab1(self):
        if self.skp_df is None: return
        patterns, num_p, is_y, p_months, col_s, row_idx = self.skp_get_params()
        if not patterns or not all(patterns): return
        
        for w in self.skp_grid_frame.winfo_children(): w.destroy()
        
        exact = self.skp_exact_match_var.get()
        step_val = self.skp_selected_step_var.get()
        target_step = int(step_val.split(" ")[1]) if "Cách" in step_val else None
        rev = self.skp_allow_reverse_var.get()
        mode = self.skp_search_direction_var.get()
        
        res = {}; c_pos = set(); p_pos = set()
        if mode in ["Lên Xuống", "Đồng thời"]:
            r_v, c_v, p_v = skp_scan_cau(self.skp_df, patterns, num_p, exact, is_y, p_months, col_s, target_step, rev)
            res.update(r_v); c_pos.update(c_v); p_pos.update(p_v)
        if mode in ["Trái Phải", "Đồng thời"]:
            r_h, c_h, p_h = skp_scan_cau_horizontal(self.skp_df, patterns, num_p, exact, is_y, p_months, col_s, row_idx, target_step, rev)
            res.update(r_h); c_pos.update(c_h); p_pos.update(p_h)
            
        all_pairs = []
        for k in res: all_pairs.extend(res[k]['pairs'])
        
        if all_pairs:
            cnt = Counter(all_pairs)
            top = cnt.most_common(5)
            txt = "⭐ Dự đoán (Top common): " + ", ".join([f"{p}({c})" for p, c in top])
            self.skp_pred_label.config(text=txt)
        else: self.skp_pred_label.config(text="Không tìm thấy cầu phù hợp.")

        show_hl = (self.skp_view_mode_var.get() == "Highlight Cầu")
        df_cols = list(self.skp_df.columns)
        for j, col in enumerate(df_cols):
            tk.Label(self.skp_grid_frame, text=col, bg=self.accent_color, fg="white", font=('Segoe UI', 9, 'bold'), width=8, relief='solid').grid(row=0, column=j, sticky='nsew')
        
        # If we have a highlight target, we only want to show the bridge(s) for that specific target
        target_bridge_set = set()
        if self.skp_highlight_target:
            for k in res:
                for item in res[k]['items']:
                    if item['predict_pos'] == self.skp_highlight_target:
                        target_bridge_set.update(item['cau_pos'])
                        target_bridge_set.add(item['predict_pos'])

        for i in range(len(self.skp_df)):
            for j, col in enumerate(df_cols):
                val = self.skp_df.iloc[i][col]
                bg = self.secondary_bg; fg = self.fg_color; weight = 'normal'
                
                pos = (i, col)
                is_hit = False
                if self.skp_highlight_target:
                    if pos == self.skp_highlight_target: bg = '#ff4b4b'; fg = 'white'; weight = 'bold'; is_hit = True
                    elif pos in target_bridge_set: bg = '#ffff99'; fg = 'black'; weight = 'bold'
                elif show_hl:
                    if pos in p_pos: bg = '#ff4b4b'; fg = 'white'; weight = 'bold'; is_hit = True
                    elif pos in c_pos: bg = '#ffff99'; fg = 'black'; weight = 'bold'
                
                lbl = tk.Label(self.skp_grid_frame, text=val, bg=bg, fg=fg, font=('Consolas', 9, weight), width=8, relief='flat', padx=2, pady=2)
                lbl.grid(row=i+1, column=j, sticky='nsew')
                if is_hit:
                    lbl.bind("<Button-1>", lambda e, p=pos: self.skp_show_cau_detail(p, res))
                    lbl.config(cursor="hand2")

    def skp_show_cau_detail(self, pos, all_results):
        self.skp_highlight_target = pos
        self.skp_clear_hl_btn.state(['!disabled'])
        self.skp_refresh_tab1()

    def skp_clear_highlight(self):
        self.skp_highlight_target = None
        self.skp_clear_hl_btn.state(['disabled'])
        self.skp_refresh_tab1()

    def skp_refresh_tab2(self):
        if self.skp_df is None: return
        patterns, num_p, is_y, p_months, col_s, r_idx = self.skp_get_params()
        if not patterns or not all(patterns): return
        
        for w in self.skp_tab2_left.winfo_children(): w.destroy()
        self.skp_summary_text.delete("1.0", tk.END)
        
        exact = self.skp_exact_match_var.get()
        rev = self.skp_allow_reverse_var.get()
        mode = self.skp_search_direction_var.get()
        
        res = {}
        if mode in ["Lên Xuống", "Đồng thời"]:
            r_v, _, _ = skp_scan_cau(self.skp_df, patterns, num_p, exact, is_y, p_months, col_s, None, rev)
            res.update(r_v)
        if mode in ["Trái Phải", "Đồng thời"]:
            r_h, _, _ = skp_scan_cau_horizontal(self.skp_df, patterns, num_p, exact, is_y, p_months, col_s, r_idx, None, rev)
            res.update(r_h)
            
        all_pairs = []
        for i, (key, data) in enumerate(res.items()):
            count = data['count']
            items = data['items']
            pairs = data['pairs']
            all_pairs.extend(pairs)
            
            f = tk.LabelFrame(self.skp_tab2_left, bg=self.skp_COLORS[i % len(self.skp_COLORS)], pady=5, padx=5, text=f"{key}: {count} kết quả", fg="black")
            f.pack(fill="x", pady=2)
            
            for item in items:
                v = item['value']
                pos = item['predict_pos']
                
                m_pos = item.get('match_position')
                if exact:
                    num_found = v[m_pos:m_pos+2] if m_pos is not None else v[-2:]
                else:
                    unique_digits = sorted(list(set([d for d in v if d.isdigit()])))
                    num_found = "Nhị hợp (" + "".join(unique_digits) + ")"
                
                txt = f"🎯 Số: {num_found} (Giải: {v})"
                lbl = tk.Label(f, text=txt, bg=f['bg'], foreground="#0000AA", cursor="hand2", font=('Segoe UI', 9, 'underline'))
                lbl.pack(anchor="w")
                lbl.bind("<Button-1>", lambda e, p=pos: self.skp_jump_to_cau(p))
        
        if all_pairs:
            cnt = Counter(all_pairs)
            self.skp_summary_text.insert(tk.END, "PHÂN TÍCH TỔNG HỢP MỨC SỐ:\n\n")
            
            # Group by count
            by_count = defaultdict(list)
            for num, count in cnt.items():
                by_count[count].append(num)
            
            # Sort counts descending
            sorted_counts = sorted(by_count.keys(), reverse=True)
            for c in sorted_counts:
                nums_str = ", ".join(sorted(by_count[c]))
                stars = "⭐" * min(c, 5)
                self.skp_summary_text.insert(tk.END, f"Mức {c}: {nums_str} {stars}\n")
            
            # Additional groupings
            tongs = Counter([sum(int(d) for d in p)%10 for p in all_pairs])
            self.skp_summary_text.insert(tk.END, "\nTHỐNG KÊ THEO TỔNG:\n")
            for t, c in tongs.most_common(): self.skp_summary_text.insert(tk.END, f"Tổng {t}: {c} lần\n")

    def skp_check_number(self):
        num = self.skp_check_num_var.get().strip()
        if not num or len(num) != 2 or not num.isdigit():
            messagebox.showwarning("Cảnh báo", "Vui lòng nhập số có 2 chữ số.")
            return
        
        for item in self.skp_check_tree.get_children(): self.skp_check_tree.delete(item)
        if self.skp_df is None: return
        patterns, num_p, is_y, p_months, col_s, r_idx = self.skp_get_params()
        if not patterns or not all(patterns): return
        
        exact = self.skp_exact_match_var.get()
        rev = self.skp_allow_reverse_var.get()
        
        res_v, _, _ = skp_scan_cau(self.skp_df, patterns, num_p, exact, is_y, p_months, col_s, None, rev)
        
        for i in range(6):
            key_down = f"Trên xuống (↓) - Cách {i}"
            key_up = f"Dưới lên (↑) - Cách {i}"
            
            m_down = res_v.get(key_down, {}).get('pairs', []).count(num)
            m_up = res_v.get(key_up, {}).get('pairs', []).count(num)
            self.skp_check_tree.insert("", tk.END, values=(f"Cách {i}", m_down, m_up))

    def skp_jump_to_cau(self, predict_pos):
        """Switch to Tab 1 and highlight the specific bridge for the target."""
        self.skp_highlight_target = predict_pos
        self.skp_notebook.select(0) # Select first tab
        self.skp_clear_hl_btn.state(['!disabled'])
        self.skp_refresh_tab1()

    # ====================================
    # TAB 8: BỆT CHẠM ĐB
    # ====================================

    def create_bet_cham_view(self):
        """Create the professional Canvas-based split UI for Bệt Chạm ĐB analysis."""
        # Main header
        header_frame = tk.Frame(self.bet_cham_frame, bg="#1a472a")
        header_frame.pack(fill=tk.X)
        
        self.bet_cham_title_label = tk.Label(
            header_frame, 
            text="📑 PHÂN TÍCH BỆT THẲNG & DÀN NUÔI CHẠM NHỊ HỢP (60 NGÀY)", 
            bg="#1a472a", fg="white", font=('Segoe UI', 12, 'bold')
        )
        self.bet_cham_title_label.pack(side=tk.LEFT, padx=20, pady=8)

        # Source Switcher for Bệt Detection
        src_frame = tk.Frame(header_frame, bg="#1a472a")
        src_frame.pack(side=tk.RIGHT, padx=20)
        
        tk.Label(src_frame, text="Nguồn soi:", bg="#1a472a", fg="white", font=('Segoe UI', 10)).pack(side=tk.LEFT, padx=5)
        self.bet_cham_src_var = tk.StringVar(value="XSMB (ĐB)")
        self.bet_cham_src_combo = ttk.Combobox(
            src_frame, textvariable=self.bet_cham_src_var, 
            values=["XSMB (ĐB)", "Điện Toán", "Thần Tài"], 
            state="readonly", width=15
        )
        self.bet_cham_src_combo.pack(side=tk.LEFT, padx=5)
        self.bet_cham_src_combo.bind("<<ComboboxSelected>>", lambda e: self.render_bet_cham_analysis())

        # Content Container (Split View)
        content_container = tk.Frame(self.bet_cham_frame, bg=self.bg_color)
        content_container.pack(fill=tk.BOTH, expand=True)

        # === LEFT PANEL: CANVAS GRID (70%) ===
        left_frame = tk.Frame(content_container, bg=self.bg_color)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Scrollable Canvas
        canvas_container = tk.Frame(left_frame, bg=self.bg_color)
        canvas_container.pack(fill=tk.BOTH, expand=True)

        self.bet_cham_canvas = tk.Canvas(canvas_container, bg="#1e1e1e", highlightthickness=0)
        yscroll = ttk.Scrollbar(canvas_container, orient=tk.VERTICAL, command=self.bet_cham_canvas.yview)
        xscroll = ttk.Scrollbar(left_frame, orient=tk.HORIZONTAL, command=self.bet_cham_canvas.xview)
        
        self.bet_cham_canvas.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        
        yscroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.bet_cham_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._setup_canvas_mousewheel(self.bet_cham_canvas)
        xscroll.pack(side=tk.BOTTOM, fill=tk.X)

        # Container inside canvas
        self.bet_cham_grid_frame = tk.Frame(self.bet_cham_canvas, bg="#1e1e1e")
        self.bet_cham_canvas.create_window((0, 0), window=self.bet_cham_grid_frame, anchor='nw')

        # === RIGHT PANEL: STATS SIDEBAR (30%) ===
        right_frame = tk.Frame(content_container, bg="#2d2d2d", width=350)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, padx=(2, 0))

        tk.Label(right_frame, text="📊 THỐNG KÊ CHI TIẾT", bg="#333333", fg=self.accent_color, 
                 font=('Segoe UI', 11, 'bold'), pady=5).pack(fill=tk.X)


        text_frame = tk.Frame(right_frame, bg="#1e1e1e")
        text_frame.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)

        stats_scroll = ttk.Scrollbar(text_frame)
        stats_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.bet_cham_stats_text = tk.Text(
            text_frame, wrap=tk.WORD, bg="#1e1e1e", fg="white", 
            font=('Consolas', 10), width=35, yscrollcommand=stats_scroll.set,
            borderwidth=0, padx=10, pady=10
        )
        self.bet_cham_stats_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        stats_scroll.config(command=self.bet_cham_stats_text.yview)

        # Variables for sidebar checklist
        self.bet_cham_vars = {} 
        self.bet_cham_nurture_auto = [] 
        self.bet_cham_nurture_manual = []
        self.bet_cham_stats_text.tag_configure('section_header', font=('Segoe UI', 10, 'bold'), foreground="#9b59b6") # Purple for titles
        self.bet_cham_stats_text.tag_configure('date_header', font=('Segoe UI', 10, 'bold'), foreground=self.accent_color)
        self.bet_cham_stats_text.tag_configure('dan_info', font=('Consolas', 9), foreground="#bdc3c7")
        self.bet_cham_stats_text.tag_configure('hit_info', foreground="#2ecc71")
        self.bet_cham_stats_text.tag_configure('miss_info', foreground="#e74c3c")

        # === BOTTOM SUMMARY ===
        self.bet_cham_footer = tk.Frame(self.bet_cham_frame, bg="#333333", height=30)
        self.bet_cham_footer.pack(fill=tk.X)
        self.bet_cham_summary_label = tk.Label(
            self.bet_cham_footer, text="📊 Đang đợi dữ liệu phân tích...", 
            bg="#333333", fg="white", font=('Segoe UI', 10)
        )
        self.bet_cham_summary_label.pack(pady=5)

    def cross_check_with_matrix(self, dan_list):
        """Find numbers in dan_list that exist in Matrix high levels (Freq >= 3)."""
        convergent = set()
        if not hasattr(self, 'matrix_2d_freq') or not self.matrix_2d_freq:
            return convergent
            
        for num in dan_list:
            if self.matrix_2d_freq.get(num, 0) >= 3:
                convergent.add(num)
        return convergent

    def update_bet_cham_sidebar_summary(self):
        """Update the 'Mức số' section in the sidebar based on checked items."""
        # Find where the summary starts
        summary_mark = self.bet_cham_stats_text.search("📊 MỨC SỐ", "1.0")
        if summary_mark:
            self.bet_cham_stats_text.delete(f"{summary_mark} - 2 lines", tk.END)
        
        self.bet_cham_stats_text.insert(tk.END, "\n" + "="*30 + "\n")
        self.bet_cham_stats_text.insert(tk.END, "📊 MỨC SỐ (Tổng hợp các dàn chọn):\n", 'date_header')
        
        pair_counts = Counter()
        any_checked = False
        
        # 1. From Auto-detected (0 hits)
        for date, dan_list, b_val, t_val, d_str in self.bet_cham_nurture_auto:
            if self.bet_cham_vars.get(date) and self.bet_cham_vars[date].get():
                pair_counts.update(dan_list)
                any_checked = True
                
        # 2. From Manual selection (grid)
        for date, dan_list, b_val, t_val, d_str in self.bet_cham_nurture_manual:
            sidebar_key = f"man_{date}"
            if self.bet_cham_vars.get(sidebar_key) and self.bet_cham_vars[sidebar_key].get():
                pair_counts.update(dan_list)
                any_checked = True
        
        if not any_checked:
            self.bet_cham_stats_text.insert(tk.END, "(Chưa chọn dàn nào để xem mức số)\n", 'dan_info')
            return

        # Group by frequencies
        freq_groups = {}
        for pair, count in pair_counts.items():
            freq_groups.setdefault(count, []).append(pair)
        
        for count in sorted(freq_groups.keys(), reverse=True):
            pairs = sorted(freq_groups[count])
            tag = 'hit_info' if count >= 3 else 'dan_info'
            self.bet_cham_stats_text.insert(tk.END, f"Mức {count} ({len(pairs)}): ", tag)
            self.bet_cham_stats_text.insert(tk.END, f"{', '.join(pairs)}\n", 'dan_info')

    def render_bet_cham_analysis(self):
        """Perform the Bệt pattern analysis and render the Canvas-based grid."""
        if not hasattr(self, 'bet_cham_canvas'): return
        
        region = self.region_var.get()
        # Nguồn soi bệt (XSMB, Điện Toán, Thần Tài)
        b_src_var = getattr(self, 'bet_cham_src_var', None)
        b_src = b_src_var.get() if b_src_var else "XSMB (ĐB)"
        
        # Select data source based on region
        if region == "Miền Bắc":
            data_source = self.master_data
        else:
            data_source = self.station_data
            
        if not data_source:
            self.bet_cham_summary_label.config(text="⚠️ Không có dữ liệu để hiển thị")
            return

        # NEW: Restriction for MN/MT All Days or All Stations
        if region != "Miền Bắc":
            day = self.day_var.get()
            stn = self.station_var.get()
            if day == "Tất cả" or stn == "Tất cả":
                for widget in self.bet_cham_grid_frame.winfo_children(): widget.destroy()
                self.bet_cham_summary_label.config(text="⚠️ Bệt Chạm chỉ hỗ trợ khi chọn 1 đài cụ thể (theo tuần) để đảm bảo tính chính xác của nhịp.")
                return

        # Prepare normalized data for analysis
        raw_data = []
        for d in data_source:
            val_pattern = ""
            if region != "Miền Bắc":
                val_pattern = d.get('db', '')
            else:
                if b_src == "Điện Toán":
                    val_pattern = "".join(d.get('dt_numbers', []))
                elif b_src == "Thần Tài":
                    val_pattern = d.get('tt_number', '')
                else:
                    val_pattern = d.get('xsmb_full', '')
            
            # Lấy kết quả để soi nổ (tracking)
            if region == "Miền Bắc":
                val_track = d.get('xsmb_full', d.get('db', ''))
            else:
                val_track = d.get('db', '')
            
            if val_pattern:
                entry = d.copy()
                entry['db_pattern'] = val_pattern
                entry['track_tail'] = val_track[-2:] if len(val_track) >= 2 else ""
                raw_data.append(entry)
        
        if not raw_data: return

        # Chronological order for vertical bệt check
        full_chrono_data = sorted(raw_data, key=lambda x: datetime.strptime(x['date'], "%d/%m/%Y"))
        dates_list = [d['date'] for d in full_chrono_data]
        
        # Apply Backtest Slice: If backtest=1, we ignore the absolute last day from the rows
        try: offset = self.backtest_var.get()
        except: offset = 0
        if offset > 0:
            chrono_data = full_chrono_data[:-offset] if offset < len(full_chrono_data) else []
        else:
            chrono_data = full_chrono_data
            
        # Mapping for hits tracking (Always based on FULL raw_data so we can see hits in the 'future' relative to backtest)
        date_to_track_tail = {d['date']: d['track_tail'] for d in raw_data}

        rows = []
        total_hits = 0
        total_checks = 0
        total_nuoi_stats = 0

        # Limit analysis display to 60 days, but keep stats range for summary at 28
        DISPLAY_DAYS = 60
        STATS_RANGE = 28
        start_loop = max(1, len(chrono_data) - DISPLAY_DAYS)
        for i in range(start_loop, len(chrono_data)):
            curr = chrono_data[i]
            prev = chrono_data[i-1]
            db_curr = curr['db_pattern']
            db_prev = prev['db_pattern']

            if len(db_curr) < 4 or len(db_prev) < 4: continue

            # Split and Pad (6 digits for MN/MT/Mixed compatibility)
            digits_curr = list(db_curr)
            while len(digits_curr) < 6: digits_curr.insert(0, "")
            digits_prev = list(db_prev)
            while len(digits_prev) < 6: digits_prev.insert(0, "")

            # Bệt Thẳng
            bets = []
            for j in range(6):
                if digits_curr[j] != "" and digits_curr[j] == digits_prev[j]:
                    bets.append(digits_curr[j])
            
            bet_val = ",".join(bets) if bets else ""
            dan_nuoi = ""
            dan_list = []
            
            if bets:
                # LogicNhị hợp của 2 GĐB phát hiện Bệt
                representative_digits = set([c for c in db_curr if c.isdigit()])
                representative_digits.update([c for c in db_prev if c.isdigit()])
                
                if representative_digits:
                    for b in bets:
                        for d in representative_digits:
                            p1, p2 = f"{b}{d}", f"{d}{b}"
                            if p1 not in dan_list: dan_list.append(p1)
                            if p2 not in dan_list: dan_list.append(p2)
                    dan_list.sort()
                    dan_nuoi = ",".join(dan_list)

            # Tracking 28 days (STRICTLY MB GĐB)
            tracking = [] # List of (date_str, is_hit)
            hit_count_capped = 0  # Initialize before conditional to prevent UnboundLocalError
            
            is_within_stats = (len(chrono_data) - i) <= STATS_RANGE
            if dan_list:
                if is_within_stats:
                    total_nuoi_stats += 1
                
                # Search hits in data respecting backtest (simulation mode)
                # Only use chrono_data which is already sliced by backtest offset
                search_limit = len(chrono_data)
                for j in range(i + 1, search_limit):
                    idx = j - (i + 1) # Index relative to the current row's next day
                    if idx < 28: # Max 28 days
                        fut_date = chrono_data[j]['date']
                        # Use the track tail from the pre-computed map, but since we are iterating
                        # chrono_data, this just fetches the tail for that valid historic day.
                        fut_track_tail = date_to_track_tail.get(fut_date, "")
                        is_hit = fut_track_tail in dan_list if fut_track_tail else False
                        tracking.append((fut_date, is_hit, fut_track_tail))
                    else:
                        break
                
                # IMPORTANT: Count hits for sidebar 'Chưa ra' status (strictly up to backtest cutoff)
                # chrono_data is already sliced by [:-offset]
                search_limit = len(chrono_data)
                for j in range(i + 1, search_limit):
                    idx = j - (i + 1)
                    if idx < 28:
                        fut_date = chrono_data[j]['date']
                        fut_track_tail = date_to_track_tail.get(fut_date, "")
                        if fut_track_tail in dan_list:
                            hit_count_capped += 1
                            if is_within_stats:
                                total_hits += 1 
                        if is_within_stats:
                            total_checks += 1
                
                # Fill remaining slots with placeholders for display
                while len(tracking) < 28:
                    tracking.append((None, None, ""))

            rows.append({
                "date": curr['date'], "db": db_curr, "tail": curr['track_tail'],
                "digits": digits_curr, "bet": bet_val,
                "dan": dan_nuoi, "dan_list": dan_list, "total": len(dan_list),
                "tracking": tracking, "is_bet": bool(bets),
                "date_key": curr['date'],
                "hit_count_capped": hit_count_capped, # For sidebar 'Chưa ra'
                "is_within_stats": is_within_stats
            })

        # --- RENDER TO CANVAS ---
        for widget in self.bet_cham_grid_frame.winfo_children(): widget.destroy()
        
        CELL_W_SEL, CELL_W_STT, CELL_W_DATE, CELL_W_DB, CELL_W_TAIL = 30, 40, 85, 80, 40
        CELL_W_DIGIT, CELL_W_BET, CELL_W_DAN, CELL_W_SIGMA = 25, 45, 120, 30
        CELL_W_TRK = 24
        ROW_H = 26
        
        # Headers
        headers_fixed = [
            ("Chọn", CELL_W_SEL), ("STT", CELL_W_STT), ("Ngày", CELL_W_DATE), ("GĐB", CELL_W_DB), ("Đuôi", CELL_W_TAIL),
            ("0", CELL_W_DIGIT), ("A", CELL_W_DIGIT), ("B", CELL_W_DIGIT), ("C", CELL_W_DIGIT), ("D", CELL_W_DIGIT), ("E", CELL_W_DIGIT),
            ("Bệt", CELL_W_BET), ("Dàn", CELL_W_DAN), ("Σ", CELL_W_SIGMA)
        ]
        
        cur_x = 0
        grid_frame = self.bet_cham_grid_frame
        
        def create_header(x, text, w, bg=None):
            bg_color = bg if bg else self.accent_color
            lbl = tk.Label(grid_frame, text=text, width=w//8, bg=bg_color, fg="white", 
                           font=('Segoe UI', 9, 'bold'), relief='flat', borderwidth=0, padx=2, pady=5)
            lbl.place(x=x, y=0, width=w, height=ROW_H+4)

        for h_text, h_w in headers_fixed:
            create_header(cur_x, h_text, h_w)
            cur_x += h_w
            
        # N1-N28 Headers with Zone Colors
        for n in range(1, 29):
            h_text = f"N{n}"
            h_w = CELL_W_TRK
            # Zone definitions (4 zones, 7 days each)
            if 1 <= n <= 7: zone_bg = "#2980b9"   # Blue
            elif 8 <= n <= 14: zone_bg = "#f39c12" # Yellow/Orange
            elif 15 <= n <= 21: zone_bg = "#c0392b" # Red/Maroon
            else: zone_bg = "#8e44ad"             # Purple/Magenta
            
            create_header(cur_x, h_text, h_w, bg=zone_bg)
            cur_x += h_w

        # Data Rows (Newest First)
        self.bet_cham_stats_text.delete('1.0', tk.END)
        display_rows = list(reversed(rows))
        
        self.bet_cham_nurture_auto = [] # List of (date, dan_list) for 0-hit patterns
        self.bet_cham_nurture_manual = [] # List of (date, dan_list) for manual selection
        
        # Grid variables for manual selection (checkboxes in grid)
        if not hasattr(self, 'bet_cham_grid_vars'): self.bet_cham_grid_vars = {}

        for idx, r in enumerate(display_rows):
            cur_y = (idx + 1) * (ROW_H + 1) + 4
            row_bg = "#2d2d2d" if idx % 2 == 0 else "#1e1e1e"
            
            x = 0
            # Helper to draw cell
            def draw_cell(cx, cy, cw, val, bg=row_bg, fg="white", font=('Consolas', 9), icon=None, highlight=False):
                actual_bg = "#f1c40f" if highlight else bg
                actual_fg = "black" if highlight else fg
                c_lbl = tk.Label(grid_frame, text=val if not icon else icon, bg=actual_bg, fg=actual_fg, font=font, borderwidth=0)
                c_lbl.place(x=cx, y=cy, width=cw-1, height=ROW_H)

            # Manual Selection Checkbox
            date_key = r['date']
            if date_key not in self.bet_cham_grid_vars:
                self.bet_cham_grid_vars[date_key] = tk.BooleanVar(value=False)
            
            sel_frame = tk.Frame(grid_frame, bg=row_bg)
            sel_frame.place(x=x, y=cur_y, width=CELL_W_SEL-1, height=ROW_H)
            sel_cb = tk.Checkbutton(sel_frame, variable=self.bet_cham_grid_vars[date_key], 
                                    bg=row_bg, activebackground=row_bg, selectcolor="#1e1e1e",
                                    fg="#f39c12", bd=0, highlightthickness=0,
                                    command=self.render_bet_cham_sidebar) # Optimized: Only refresh sidebar
            sel_cb.pack(expand=True)
            x += CELL_W_SEL

            # Standard Info
            draw_cell(x, cur_y, CELL_W_STT, len(rows) - idx); x += CELL_W_STT
            draw_cell(x, cur_y, CELL_W_DATE, r['date']); x += CELL_W_DATE
            draw_cell(x, cur_y, CELL_W_DB, r['db']); x += CELL_W_DB
            draw_cell(x, cur_y, CELL_W_TAIL, r['tail'], fg="#f1c40f"); x += CELL_W_TAIL
            
            # Digits with individual Bệt highlighting
            chrono_idx = -1
            for k, cr in enumerate(chrono_data):
                if cr['date'] == r['date']:
                    chrono_idx = k
                    break
            
            digits_to_highlight = []
            if chrono_idx > 0:
                p_digits = list(chrono_data[chrono_idx-1]['db_pattern'])
                while len(p_digits) < 6: p_digits.insert(0, "")
                c_digits = r['digits']
                for j in range(6):
                    if c_digits[j] != "" and c_digits[j] == p_digits[j]:
                        digits_to_highlight.append(j)

            for d_idx, digit in enumerate(r['digits']):
                is_high = d_idx in digits_to_highlight
                draw_cell(x, cur_y, CELL_W_DIGIT, digit, highlight=is_high); x += CELL_W_DIGIT
            
            # Bệt column
            draw_cell(x, cur_y, CELL_W_BET, r['bet'], highlight=bool(r['bet']), font=('Segoe UI', 9, 'bold')); x += CELL_W_BET
            
            # Dàn & Sigma
            draw_cell(x, cur_y, CELL_W_DAN, f"Dàn {r['total']} số" if r['total'] else "", bg=row_bg); x += CELL_W_DAN
            draw_cell(x, cur_y, CELL_W_SIGMA, r['total'] if r['total'] else "", bg=row_bg); x += CELL_W_SIGMA
            
            # Tracking Boxes with Zone Styling
            for tr_idx, (tr_date, is_hit, tr_val) in enumerate(r['tracking']):
                n_day = tr_idx + 1 # N1 to N28
                
                # Determine Zone theme (4 zones, 7 days each)
                if 1 <= n_day <= 7:
                    zone_miss_bg = "#154360" # Dark Blue
                    zone_empty_bg = "#0e2d40"
                    zone_label = "KĐ"
                elif 8 <= n_day <= 14:
                    zone_miss_bg = "#6e4506" # Dark Orange/Brown
                    zone_empty_bg = "#4a2e04"
                    zone_label = "B1"
                elif 15 <= n_day <= 21:
                    zone_miss_bg = "#511610" # Dark Red/Maroon
                    zone_empty_bg = "#350e0a"
                    zone_label = "B2"
                else:
                    zone_miss_bg = "#4b1e52" # Dark Purple
                    zone_empty_bg = "#2e1232" # Deep Purple
                    zone_label = "B3"

                if is_hit is True:
                    t_bg, t_fg, t_txt, t_font = "#2ecc71", "white", tr_val, ('Consolas', 11, 'bold')
                elif is_hit is False:
                    t_bg, t_fg, t_txt, t_font = zone_miss_bg, "#f1c40f", "-", ('Consolas', 9, 'bold')
                else: # Placeholder (None, None, "")
                    t_bg, t_fg, t_txt, t_font = zone_empty_bg, "#333333", "", ('Consolas', 7)
                
                t_lbl = tk.Label(grid_frame, text=t_txt, bg=t_bg, fg=t_fg, font=t_font, relief='flat')
                t_lbl.place(x=x, y=cur_y, width=CELL_W_TRK-2, height=ROW_H-2)
                
                # Subtle border for zones
                if n_day in [1, 8, 15, 22]:
                   tk.Frame(grid_frame, bg="gray").place(x=x-1, y=cur_y, width=1, height=ROW_H)

                x += CELL_W_TRK

            # Store last processed rows for sidebar re-rendering
        self.bet_cham_last_rows = rows
        
        # Initial sidebar render
        self.render_bet_cham_sidebar()

        # Update Canvas Scrollregion
        grid_frame.update_idletasks()
        total_w = cur_x
        total_h = (len(rows) + 2) * (ROW_H + 1) + 20
        grid_frame.config(width=total_w, height=total_h)
        self.bet_cham_canvas.config(scrollregion=(0, 0, total_w, total_h))

        # Bottom Summary
        rate = (total_hits / total_checks * 100) if total_checks > 0 else 0
        summary_text = f"📊 Tổng lượt nuôi (28n): {total_nuoi_stats} | Kiểm tra: {total_checks} | Trúng: {total_hits} | Tỷ nổ: {rate:.1f}%"
        self.bet_cham_summary_label.config(text=summary_text)

    def render_bet_cham_sidebar(self):
        """Update ONLY the sidebar content without re-rendering the entire grid."""
        if not hasattr(self, 'bet_cham_last_rows'): return
        rows = self.bet_cham_last_rows

        # --- RE-FILTER NURTURE LISTS BASED ON CURRENT SELECTIONS ---
        self.bet_cham_nurture_auto = []
        self.bet_cham_nurture_manual = []
        
        for r in rows:
            if r['dan_list']:
                hit_count = r['hit_count_capped']
                date_key = r['date']
                
                # Check for manual selection in grid
                is_manually_selected = self.bet_cham_grid_vars.get(date_key) and self.bet_cham_grid_vars[date_key].get()
                
                if is_manually_selected:
                    self.bet_cham_nurture_manual.append((date_key, r['dan_list'], r['bet'], r['total'], r['dan']))
                
                if hit_count == 0 and r.get('is_within_stats'):
                    self.bet_cham_nurture_auto.append((date_key, r['dan_list'], r['bet'], r['total'], r['dan']))

        # --- RENDER SIDEBAR CONTENT ---
        self.bet_cham_stats_text.delete('1.0', tk.END)
        
        # 1. DÀN CHƯA RA
        self.bet_cham_stats_text.insert(tk.END, f"📊 DÀN CHƯA RA ({len(self.bet_cham_nurture_auto)}):\n", 'section_header')
        for d_key, d_list, b_val, t_val, d_str in self.bet_cham_nurture_auto:
            if d_key not in self.bet_cham_vars:
                self.bet_cham_vars[d_key] = tk.BooleanVar(value=True)
            
            cb = tk.Checkbutton(
                self.bet_cham_stats_text, variable=self.bet_cham_vars[d_key],
                bg="#1e1e1e", activebackground="#1e1e1e", 
                selectcolor="#1e1e1e", fg="#f39c12",
                command=self.update_bet_cham_sidebar_summary
            )
            self.bet_cham_stats_text.window_create(tk.END, window=cb)
            b_info = f" [Bệt: {b_val}]" if b_val else ""
            self.bet_cham_stats_text.insert(tk.END, f" {d_key}{b_info} ({t_val}s):\n", 'date_header')
            
            # --- Convergence Logic for DÀN CHƯA RA ---
            tracking_day = 0
            for r in rows:
                if r['date'] == d_key:
                    for tr_idx, (tr_date, is_hit, tr_val) in enumerate(r['tracking']):
                        if tr_date is not None: tracking_day = tr_idx + 1
                        else: break
                    break
            
            if 7 <= tracking_day <= 28:
                convergent_nums = self.cross_check_with_matrix(d_list)
                if convergent_nums:
                    self.bet_cham_stats_text.insert(tk.END, "      ")
                    for i, num in enumerate(d_list):
                        tag = 'hit_info' if num in convergent_nums else 'dan_info'
                        self.bet_cham_stats_text.insert(tk.END, num, tag)
                        if i < len(d_list) - 1: self.bet_cham_stats_text.insert(tk.END, ", ", 'dan_info')
                    self.bet_cham_stats_text.insert(tk.END, " ✨\n\n", 'dan_info')
                    continue 

            self.bet_cham_stats_text.insert(tk.END, f"      {d_str}\n\n", 'dan_info')

        # 2. DÀN CHỌN THỦ CÔNG
        self.bet_cham_stats_text.insert(tk.END, f"📊 DÀN CHỌN THỦ CÔNG ({len(self.bet_cham_nurture_manual)}):\n", 'section_header')
        for idx, (d_key, d_list, b_val, t_val, d_str) in enumerate(self.bet_cham_nurture_manual):
            # We use a separate var key for manual in sidebar if needed, 
            # but usually manual means we DEFINITELY want to include it in summary if it's checked in grid.
            # However, for consistency with Matrix, let's give it a sidebar checkbox too.
            sidebar_key = f"man_{d_key}"
            if sidebar_key not in self.bet_cham_vars:
                # Default to true since user checked it in grid
                self.bet_cham_vars[sidebar_key] = tk.BooleanVar(value=True)
            
            cb = tk.Checkbutton(
                self.bet_cham_stats_text, variable=self.bet_cham_vars[sidebar_key],
                bg="#1e1e1e", activebackground="#1e1e1e", 
                selectcolor="#1e1e1e", fg="#f39c12",
                command=self.update_bet_cham_sidebar_summary
            )
            self.bet_cham_stats_text.window_create(tk.END, window=cb)
            b_info = f" [Bệt: {b_val}]" if b_val else ""
            self.bet_cham_stats_text.insert(tk.END, f" M{idx+1}. {d_key}{b_info} ({t_val}s):\n", 'date_header')
            
            # --- Convergence Logic for MANUAL ---
            tracking_day = 0
            for r in rows:
                if r['date'] == d_key:
                    for tr_idx, (tr_date, is_hit, tr_val) in enumerate(r['tracking']):
                        if tr_date is not None: tracking_day = tr_idx + 1
                        else: break
                    break
            
            if 7 <= tracking_day <= 28:
                convergent_nums = self.cross_check_with_matrix(d_list)
                if convergent_nums:
                    self.bet_cham_stats_text.insert(tk.END, "      ")
                    for i, num in enumerate(d_list):
                        tag = 'hit_info' if num in convergent_nums else 'dan_info'
                        self.bet_cham_stats_text.insert(tk.END, num, tag)
                        if i < len(d_list) - 1: self.bet_cham_stats_text.insert(tk.END, ", ", 'dan_info')
                    self.bet_cham_stats_text.insert(tk.END, " ✨\n\n", 'dan_info')
                    continue

            self.bet_cham_stats_text.insert(tk.END, f"      {d_str}\n\n", 'dan_info')

        # Final Summary calculation
        self.update_bet_cham_sidebar_summary()

        # Update Canvas Scrollregion
        self.bet_cham_grid_frame.update_idletasks()
        # Bottom Summary (Calculated in render_bet_cham_analysis but can be refreshed here if needed)
        # We'll just rely on the summary text already being set or update it if we have stats.

    # ================== KYBE - GROK METHODS ==================
    def _is_straight_mod10_anyorder(self, d: list[int]) -> bool:
        if len(d) != 5: return False
        S = set(d)
        if len(S) != 5: return False
        for b in range(10):
            if S == {(b+i) % 10 for i in range(5)}:
                return True
        return False

    def _classify_xito_anypos(self, d: list[int]) -> str:
        if len(d) != 5: return "—"
        cnt = Counter(d)
        counts = sorted(cnt.values(), reverse=True)
        if counts[0] == 5: return "N"
        if counts[0] == 4: return "T"
        if counts[0] == 3 and (len(counts) >= 2 and counts[1] == 2): return "C"
        if self._is_straight_mod10_anyorder(d): return "S"
        if counts[0] == 3: return "3"
        if counts[0] == 2 and (len(counts) >= 2 and counts[1] == 2): return "2"
        if counts[0] == 2: return "1"
        return "R"

    def _classify_ngau(self, d: list[int]) -> str:
        import itertools
        if len(d) != 5: return "—"
        if len(set(d)) == 1:
            return "H" if d[0] == 0 else "K"
        best = -1; found = False
        for a,b,c in itertools.combinations(range(5),3):
            if (d[a]+d[b]+d[c]) % 10 != 0:
                continue
            found = True
            rem = [x for x in range(5) if x not in (a,b,c)]
            i, j = rem[0], rem[1]
            score = (d[i] + d[j]) % 10
            if score == 0: return "H"
            if score > best: best = score
        if not found: return "K"
        return str(best)



    def create_kybe_view(self):
        """Create the UI for the Kybe - Grok tab."""
        if not hasattr(self, 'kybe_frame'): return
        
        main_paned = ttk.PanedWindow(self.kybe_frame, orient=tk.HORIZONTAL)
        main_paned.pack(fill=tk.BOTH, expand=True)

        # --- LEFT SIDE: GRID & MATRIX ---
        left_frame = ttk.Frame(main_paned, style='TFrame')
        main_paned.add(left_frame, weight=3)
        
        # Header Controls
        ctrl_frame = ttk.Frame(left_frame, style='TFrame')
        ctrl_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(ctrl_frame, text="Nguồn:").pack(side=tk.LEFT, padx=5)
        self.kybe_src_var = tk.StringVar(value="XSMB (ĐB)")
        src_cbo = ttk.Combobox(ctrl_frame, textvariable=self.kybe_src_var, width=15, values=["XSMB (ĐB)", "Điện Toán", "Thần Tài"])
        src_cbo.pack(side=tk.LEFT, padx=5)
        src_cbo.bind("<<ComboboxSelected>>", lambda e: self.render_kybe_analysis())

        ttk.Label(ctrl_frame, text="Hiển thị (N):").pack(side=tk.LEFT, padx=5)
        self.kybe_period_var = tk.IntVar(value=100)
        p_cbo = ttk.Combobox(ctrl_frame, textvariable=self.kybe_period_var, width=5, values=[30, 50, 100, 200, 500])
        p_cbo.pack(side=tk.LEFT, padx=5)
        p_cbo.bind("<<ComboboxSelected>>", lambda e: self.render_kybe_analysis())

        ttk.Label(ctrl_frame, text="Pool Top-N:").pack(side=tk.LEFT, padx=5)
        self.kybe_pool_n_var = tk.IntVar(value=5)
        pool_cbo = ttk.Combobox(ctrl_frame, textvariable=self.kybe_pool_n_var, width=3, values=[3, 4, 5, 8, 10])
        pool_cbo.pack(side=tk.LEFT, padx=5)
        pool_cbo.bind("<<ComboboxSelected>>", lambda e: self.render_kybe_analysis())

        ttk.Label(ctrl_frame, text="Top N:").pack(side=tk.LEFT, padx=5)
        self.kybe_top_n_var = tk.IntVar(value=4)
        top_n_cbo = ttk.Combobox(ctrl_frame, textvariable=self.kybe_top_n_var, width=3, values=[3, 4, 5, 6])
        top_n_cbo.pack(side=tk.LEFT, padx=5)
        top_n_cbo.bind("<<ComboboxSelected>>", lambda e: self.render_kybe_analysis())

        self.kybe_only_cyc_var = tk.BooleanVar(value=False)
        chk_cyc = ttk.Checkbutton(ctrl_frame, text="Chỉ có chu kỳ", variable=self.kybe_only_cyc_var, command=self.render_kybe_analysis)
        chk_cyc.pack(side=tk.LEFT, padx=10)


        # Grid Content Area
        self.kybe_canvas_frame = ttk.Frame(left_frame, style='TFrame')
        self.kybe_canvas_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Scrollable Canvas for Grid
        self.kybe_canvas = tk.Canvas(self.kybe_canvas_frame, bg="#0a0f1a", highlightthickness=0)
        self.kybe_v_scroll = ttk.Scrollbar(self.kybe_canvas_frame, orient=tk.VERTICAL, command=self.kybe_canvas.yview)
        self.kybe_h_scroll = ttk.Scrollbar(self.kybe_canvas_frame, orient=tk.HORIZONTAL, command=self.kybe_canvas.xview)
        self.kybe_canvas.configure(yscrollcommand=self.kybe_v_scroll.set, xscrollcommand=self.kybe_h_scroll.set)
        
        self.kybe_v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.kybe_h_scroll.pack(side=tk.BOTTOM, fill=tk.X)
        self.kybe_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        self.kybe_grid_frame = tk.Frame(self.kybe_canvas, bg="#0a0f1a")
        self.kybe_canvas.create_window((0, 0), window=self.kybe_grid_frame, anchor="nw")

        # Cycle 3-4s Dashboard Display Area
        stats_group = tk.LabelFrame(left_frame, text="📊 Chu kỳ 3-4s & Thống kê nâng cao", bg="#1e293b", fg="#93c5fd", font=('Segoe UI', 10, 'bold'))
        stats_group.pack(fill=tk.BOTH, expand=False, padx=5, pady=5, side=tk.BOTTOM)
        
        self.kybe_stats_text = tk.Text(stats_group, height=15, bg="#0f172a", fg="#cbd5e1", font=('Consolas', 9), bd=0, wrap=tk.NONE)
        sv = ttk.Scrollbar(stats_group, orient=tk.VERTICAL, command=self.kybe_stats_text.yview)
        sh = ttk.Scrollbar(stats_group, orient=tk.HORIZONTAL, command=self.kybe_stats_text.xview)
        self.kybe_stats_text.configure(yscrollcommand=sv.set, xscrollcommand=sh.set)
        sv.pack(side=tk.RIGHT, fill=tk.Y)
        sh.pack(side=tk.BOTTOM, fill=tk.X)
        self.kybe_stats_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Configure tags for colors and styles
        self.kybe_stats_text.tag_configure('header', font=('Segoe UI', 10, 'bold'), foreground="#facc15")
        self.kybe_stats_text.tag_configure('taixiu', foreground="#ec4899", font=('Consolas', 10, 'bold'))
        self.kybe_stats_text.tag_configure('ok', foreground="#22c55e")
        self.kybe_stats_text.tag_configure('warn', foreground="#ef4444")
        self.kybe_stats_text.tag_configure('muted', foreground="#9ca3af")
        self.kybe_stats_text.tag_configure('pill', background="#374151", foreground="white")

        # --- RIGHT SIDE: SIDEBAR (Simulations & Touch Pattern) ---
        right_scroll_frame = ttk.Frame(main_paned, style='TFrame')
        main_paned.add(right_scroll_frame, weight=2)
        
        sidebar_canvas = tk.Canvas(right_scroll_frame, bg="#1e1e1e", highlightthickness=0)
        sidebar_v_scroll = ttk.Scrollbar(right_scroll_frame, orient=tk.VERTICAL, command=sidebar_canvas.yview)
        sidebar_canvas.configure(yscrollcommand=sidebar_v_scroll.set)
        
        sidebar_v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        sidebar_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        self.kybe_sidebar_inner = tk.Frame(sidebar_canvas, bg="#1e1e1e")
        sidebar_canvas.create_window((0, 0), window=self.kybe_sidebar_inner, anchor="nw")
        
        def update_sidebar_scroll(event):
            sidebar_canvas.configure(scrollregion=sidebar_canvas.bbox("all"))
        self.kybe_sidebar_inner.bind("<Configure>", update_sidebar_scroll)

        # 2. Touch Pattern Module
        touch_group = tk.LabelFrame(self.kybe_sidebar_inner, text="🔢 Touch Pattern Module", bg="#1e1e1e", fg="#93c5fd", font=('Segoe UI', 10, 'bold'))
        touch_group.pack(fill=tk.X, padx=5, pady=5)
        
        # Inputs for Ngầu and Tổng
        t_input_frame = tk.Frame(touch_group, bg="#1e1e1e")
        t_input_frame.pack(fill=tk.X, padx=5, pady=5)
        
        tk.Label(t_input_frame, text="Ngầu:", bg="#1e1e1e", fg="white").grid(row=0, column=0, sticky="w")
        self.kybe_ngau_input = tk.Entry(t_input_frame, bg="#0f172a", fg="#ec4899", insertbackground="white")
        self.kybe_ngau_input.grid(row=0, column=1, sticky="ew", padx=5)
        self.kybe_ngau_input.bind("<KeyRelease>", lambda e: self.update_kybe_touch_summary())

        tk.Label(t_input_frame, text="Tổng:", bg="#1e1e1e", fg="white").grid(row=1, column=0, sticky="w")
        self.kybe_tong_input = tk.Entry(t_input_frame, bg="#0f172a", fg="#3b82f6", insertbackground="white")
        self.kybe_tong_input.grid(row=1, column=1, sticky="ew", padx=5)
        self.kybe_tong_input.bind("<KeyRelease>", lambda e: self.update_kybe_touch_summary())
        
        t_input_frame.columnconfigure(1, weight=1)

        # Mode Selection
        m_frame = tk.Frame(touch_group, bg="#1e1e1e")
        m_frame.pack(fill=tk.X, padx=5)
        tk.Label(m_frame, text="Chế độ:", bg="#1e1e1e", fg="white").pack(side=tk.LEFT)
        self.kybe_touch_mode = tk.StringVar(value="Chạm Tổng")
        tm_cbo = ttk.Combobox(m_frame, textvariable=self.kybe_touch_mode, width=12, values=["Chạm", "Tổng", "Chạm Tổng"])
        tm_cbo.pack(side=tk.LEFT, padx=5)
        tm_cbo.bind("<<ComboboxSelected>>", lambda e: self.update_kybe_touch_summary())
        
        # Mức Số results
        self.kybe_touch_results = tk.Text(touch_group, height=15, bg="#0f172a", fg="#cbd5e1", font=('Consolas', 10), bd=0)
        self.kybe_touch_results.pack(fill=tk.X, padx=5, pady=5)

    # --- KYBE STATISTICAL METHODS (PORTED FROM GROK) ---
    def _get_bacnho_preds(self, data_slice, latest_digits):
        """Dự đoán Bạc Nhớ (Chữ số đơn) dựa trên bộ 2 và 3."""
        pred_counter = Counter()
        latest_digits_str = [str(d) for d in latest_digits]
        for comb_size in (2, 3):
            combs = list(itertools.combinations(latest_digits_str, comb_size))
            for c in combs:
                c_set = set(c)
                for j in range(1, len(data_slice)):
                    try:
                        h_digits = set(str(data_slice[j][i]) for i in range(5)) # data_slice is already seq-style or needs unpack?
                        # NOTE: Grok search_data[j][i+2] assumes raw rows. 
                        # If data_slice is seqs[pos][idx], need to handle differently.
                        # I'll assume data_slice is a list of [c, ng, tr, ch, dv] lists.
                        if c_set.issubset(h_digits):
                            next_res = [str(data_slice[j-1][i]) for i in range(5)]
                            for d in next_res: pred_counter[d] += 1
                    except: continue
        top5 = sorted(pred_counter.items(), key=lambda x: (-x[1], x[0]))[:5]
        return [d for d, _ in top5], pred_counter

    def _get_bacnho_comb_preds(self, data_slice, current_digits, size=2, n=1):
        """Tìm bạc nhớ cho tổ hợp."""
        current_set = set(str(d) for d in current_digits)
        combs = list(itertools.combinations(sorted(list(current_set)), size))
        
        future_counts = Counter()
        for c in combs:
            c_set = set(c)
            for j in range(1, len(data_slice)):
                try:
                    h_digits = set(str(data_slice[j][i]) for i in range(5))
                    if c_set.issubset(h_digits):
                        next_res = [str(data_slice[j-1][i]) for i in range(5)]
                        for next_c in itertools.combinations(sorted(next_res), size):
                            future_counts[next_c] += 1
                except: continue
        
        top = sorted(future_counts.items(), key=lambda x: (-x[1], x[0]))
        return [list(x[0]) for x in top[:n]]

    def _compute_cycles(self, digsets, dates, combos, L_total):
        """Tính toán chu kỳ cho các bộ số."""
        out = []
        for tup in combos:
            idxs = [i for i, S in enumerate(digsets) if all(d in S for d in tup)]
            if not idxs: continue
            idxs.sort()
            gaps = [idxs[i + 1] - idxs[i] for i in range(len(idxs) - 1)]
            cyc, support = (None, 0)
            if gaps:
                c = Counter(gaps)
                cyc, support = max(c.items(), key=lambda kv: (kv[1], -kv[0]))
            
            last_idx = idxs[-1]
            last_date = dates[last_idx] if 0 <= last_idx < len(dates) else ""
            miss = L_total - 1 - last_idx
            due = None if not cyc else (cyc - (miss % cyc)) % cyc
            gan_max = (max(gaps) if gaps else None)
            
            # Simplified ky_list (just last 3 digits of index or similar)
            ky_list = [str(i).zfill(3) for i in idxs[::-1][:5]] 
            
            out.append({
                "tok": "".join(map(str, sorted(tup))),
                "cyc": cyc, "support": support, "miss": miss,
                "due": due, "gan_max": gan_max, "occ": len(idxs),
                "ky_list": ky_list
            })
        return out

    def _calculate_taixiu_stats(self, seqs):
        """Tính toán thống kê Tài/Xỉu và Rồng/Hổ."""
        L = len(seqs[0])
        rh_tokens, tx_tokens = [], []
        counts = Counter()
        for i in range(L):
            c0, c4 = seqs[0][i], seqs[4][i]
            rh = "R" if c0 > c4 else ("H" if c0 < c4 else "=")
            
            total5 = sum(seqs[p][i] for p in range(5))
            tx = "T" if total5 >= 23 else "X"
            
            rh_tokens.append(rh)
            tx_tokens.append(tx)
            counts[tx] += 1
            
        # Detect signals (simplified)
        # Rồng & Tài -> Vào X
        # Hổ & Xỉu -> Vào T
        # (This logic is more involved in the original, I'll port basic versions)
        return rh_tokens, tx_tokens, counts

    def update_kybe_touch_summary(self):
        """Logic for Mức 0, 1, 2 based on selected Chạm/Tổng."""
        ngau_str = self.kybe_ngau_input.get()
        tong_str = self.kybe_tong_input.get()
        mode = self.kybe_touch_mode.get()
        
        digits_ngau = set("".join(filter(str.isdigit, ngau_str)))
        digits_tong = set("".join(filter(str.isdigit, tong_str)))
        
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
                else:
                    if match_cham or match_tong: res.add(s)
            return res

        set_a = get_set(digits_ngau)
        set_b = get_set(digits_tong)
        
        muc2, muc1, muc0 = [], [], []
        for i in range(100):
            s = f"{i:02d}"
            in_a = s in set_a
            in_b = s in set_b
            if in_a and in_b: muc2.append(s)
            elif in_a or in_b: muc1.append(s)
            else: muc0.append(s)
        
        res_text = f"📊 Dàn Ngầu ({len(set_a)}): " + ",".join(sorted(list(set_a))) + "\n"
        res_text += f"📊 Dàn Tổng ({len(set_b)}): " + ",".join(sorted(list(set_b))) + "\n\n"
        res_text += f"💎 Mức 2 ({len(muc2)}):\n" + ",".join(muc2) + "\n\n"
        res_text += f"💎 Mức 1 ({len(muc1)}):\n" + ",".join(muc1) + "\n\n"
        res_text += f"💎 Mức 0 ({len(muc0)}):\n" + ",".join(muc0)
        
        self.kybe_touch_results.config(state='normal')
        self.kybe_touch_results.delete('1.0', tk.END)
        self.kybe_touch_results.insert(tk.END, res_text)
        self.kybe_touch_results.config(state='disabled')

    def refresh_kybe_highlights(self):
        """Update just the colors and borders of the Kybe grid without redrawing widgets."""
        if not hasattr(self, 'kybe_grid_widgets') or not self.kybe_grid_widgets: return
        if not hasattr(self, 'kybe_current_data'): return
        
        data = self.kybe_current_data
        seqs = data['seqs']
        ng_tokens = data['ng_tokens']
        sum3_tokens = data['sum3_tokens']
        sum5_tokens = data['sum5_tokens']
        xi_tokens = data['xi_tokens']
        L = len(seqs[0])
        display_L = data['display_L']
        
        # Recalculate Ngầu Absent
        ngau_absent = []
        for idx, token in enumerate(ng_tokens):
            if not str(token).isdigit() or idx - 4 < 0: continue
            token_val = int(token)
            is_absent = True
            for offset in range(1, 5):
                prev_idx = idx - offset
                found_in_any = False
                for col in range(5):
                    if 0 <= prev_idx < L and seqs[col][prev_idx] == token_val:
                        found_in_any = True; break
                if found_in_any: is_absent = False; break
            if is_absent: ngau_absent.append(idx)

        hl_ngau = self.kybe_highlight_ngau
        hl_sum = self.kybe_highlight_sum
        hl_digits = self.kybe_highlight_digits
        hl_tram = getattr(self, 'kybe_highlight_tram', [])
        touch_mode = self.kybe_touch_mode.get()
        match_str = (self.kybe_tong_input.get() + "," + self.kybe_ngau_input.get()).replace(" ", "")
        match_items = set(match_str.split(","))

        # Update Digit Rows (0-4)
        for p in range(5):
            for i in range(display_L):
                widget_key = (p, i)
                if widget_key not in self.kybe_grid_widgets: continue
                
                widget = self.kybe_grid_widgets[widget_key]
                parent = widget.master
                val = seqs[p][i]
                fg = "white"
                cell_bg = "#0a0f1a"
                indicator_bg = "#1e293b"
                
                # Highlighting Logic
                is_digit_absent = False
                if i - 4 >= 0:
                    digit_is_absent = True
                    for offset in range(1, 5):
                        prev_idx = i - offset
                        found_in_any = False
                        for r_idx in range(5):
                            if seqs[r_idx][prev_idx] == val:
                                found_in_any = True; break
                        if found_in_any: digit_is_absent = False; break
                    is_digit_absent = digit_is_absent

                highlighted_extra = False
                
                # Check if this is a clicked cell (for reverse mode)
                is_clicked_digit = (val, i) in hl_digits and p in [3, 4]
                is_clicked_tram = (val, i) in hl_tram and p == 2
                
                if is_clicked_digit:
                    # Special color for clicked cell (source) in digit reverse mode
                    cell_bg = "#ef4444"; indicator_bg = "#dc2626"; fg = "white"
                    highlighted_extra = True
                elif is_clicked_tram:
                    # Distinct color for clicked cell in Trăm reverse mode (Indigo/Cyan)
                    cell_bg = "#6366f1"; indicator_bg = "#4f46e5"; fg = "white"
                    highlighted_extra = True
                
                if not highlighted_extra:
                    # Trăm reverse highlighting: highlight Ngàn/Chục ngàn to the right
                    if p in [0, 1] and hl_tram:
                        for tv, ti in hl_tram:
                            if i > ti and val == tv:
                                highlighted_extra = True
                                # Indigo/Purple for Trăm target match
                                cell_bg = "#a855f7"; indicator_bg = "#9333ea"; fg = "white"
                                break
                                
                if not highlighted_extra:
                    for s_mode, s_val, s_pos in hl_sum:
                        if i < s_pos:  # Xuôi: Only highlight LEFT (newer periods)
                            match = False
                            chuc, donvi = seqs[3][i], seqs[4][i]
                            v_tong_de = (chuc + donvi) % 10
                            if touch_mode == "Chạm":
                                if val == s_val: match = True
                            elif touch_mode == "Tổng":
                                if v_tong_de == s_val: match = True
                            else: # Chạm Tổng
                                if val == s_val or v_tong_de == s_val: match = True
                            
                            if match:
                                highlighted_extra = True
                                cell_bg = "#facc15" if is_digit_absent else ("#22c55e" if s_mode == 'sum5' else "#3b82f6")
                                indicator_bg = cell_bg
                                break

                if not highlighted_extra:
                    for n_val, n_pos in hl_ngau:
                        if i < n_pos and p in [2, 3, 4]:  # Xuôi: Only highlight LEFT (newer periods)
                            match = False
                            chuc, donvi = seqs[3][i], seqs[4][i]
                            tong = (chuc + donvi) % 10
                            if touch_mode == "Chạm":
                                if val == n_val: match = True
                            elif touch_mode == "Tổng":
                                if tong == n_val: match = True
                            else: # Chạm Tổng
                                if n_val == val or tong == n_val: match = True
                            
                            if match:
                                highlighted_extra = True
                                cell_bg = "#facc15" if is_digit_absent else ("#ec4899" if tong == n_val else "#f97316")
                                indicator_bg = cell_bg
                                break
                
                # Disabled: Touch Pattern Module highlighting (interferes with directional highlighting)
                # if not highlighted_extra:
                #     if str(val) in match_items: 
                #         cell_bg = "#34d399"
                #         indicator_bg = "#059669"
                
                # Premium Border Implementation
                # We use the parent frame as the border, and the widget as the inner cell
                parent.config(bg=indicator_bg)
                widget.config(bg=cell_bg, fg=fg)

        # Update Xì Tố Row (static mostly) - p=5
        for i in range(display_L):
            w = self.kybe_grid_widgets.get((5, i))
            if w: w.config(bg="#0a0f1a", fg="#93c5fd")

        # Update Ngầu Row - p=6
        for i in range(display_L):
            widget = self.kybe_grid_widgets.get((6, i))
            if not widget: continue
            parent = widget.master
            val = ng_tokens[i]
            cell_bg = "#0a0f1a"
            indicator_bg = "#1e293b"
            fg = "#ec4899"
            
            t_val = -1
            try: t_val = int(val)
            except: pass
            
            is_reverse_hl_active = len(hl_digits) > 0
            is_clicked_cell = (t_val, i) in hl_ngau or (val, i) in hl_ngau
            
            if is_clicked_cell and not is_reverse_hl_active:
                # Special color for clicked cell (source)
                cell_bg = "#ef4444"
                indicator_bg = "#dc2626"
                fg = "white"
            elif not is_reverse_hl_active and is_clicked_cell:
                cell_bg = "#ec4899"
                indicator_bg = "#db2777"
                fg = "white"
            else:
                digit_match = False
                for dv, di in hl_digits:
                    if i > di:  # Ngược: Only highlight RIGHT (older periods)
                        if str(dv) == str(val) or (str(dv) == "0" and str(val) == "K"):
                            digit_match = True; break
                if digit_match:
                    cell_bg = "#ec4899"
                    indicator_bg = "#db2777"
                    fg = "white"
                elif i in ngau_absent:
                    fg = "#facc15"
            
            parent.config(bg=indicator_bg)
            widget.config(bg=cell_bg, fg=fg)

        # Update Tổng 3 Row - p=7
        for i in range(display_L):
            widget = self.kybe_grid_widgets.get((7, i))
            if not widget: continue
            parent = widget.master
            val = sum3_tokens[i]
            cell_bg = "#0a0f1a"
            indicator_bg = "#1e293b"
            fg = "#a855f7"
            
            is_reverse_hl_active = len(hl_digits) > 0
            is_clicked_cell = ('sum3', int(val), i) in hl_sum
            
            if is_clicked_cell and not is_reverse_hl_active:
                # Special color for clicked cell (source)
                cell_bg = "#ef4444"; indicator_bg = "#dc2626"; fg = "white"
            elif (not is_reverse_hl_active) and is_clicked_cell:
                cell_bg = "#3b82f6"; indicator_bg = "#2563eb"; fg = "white"
            else:
                digit_match = False
                for dv, di in hl_digits:
                    if i > di and int(dv) == int(val):  # Ngược: Only highlight RIGHT (older periods)
                        digit_match = True; break
                if digit_match:
                    cell_bg = "#3b82f6"; indicator_bg = "#2563eb"; fg = "white"
            
            parent.config(bg=indicator_bg)
            widget.config(bg=cell_bg, fg=fg)

        # Update Tổng 5 Row - p=8
        for i in range(display_L):
            widget = self.kybe_grid_widgets.get((8, i))
            if not widget: continue
            parent = widget.master
            val = sum5_tokens[i]
            cell_bg = "#0a0f1a"
            indicator_bg = "#1e293b"
            fg = "#f97316"
            
            is_reverse_hl_active = len(hl_digits) > 0
            is_clicked_cell = ('sum5', int(val), i) in hl_sum
            
            if is_clicked_cell and not is_reverse_hl_active:
                # Special color for clicked cell (source)
                cell_bg = "#ef4444"; indicator_bg = "#dc2626"; fg = "white"
            elif (not is_reverse_hl_active) and is_clicked_cell:
                cell_bg = "#22c55e"; indicator_bg = "#16a34a"; fg = "white"
            else:
                digit_match = False
                for dv, di in hl_digits:
                    if i > di and int(dv) == int(val):  # Ngược: Only highlight RIGHT (older periods)
                        digit_match = True; break
                if digit_match:
                    cell_bg = "#22c55e"; indicator_bg = "#16a34a"; fg = "white"
            
            parent.config(bg=indicator_bg)
            widget.config(bg=cell_bg, fg=fg)

    def render_kybe_analysis(self):
        """Perform data processing and render grid (reusing widgets if possible)."""
        if not hasattr(self, 'kybe_grid_frame'): return
        
        region = self.region_var.get()
        src = self.kybe_src_var.get()
        data_source = self.master_data if region == "Miền Bắc" else self.station_data
        if not data_source: return
        
        backtest_offset = self.backtest_var.get()
        n_days = self.kybe_period_var.get()
        working_data = data_source[backtest_offset : backtest_offset + n_days]
        if not working_data: return

        seqs = [[] for _ in range(5)]
        dates = []
        for d in working_data:
            val = ""
            if region == "Miền Bắc":
                if src == "Điện Toán": val = "".join(d.get('dt_numbers', []))
                elif src == "Thần Tài": val = d.get('tt_number', '')
                else: val = d.get('xsmb_full', '')
            else:
                val = d.get('db', '').replace(',', '').strip()
            
            if len(val) >= 5:
                digits = [int(c) for c in val[-5:]]
                for i in range(5): seqs[i].append(digits[i])
                dates.append(d.get('date', ''))
        
        if not seqs[0]: return
        self.kybe_last_rows = seqs 
        
        L = len(seqs[0])
        max_cols = 40 
        display_L = min(L, max_cols)
        
        xi_tokens = [self._classify_xito_anypos([seqs[p][i] for p in range(5)]) for i in range(L)]
        ng_tokens = [self._classify_ngau([seqs[p][i] for p in range(5)]) for i in range(L)]
        sum3_tokens = [str((seqs[2][i] + seqs[3][i] + seqs[4][i]) % 10) for i in range(L)]
        sum5_tokens = [str(sum(seqs[p][i] for p in range(5)) % 10) for i in range(L)]

        # Store Current Data for Highlight Refresh
        self.kybe_current_data = {
            'seqs': seqs, 'dates': dates, 'xi_tokens': xi_tokens, 
            'ng_tokens': ng_tokens, 'sum3_tokens': sum3_tokens, 
            'sum5_tokens': sum5_tokens, 'display_L': display_L
        }

        # Render Dashboard Text (Already fast, keep as is)
        self.kybe_stats_text.config(state='normal')
        self.kybe_stats_text.delete('1.0', tk.END)
        
        # 1. TÀI XỈU / RỒNG HỔ
        rh_toks, tx_toks, tx_counts = self._calculate_taixiu_stats(seqs)
        L_tx = min(L, 20)
        
        self.kybe_stats_text.insert(tk.END, "📊 TÀI / XỈU · RỒNG / HỔ (Gộp)\n", 'header')
        latest_tx = tx_toks[0] if tx_toks else "—"
        latest_rh = rh_toks[0] if rh_toks else "—"
        self.kybe_stats_text.insert(tk.END, f"Hiện tại: RH={latest_rh} · TX={latest_tx}\n")
        
        # Detect signals (RT -> X, HX -> T)
        sig_lines = []
        for i in range(L - 3):
            if rh_toks[i] == "R" and tx_toks[i] == "T":
                so_ky = dates[i]
                sig_lines.append(f"Tín hiệu Rồng & Tài -> Vào X (Kỳ {so_ky})")
                break
        for i in range(L - 3):
            if rh_toks[i] == "H" and tx_toks[i] == "X":
                so_ky = dates[i]
                sig_lines.append(f"Tín hiệu Hổ & Xỉu -> Vào T (Kỳ {so_ky})")
                break
        if sig_lines:
            for sl in sig_lines: self.kybe_stats_text.insert(tk.END, sl + "\n", 'warn')
        else:
            self.kybe_stats_text.insert(tk.END, "Chưa có tín hiệu Tài/Xỉu\n", 'muted')
            
        self.kybe_stats_text.insert(tk.END, "Ngầu:   " + " ".join(map(str, ng_tokens[:L_tx])) + "\n")
        self.kybe_stats_text.insert(tk.END, "R/H:    " + " ".join(rh_toks[:L_tx]) + "\n")
        self.kybe_stats_text.insert(tk.END, "Tài/Xỉu:" + " ".join(tx_toks[:L_tx]) + "\n")
        self.kybe_stats_text.insert(tk.END, f"Tổng Tài: {tx_counts['T']} · Tổng Xỉu: {tx_counts['X']}\n\n")

        # 2. NHỊ HỢP & GIAO NHAU (Frequency Matrix)
        self.kybe_stats_text.insert(tk.END, "📊 Nhị hợp & Giao nhau\n", 'header')
        
        pool_n = self.kybe_pool_n_var.get()
        top_n = self.kybe_top_n_var.get()
        
        # Prepare counters for Current, Prev 1, 2, 3
        def get_cnt(offset):
            if offset + 40 > L: return Counter()
            # Find frequency of digits in columns following combinations of last5 digits
            try:
                slice_seqs = [seqs[p][offset:offset+40] for p in range(5)]
                last5 = [slice_seqs[p][0] for p in range(5)]
                combos = list(itertools.combinations(last5, 3)) + list(itertools.combinations(last5, 4))
                digsets = [set(slice_seqs[p][j] for p in range(5)) for j in range(len(slice_seqs[0]))]
                cnt = Counter()
                for tup in combos:
                    idxs = [j for j, S in enumerate(digsets) if all(d in S for d in tup)]
                    for j in idxs:
                        if j > 0:
                            for p in range(5): cnt[str(slice_seqs[p][j-1])] += 1
                return cnt
            except: return Counter()

        c_cur = get_cnt(0)
        c_p1 = get_cnt(1)
        c_p2 = get_cnt(2)
        c_p3 = get_cnt(3)
        
        # Get Top-N for each
        def get_top(cnt):
            return [d for d, _ in sorted(cnt.items(), key=lambda x: (-x[1], x[0]))[:top_n]]
        
        t_cur = get_top(c_cur)
        t_p1 = get_top(c_p1)
        t_p2 = get_top(c_p2)
        t_p3 = get_top(c_p3)
        
        def get_common(list1, list2, label):
            common = [x for x in list1 if x in list2]
            if common:
                # Helper to generate filtered pairs
                def generate_dan(digits, common_digits):
                    pairs = list(itertools.permutations(digits, 2))
                    filtered = []
                    for p in pairs:
                        if p[0] in common_digits or p[1] in common_digits:
                            filtered.append("".join(p))
                    return filtered

                # 1. Dàn Current/Source
                dan1_list = generate_dan(list1, common)
                
                # 2. Dàn Comparison/Next Row (Lùi X)
                dan2_list = generate_dan(list2, common)
                
                # 3. Dàn Chung (Intersection)
                dan_chung_list = sorted(list(set(dan1_list) & set(dan2_list)))
                
                dan1_str = ",".join(dan1_list)
                dan2_str = ",".join(dan2_list)
                dan_chung_str = ",".join(dan_chung_list)
                
                return (f" > cùng có {label} số {','.join(common)} "
                        f"| Dàn: {dan1_str} "
                        f"| Dàn {label}: {dan2_str} "
                        f"| Dàn chung: {dan_chung_str}")
            return f" > cùng có {label}: không có"

        common_cur_p1 = get_common(t_cur, t_p1, "Lùi 1")
        common_p1_p2 = get_common(t_p1, t_p2, "Lùi 2")
        common_p2_p3 = get_common(t_p2, t_p3, "Lùi 3")
        
        self.kybe_stats_text.insert(tk.END, f"Hiện tại: {','.join(t_cur)}{common_cur_p1}\n")
        self.kybe_stats_text.insert(tk.END, f"Lùi 1:    {','.join(t_p1)}{common_p1_p2}\n")
        self.kybe_stats_text.insert(tk.END, f"Lùi 2:    {','.join(t_p2)}{common_p2_p3}\n")
        self.kybe_stats_text.insert(tk.END, f"Lùi 3:    {','.join(t_p3)}\n\n")

        # 3. BẠC NHỚ CHI TIẾT
        self.kybe_stats_text.insert(tk.END, "📊 Bạc Nhớ Cặp & Bộ (Lịch sử)\n", 'header')
        # Use 40 days for Bạc Nhớ
        bn_rows = [[seqs[p][i] for p in range(5)] for i in range(min(L, 40))]
        latest_digits = bn_rows[0] if bn_rows else []
        
        if latest_digits:
            p_5t = self._get_bacnho_comb_preds(bn_rows, latest_digits, 2, 3)
            p_ht = self._get_bacnho_comb_preds(bn_rows, [latest_digits[i] for i in [2,3,4]], 2, 3)
            
            self.kybe_stats_text.insert(tk.END, "2 SỐ 5 TINH: " + " | ".join([",".join(map(str, c)) for c in p_5t]) + "\n")
            self.kybe_stats_text.insert(tk.END, "2 SỐ HẬU TỨ: " + " | ".join([",".join(map(str, c)) for c in p_ht]) + "\n\n")

        # 4. CHU KỲ BỘ 3 & BỘ 4
        self.kybe_stats_text.insert(tk.END, "📊 Dự đoán theo chu kỳ (Bộ 3 & 4)\n", 'header')
        
        last5_newest = [seqs[p][0] for p in range(5)]
        combos3 = [t for t in itertools.combinations(last5_newest, 3) if len(set(t)) == 3]
        combos4 = [t for t in itertools.combinations(last5_newest, 4) if len(set(t)) == 4]
        
        digsets = [set(seqs[p][j] for p in range(5)) for j in range(L)]
        
        res3 = self._compute_cycles(digsets, dates, combos3, L)
        res4 = self._compute_cycles(digsets, dates, combos4, L)
        
        # Merge and sort by 'due' ascending
        all_res = res3 + res4
        only_cyc = self.kybe_only_cyc_var.get()
        if only_cyc:
            all_res = [r for r in all_res if r["cyc"] is not None]
        
        all_res.sort(key=lambda r: (r["due"] if r["due"] is not None else 999, -r["occ"]))
        
        header_table = f"{'Bộ':<10} {'Chu kỳ':<8} {'Gan':<5} {'Hạn':<5} {'Occ':<5} {'Lịch sử'}\n"
        self.kybe_stats_text.insert(tk.END, header_table, 'muted')
        for r in all_res[:10]: # Top 10
            line = f"{r['tok']:<10} {str(r['cyc']):<8} {str(r['miss']):<5} {str(r['due']):<5} {str(r['occ']):<5} {','.join(r['ky_list'])}\n"
            tag = 'ok' if r['due'] == 0 else ('warn' if r['due'] is not None and r['due'] <= 2 else None)
            self.kybe_stats_text.insert(tk.END, line, tag)
        
        self.kybe_stats_text.config(state='disabled')

        # GRID REBUILD LOGIC (Only if necessary)
        # Check if we need to rebuild: display_L changed OR data changed
        rebuild_needed = True
        if hasattr(self, 'kybe_grid_id'):
            # Simple check: comparing concatenation of first 5 dates
            current_id = "".join(dates[:5]) + str(display_L)
            if self.kybe_grid_id == current_id:
                rebuild_needed = False
        
        if rebuild_needed:
            for widget in self.kybe_grid_frame.winfo_children(): widget.destroy()
            self.kybe_grid_widgets = {}
            self.kybe_grid_id = "".join(dates[:5]) + str(display_L)
            
            pos_names = ["C.ngàn", "Ngàn", "Trăm", "Chục", "Đơn vị", "Xì Tố", "Ngầu", "Tổng 3", "Tổng 5"]
            for idx, name in enumerate(pos_names):
                tk.Label(self.kybe_grid_frame, text=name, bg="#1e293b", fg="#facc15", 
                         font=('Segoe UI', 9, 'bold'), width=8, relief='flat').grid(row=idx, column=0, padx=2, pady=1, sticky='nsew')

            for p in range(5):
                for i in range(display_L):
                    val = seqs[p][i]
                    cell_frame = tk.Frame(self.kybe_grid_frame, bg="#1e293b", bd=0, highlightthickness=1, highlightbackground="#334155")
                    cell_frame.grid(row=p, column=i+1, padx=1, pady=1, sticky='nsew')
                    if p in [2, 3, 4]:  # Trăm, Chục, Đơn vị are clickable
                        if p == 2:
                            # Trăm: highlights Ngàn/Chục ngàn
                            btn = tk.Button(cell_frame, text=str(val), font=('Consolas', 10, 'bold'), bd=0, relief='flat', width=2,
                                            command=lambda v=val, idx=i: self._on_kybe_tram_click_idx(v, idx))
                        else:
                            # Chục/Đơn vị: highlights Ngầu/Tổng
                            btn = tk.Button(cell_frame, text=str(val), font=('Consolas', 10, 'bold'), bd=0, relief='flat', width=2,
                                            command=lambda v=val, idx=i: self._on_kybe_digit_click_idx(v, idx))
                        btn.pack(fill='both', expand=True)
                        self.kybe_grid_widgets[(p, i)] = btn
                    else:
                        lbl = tk.Label(cell_frame, text=str(val), font=('Consolas', 10, 'bold'))
                        lbl.pack(fill='both', expand=True)
                        self.kybe_grid_widgets[(p, i)] = lbl

            for i in range(display_L): # Xì Tố
                lbl = tk.Label(self.kybe_grid_frame, text=xi_tokens[i], bg="#0a0f1a", fg="#93c5fd", font=('Consolas', 9, 'bold'))
                lbl.grid(row=5, column=i+1, padx=1, pady=1, sticky='nsew')
                self.kybe_grid_widgets[(5, i)] = lbl

            for i in range(display_L): # Ngầu
                cell_frame = tk.Frame(self.kybe_grid_frame, bg="#1e293b", bd=0, highlightthickness=1, highlightbackground="#334155")
                cell_frame.grid(row=6, column=i+1, padx=1, pady=1, sticky='nsew')
                btn = tk.Button(cell_frame, text=ng_tokens[i], font=('Consolas', 9, 'bold'), width=2, bd=0,
                                command=lambda v=ng_tokens[i], idx=i: self._on_kybe_ngau_click_idx(v, idx))
                btn.pack(fill='both', expand=True); self.kybe_grid_widgets[(6, i)] = btn

            for i in range(display_L): # Tổng 3
                cell_frame = tk.Frame(self.kybe_grid_frame, bg="#1e293b", bd=0, highlightthickness=1, highlightbackground="#334155")
                cell_frame.grid(row=7, column=i+1, padx=1, pady=1, sticky='nsew')
                btn = tk.Button(cell_frame, text=sum3_tokens[i], font=('Consolas', 9, 'bold'), width=2, bd=0,
                                command=lambda v=sum3_tokens[i], idx=i: self._on_kybe_sum_click_idx('sum3', v, idx))
                btn.pack(fill='both', expand=True); self.kybe_grid_widgets[(7, i)] = btn

            for i in range(display_L): # Tổng 5
                cell_frame = tk.Frame(self.kybe_grid_frame, bg="#1e293b", bd=0, highlightthickness=1, highlightbackground="#334155")
                cell_frame.grid(row=8, column=i+1, padx=1, pady=1, sticky='nsew')
                btn = tk.Button(cell_frame, text=sum5_tokens[i], font=('Consolas', 9, 'bold'), width=2, bd=0,
                                command=lambda v=sum5_tokens[i], idx=i: self._on_kybe_sum_click_idx('sum5', v, idx))
                btn.pack(fill='both', expand=True); self.kybe_grid_widgets[(8, i)] = btn

        # FINALLY: Refresh highlights
        self.refresh_kybe_highlights()
        self.kybe_grid_frame.update_idletasks()
        self.kybe_canvas.config(scrollregion=self.kybe_canvas.bbox("all"))


    def _on_kybe_digit_click_idx(self, val, idx):
        """Toggle (val, idx) in highlight_digits for reverse highlighting."""
        pair = (val, idx)
        if pair in self.kybe_highlight_digits:
            self.kybe_highlight_digits.remove(pair)
            self._remove_from_input(self.kybe_tong_input, str(val))
        else:
            # Reverse Mode coexistence: Clicking digit does NOT clear Trăm mode anymore
            self.kybe_highlight_ngau = []
            self.kybe_highlight_sum = []
            # (self.kybe_highlight_tram stays as is)
            
            if len(self.kybe_highlight_digits) >= 10:
                self.kybe_highlight_digits.pop(0)
            self.kybe_highlight_digits.append(pair)
            self._add_to_input(self.kybe_tong_input, str(val))
        
        self.update_kybe_touch_summary()
        self.refresh_kybe_highlights()

    def _on_kybe_tram_click_idx(self, val, idx):
        """Toggle (val, idx) in highlight_tram for reverse highlighting (highlights Ngàn/C.ngàn)."""
        if not hasattr(self, 'kybe_highlight_tram'):
            self.kybe_highlight_tram = []
            
        pair = (val, idx)
        if pair in self.kybe_highlight_tram:
            self.kybe_highlight_tram.remove(pair)
        else:
            # Reverse Mode coexistence: Clicking Trăm clears Xuôi modes but NOT Digit mode
            self.kybe_highlight_ngau = []
            self.kybe_highlight_sum = []
            
            if len(self.kybe_highlight_tram) >= 10:
                self.kybe_highlight_tram.pop(0)
            self.kybe_highlight_tram.append(pair)
        
        self.update_kybe_touch_summary()
        self.refresh_kybe_highlights()

    def _on_kybe_ngau_click_idx(self, val, idx):
        """Toggle (val, idx) in highlight_ngau and update Ngầu input."""
        v_str = "0" if str(val).upper() == "K" else str(val)
        v_int = -1
        try: v_int = int(v_str)
        except: pass
        
        pair = (v_int if v_int != -1 else v_str, idx)
        if pair in self.kybe_highlight_ngau:
            self.kybe_highlight_ngau.remove(pair)
            self._remove_from_input(self.kybe_ngau_input, v_str)
        else:
            # Clear reverse modes when activating forward mode
            self.kybe_highlight_digits = []
            if hasattr(self, 'kybe_highlight_tram'): self.kybe_highlight_tram = []
            
            if len(self.kybe_highlight_ngau) >= 10:
                self.kybe_highlight_ngau.pop(0)
            self.kybe_highlight_ngau.append(pair)
            self._add_to_input(self.kybe_ngau_input, v_str)
        
        self.update_kybe_touch_summary()
        self.refresh_kybe_highlights()

    def _on_kybe_sum_click_idx(self, mode, val, idx):
        """Toggle (mode, val, idx) in highlight_sum and update Tổng input."""
        v_int = int(val)
        pair = (mode, v_int, idx)
        if pair in self.kybe_highlight_sum:
            self.kybe_highlight_sum.remove(pair)
            # Mutual removal logic
            other_mode = "sum3" if mode == "sum5" else "sum5"
            other_pair = (other_mode, v_int, idx)
            if other_pair in self.kybe_highlight_sum:
                self.kybe_highlight_sum.remove(other_pair)
            self._remove_from_input(self.kybe_tong_input, str(val))
        else:
            # Clear reverse modes when activating forward mode
            self.kybe_highlight_digits = []
            if hasattr(self, 'kybe_highlight_tram'): self.kybe_highlight_tram = []
            
            if len(self.kybe_highlight_sum) >= 10:
                self.kybe_highlight_sum.pop(0)
            self.kybe_highlight_sum.append(pair)
            self._add_to_input(self.kybe_tong_input, str(val))
            
        self.update_kybe_touch_summary()
        self.refresh_kybe_highlights()

    def _add_to_input(self, entry, val):
        s = entry.get()
        items = [i for i in s.split(",") if i.strip()]
        if val not in items:
            items.append(val)
        entry.delete(0, tk.END)
        entry.insert(0, ",".join(sorted(list(set(items)))))

    def _remove_from_input(self, entry, val):
        s = entry.get()
        items = [i for i in s.split(",") if i.strip()]
        if val in items:
            items.remove(val)
        entry.delete(0, tk.END)
        entry.insert(0, ",".join(sorted(list(set(items)))))

    def _set_selectable_text(self, widget, text):
        """Helper to set text for read-only Text widgets."""
        widget.config(state='normal')
        widget.delete("1.0", tk.END)
        widget.insert("1.0", text)
        widget.config(state='disabled')

    def create_web_dashboard_view(self):
        """Create the professional 4-Module UI for the Web-Style Dashboard tab."""
        # Main container with scrolling
        container = tk.Frame(self.web_dashboard_frame, bg=self.bg_color)
        container.pack(fill=tk.BOTH, expand=True)

        canvas = tk.Canvas(container, bg=self.bg_color, highlightthickness=0)
        scrollbar = ttk.Scrollbar(container, orient=tk.VERTICAL, command=canvas.yview)
        self.web_scroll_frame = tk.Frame(canvas, bg=self.bg_color)

        self.web_scroll_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self._setup_canvas_mousewheel(canvas)
        # Also let the internal frame trigger the mousewheel setup
        self.web_scroll_frame.bind('<Enter>', lambda _: self._setup_canvas_mousewheel(canvas))

        # Ensure the interior frame expands to fill the canvas width and height
        def _configure_cell_size(event):
            # Expand frame to canvas width
            canvas.itemconfig(canvas_window, width=event.width)
            # If content is smaller than canvas, expand frame to canvas height
            # But normally we let it be its natural size if it's larger (scrolling)
            if self.web_scroll_frame.winfo_reqheight() < event.height:
                canvas.itemconfig(canvas_window, height=event.height)
        
        canvas.bind('<Configure>', _configure_cell_size)
        canvas_window = canvas.create_window((0, 0), window=self.web_scroll_frame, anchor='nw')

        # Configure layout: Column 0 (Left, modules) and Column 1 (Right, Sidebar)
        self.web_scroll_frame.columnconfigure(0, weight=65)
        self.web_scroll_frame.columnconfigure(1, weight=35)

        # 1. Prediction Module (Top Left)
        mod1 = tk.LabelFrame(self.web_scroll_frame, text="🔮 TIÊN ĐOÁN CAO CẤP", bg=self.bg_color, fg="#f1c40f", 
                            font=('Segoe UI', 12, 'bold'), padx=10, pady=10)
        mod1.grid(row=0, column=0, sticky='nsew', padx=10, pady=10)

        self.card_tram = tk.LabelFrame(mod1, text="HÀNG TRĂM & NHỊ HỢP", bg="#1e293b", fg="#34d399", 
                                       font=('Segoe UI', 10, 'bold'), padx=5, pady=5)
        self.card_tram.pack(fill='x', pady=5)
        # Use Text instead of Label for selectability
        self.lbl_tram_details = tk.Text(self.card_tram, bg="#1e293b", fg="#f1f5f9", font=('Segoe UI', 9),
                                        height=1, borderwidth=0, highlightthickness=0, wrap='word')
        self.lbl_tram_details.pack(fill='x', anchor='w')
        self.lbl_tram_extra = tk.Text(self.card_tram, bg="#1e293b", fg="#94a3b8", font=('Consolas', 9),
                                      height=3, borderwidth=0, highlightthickness=0, wrap='word')
        self.lbl_tram_extra.pack(fill='x', anchor='w')

        self.card_socuoi = tk.LabelFrame(mod1, text="2 SỐ CUỐI", bg="#1e293b", fg="#f472b6", 
                                         font=('Segoe UI', 10, 'bold'), padx=5, pady=5)
        self.card_socuoi.pack(fill='x', pady=5)
        self.lbl_socuoi_details = tk.Text(self.card_socuoi, bg="#1e293b", fg="#f1f5f9", font=('Segoe UI', 9),
                                          height=1, borderwidth=0, highlightthickness=0, wrap='word')
        self.lbl_socuoi_details.pack(fill='x', anchor='w')
        self.lbl_socuoi_extra = tk.Text(self.card_socuoi, bg="#1e293b", fg="#94a3b8", font=('Consolas', 8),
                                        height=3, borderwidth=0, highlightthickness=0, wrap='word')
        self.lbl_socuoi_extra.pack(fill='x', anchor='w')

        self.card_xito = tk.LabelFrame(mod1, text="XÌ TỐ & NGẦU", bg="#1e293b", fg="#60a5fa", 
                                       font=('Segoe UI', 10, 'bold'), padx=5, pady=5)
        self.card_xito.pack(fill='x', pady=5)
        self.lbl_xito_details = tk.Text(self.card_xito, bg="#1e293b", fg="#f1f5f9", font=('Segoe UI', 9),
                                        height=1, borderwidth=0, highlightthickness=0, wrap='word')
        self.lbl_xito_details.pack(fill='x', anchor='w')

        # 2. Smart Prediction Module (Middle Left)
        mod2 = tk.LabelFrame(self.web_scroll_frame, text="🎯 DỰ ĐOÁN THÔNG MINH", bg=self.bg_color, fg="#93c5fd", 
                            font=('Segoe UI', 12, 'bold'), padx=10, pady=10)
        mod2.grid(row=1, column=0, sticky='new', padx=10, pady=10)

        ctrl_frame = tk.Frame(mod2, bg=self.bg_color)
        ctrl_frame.pack(fill='x')
        
        self.smart_pred_mode = tk.StringVar(value='balanced')
        # Simple handler for updates
        def update_now(*args): self.render_web_dashboard()
        self.smart_pred_mode.trace_add("write", update_now)

        for m, n in [("hot", "HOT"), ("cold", "COLD"), ("balanced", "MIX")]:
            tk.Radiobutton(ctrl_frame, text=n, variable=self.smart_pred_mode, value=m, bg=self.bg_color, fg="white", selectcolor="#4c1d95", indicatoron=0, padx=10).pack(side='left', padx=2)

        self.web_tail_filter_var = tk.StringVar(value="0123456789")
        self.web_tail_filter_var.trace_add("write", update_now)
        tk.Label(ctrl_frame, text=" Bộ đuôi:", bg=self.bg_color, fg="#94a3b8").pack(side='left', padx=(10, 2))
        ttk.Entry(ctrl_frame, textvariable=self.web_tail_filter_var, width=8).pack(side='left')

        self.smart_pred_container = tk.Frame(mod2, bg=self.bg_color)
        self.smart_pred_container.pack(fill='both', expand=True, pady=10)

        self.smart_summary_frame = tk.Frame(mod2, bg="#0f172a", padx=5, pady=5)
        self.smart_summary_frame.pack(fill='x')
        self.lbl_smart_pairs = tk.Text(self.smart_summary_frame, bg="#0f172a", fg="#a78bfa", font=('Consolas', 10),
                                       height=2, borderwidth=0, highlightthickness=0, wrap='word')
        self.lbl_smart_pairs.pack(fill='x', anchor='w')

        # 3. Combinations Module (Bottom Left)
        mod3 = tk.LabelFrame(self.web_scroll_frame, text="📊 TOP 10 TỔ HỢP 4 ĐUÔI KHAN", bg=self.bg_color, fg="#f59e0b", 
                            font=('Segoe UI', 12, 'bold'), padx=10, pady=10)
        mod3.grid(row=2, column=0, sticky='new', padx=10, pady=10)

        cols = ('STT', 'Tổ hợp', 'Số lần', 'Đầu thiếu (Ngày đầu)', 'Trạng thái')
        self.web_stats_tree = ttk.Treeview(mod3, columns=cols, show='headings', height=10)
        for c in cols: self.web_stats_tree.heading(c, text=c)
        self.web_stats_tree.column('STT', width=30, anchor='center')
        self.web_stats_tree.column('Tổ hợp', width=80, anchor='center')
        self.web_stats_tree.column('Số lần', width=60, anchor='center')
        self.web_stats_tree.column('Trạng thái', width=60, anchor='center')
        self.web_stats_tree.pack(fill='both', expand=True)

        # 4. Result Grid Module (Bottom Right)
        mod4 = tk.LabelFrame(self.web_scroll_frame, text="📉 THỐNG KÊ CHI TIẾT", bg=self.bg_color, fg="#fbbf24", 
                            font=('Segoe UI', 12, 'bold'), padx=10, pady=10)
        mod4.grid(row=0, column=1, rowspan=3, sticky='nsew', padx=10, pady=10)

        nb = ttk.Notebook(mod4)
        nb.pack(fill='both', expand=True)

        page2 = tk.Frame(nb, bg=self.bg_color)
        nb.add(page2, text="Bảng Tổng")
        self.web_summary_tree = ttk.Treeview(page2, columns=('Ngày', 'Thống kê'), show='headings', height=25)
        self.web_summary_tree.heading('Ngày', text='Ngày')
        self.web_summary_tree.heading('Thống kê', text='Dàn & Thống kê')
        self.web_summary_tree.column('Ngày', width=80, anchor='center')
        self.web_summary_tree.column('Thống kê', width=250, anchor='w')
        
        vs_sum = ttk.Scrollbar(page2, orient='vertical', command=self.web_summary_tree.yview)
        self.web_summary_tree.configure(yscrollcommand=vs_sum.set)
        
        vs_sum.pack(side='right', fill='y')
        self.web_summary_tree.pack(side='left', fill='both', expand=True)

        page1 = tk.Frame(nb, bg=self.bg_color)
        nb.add(page1, text="Lưới Ô")

        # page1 = Lưới Ô
        c_grid = tk.Frame(page1, bg=self.bg_color)
        c_grid.pack(fill='both', expand=True)
        
        c_grid.grid_rowconfigure(1, weight=1) # Canvas row
        c_grid.grid_columnconfigure(0, weight=1) # Canvas column
        
        self.web_lo_canvas = tk.Canvas(c_grid, bg=self.bg_color, highlightthickness=0)
        vs = ttk.Scrollbar(c_grid, orient=tk.VERTICAL, command=self.web_lo_canvas.yview)
        hs = ttk.Scrollbar(c_grid, orient=tk.HORIZONTAL, command=self.web_lo_canvas.xview)
        
        # Layout with grid: Scrollbar at row 0 (top), Canvas at row 1
        hs.grid(row=0, column=0, sticky='ew')
        self.web_lo_canvas.grid(row=1, column=0, sticky='nsew')
        vs.grid(row=1, column=1, sticky='ns')
        
        self.web_lo_grid_frame = tk.Frame(self.web_lo_canvas, bg=self.bg_color)
        self.web_lo_canvas.create_window((0,0), window=self.web_lo_grid_frame, anchor='nw')
        self.web_lo_canvas.configure(yscrollcommand=vs.set, xscrollcommand=hs.set)
        
        self._setup_canvas_mousewheel(self.web_lo_canvas)

    def render_web_dashboard(self):
        """Perform calculations and update the Web Dashboard UI."""
        if not self.master_data and not self.station_data:
            return

        region = self.region_var.get()
        data_source = self.master_data if region == "Miền Bắc" else self.station_data
        
        # Apply current backtest/shift
        backtest_offset = self.backtest_var.get()
        working_data = data_source[backtest_offset:]
        
        if len(working_data) < 20:
            return

        # 1. Calculation: duDoanSoThieuTram
        recent_prizes = []
        for d in working_data:
            if region == "Miền Bắc":
                recent_prizes.append(d.get('xsmb_full', ''))
            else:
                # For MN/MT, handle if it's "Tất cả" or a specific station
                if 'items' in d: # All stations
                    for item in d['items']:
                        val = item.get('db', '').replace(',', '').strip()
                        if len(val) >= 5: 
                            recent_prizes.append(val)
                            break
                    else:
                        recent_prizes.append('')
                else: # Specific station
                    val = d.get('openNum', '').replace(',', '').strip()
                    if len(val) >= 5: recent_prizes.append(val)
                    else: recent_prizes.append('')

        recent_prizes = [p for p in recent_prizes if p]
        
        # Hundreds (Pos 2, 0-indexed)
        hundreds = []
        for p in recent_prizes:
            if len(p) >= 3:
                hundreds.append(int(p[-3]))
        
        last_seen = {}
        for idx, d in enumerate(hundreds):
            if d not in last_seen:
                last_seen[d] = idx
        
        missing = []
        for i in range(10):
            if i not in last_seen: missing.append(i)
            elif last_seen[i] > 20: missing.append(i)
            
        list_gan = []
        for d in range(10):
            last = last_seen.get(d, len(hundreds))
            list_gan.append({'num': d, 'last': last})
        
        # Sort by 'last' descending (most gan)
        top_gan = sorted(list_gan, key=lambda x: x['last'], reverse=True)[:5]
        top_nums = [x['num'] for x in top_gan]
        
        # Generate Nhi Hop
        nhi_hop = []
        for i in range(len(top_nums)):
            for j in range(i, len(top_nums)):
                a, b = top_nums[i], top_nums[j]
                nhi_hop.extend([f"{a}{b}", f"{b}{a}"])
        unique_nhi_hop = sorted(list(set(nhi_hop)))

        self._set_selectable_text(self.lbl_tram_details, f"Số cần chú ý (gan > 20): " + (", ".join(map(str, missing)) if missing else "Không"))
        self._set_selectable_text(self.lbl_tram_extra, f"Top 5 số lâu ra: {', '.join(map(str, top_nums))}\nDàn Nhị hợp + đảo ({len(unique_nhi_hop)} số):\n{', '.join(unique_nhi_hop)}")

        # 2. Calculation: duDoan2SoCuoi
        # Logic "4 số nóng": Thống kê tần suất của tất cả 2 số cuối trong tập dữ liệu đang xét (working_data)
        # và lấy ra 4 cặp số có số lần xuất hiện cao nhất.
        last2 = [p[-2:] for p in recent_prizes if len(p) >= 2]
        cnt2 = Counter(last2)
        hot_pairs = [k for k, v in cnt2.most_common(4)]
        
        # Positions 4 and 5 digits
        d4 = [int(p[-2]) for p in recent_prizes if len(p) >= 2]
        d5 = [int(p[-1]) for p in recent_prizes if len(p) >= 2]
        
        # Pick top 6 unique digits from the combined frequencies of both positions
        combined_freq = Counter(d4 + d5)
        u_digits = sorted([k for k, v in combined_freq.most_common(6)])
        
        nhi_hop_6x6 = []
        for a in u_digits:
            for b in u_digits:
                nhi_hop_6x6.append(f"{a}{b}")
        
        self._set_selectable_text(self.lbl_socuoi_details, f"4 số nóng: {', '.join(hot_pairs)} (Top về nhiều)")
        self._set_selectable_text(self.lbl_socuoi_extra, f"6 chữ số ghép: {', '.join(map(str, u_digits))}\nDàn Nhị hợp ({len(nhi_hop_6x6)} số):\n{', '.join(nhi_hop_6x6)}")

        # 3. Calculation: Xi To & Ngau
        xi_results = []
        ngau_results = []
        for d in working_data[:20]:
            db_num = ""
            if region == "Miền Bắc":
                db_num = d.get('xsmb_full', '')
            else:
                if 'items' in d: # MN/MT All stations
                    for it in d['items']:
                        db_num = it.get('db', ''); break
                else: # Specific station
                    db_num = d.get('db', '')
            
            if db_num:
                digits = [int(digit) for digit in db_num if digit.isdigit()][-5:]
                if len(digits) == 5:
                    xi_results.append(classifyXiTo(digits))
                    ngau_val = classifyNgau(digits)
                    if ngau_val == 'K': ngau_val = '0'
                    ngau_results.append(str(ngau_val))
        
        xi_hot = [k for k, v in Counter(xi_results).most_common(2)]
        ngau_hot = [k for k, v in Counter(ngau_results).most_common(2)]
        
        self._set_selectable_text(self.lbl_xito_details, f"Xì tố hay ra: {', '.join(xi_hot)}    |    Ngầu hay ra: {', '.join(ngau_hot)}")

        # 4. Calculation: Top 10 combos of 4 tails
        self._update_web_top_combos(working_data, region)

        # 5. Calculation: Smart Prediction
        self._update_web_smart_predictions(working_data, region)

        # 6. Calculation: Lo Analysis Grid
        self._update_web_lo_grid(working_data, region)

        # 7. Calculation: Detailed Summary Table
        self._update_web_summary_table(working_data, region)

    def _update_web_summary_table(self, working_data, region):
        for item in self.web_summary_tree.get_children(): self.web_summary_tree.delete(item)
        tail_filter = self.web_tail_filter_var.get()
        digits = "0123456789"
        
        for day in working_data[:30]:
            date_str = day.get('date', '—')
            db_val = ""
            if region == "Miền Bắc":
                db_val = day.get('xsmb_full', '')
            else:
                if 'items' in day:
                    for it in day['items']:
                        if it.get('db'): db_val = it.get('db', ''); break
                else:
                    db_val = day.get('db', '')
            
            cells = self._get_expanded_cells(day, region)
            tails = []
            dau_set = set()
            for v in cells:
                if len(v) >= 1:
                    t = v[-1]
                    if t in tail_filter:
                        tails.append(t)
                        if len(v) >= 2: dau_set.add(v[-2])
            
            # Count tails
            t_counts = Counter(tails)
            missing_dau = [d for d in digits if d not in dau_set]
            missing_str = ",".join(missing_dau) if missing_dau else "Đủ"
            
            # Consolidated info string
            stat_info = f"🎯 {db_val}  |  🔥 {len(tails)} số  |  ❌ Đầu thiếu: {missing_str}"
            
            self.web_summary_tree.insert('', tk.END, values=(
                date_str, stat_info
            ))

    def _update_web_top_combos(self, working_data, region):
        stats_days = working_data[:30]
        digits_range = list("0123456789")
        combo_list = list(combinations(digits_range, 4))
        
        combo_data = { "".join(c): {'total': 0, 'last_missing_heads': []} for c in combo_list }

        daily_tails = []
        daily_full_cells = []
        for day in stats_days:
            cells = self._get_expanded_cells(day, region)
            tails = [c[-1] for c in cells if c and c[-1].isdigit()]
            daily_tails.append(tails)
            daily_full_cells.append(cells)

        for i, tails in enumerate(daily_tails):
            for combo in combo_list:
                key = "".join(combo)
                match_count = sum(1 for t in tails if t in combo)
                combo_data[key]['total'] += match_count
                if i == 0:
                    present_heads = {c[-2] for c in daily_full_cells[i] if len(c) >= 2 and c[-1] in combo}
                    combo_data[key]['last_missing_heads'] = [d for d in digits_range if d not in present_heads]

        sorted_combos = sorted(combo_data.items(), key=lambda x: x[1]['total'])[:10]
        for item in self.web_stats_tree.get_children(): self.web_stats_tree.delete(item)
        for i, (combo, data) in enumerate(sorted_combos):
            avg = data['total'] / len(stats_days) if stats_days else 0
            missing_str = ", ".join(data['last_missing_heads']) if data['last_missing_heads'] else "Đủ"
            self.web_stats_tree.insert('', tk.END, values=(i+1, combo, f"{data['total']} lần", missing_str, f"{avg:.1f}"))

    def _update_web_smart_predictions(self, working_data, region):
        if len(working_data) < 7: return
        tail_filter = self.web_tail_filter_var.get()
        digits = "0123456789"
        
        # Analyze heads frequency over 7 days for the selected tails
        dau_freq = {d: {'count': 0, 'with_tail': Counter()} for d in digits}
        for day in working_data[:7]:
            cells = self._get_expanded_cells(day, region)
            for v in cells:
                if len(v) >= 2:
                    head, tail = v[-2], v[-1]
                    if tail in tail_filter and head in digits:
                        dau_freq[head]['count'] += 1
                        dau_freq[head]['with_tail'][tail] += 1

        # Find missing heads from 'yesterday' (working_data[0])
        cells_yesterday = self._get_expanded_cells(working_data[0], region)
        present_heads_yesterday = {v[-2] for v in cells_yesterday if len(v) >= 2 and v[-1] in tail_filter}
        missing_heads = [d for d in digits if d not in present_heads_yesterday]

        mode = self.smart_pred_mode.get()
        predictions = []
        
        if mode == 'hot':
            # Hot: Missing head yesterday + High frequency over 7 days
            hot_list = sorted([d for d in missing_heads], key=lambda d: dau_freq[d]['count'], reverse=True)[:3]
            for head in hot_list:
                best_tails = [k for k, v in dau_freq[head]['with_tail'].most_common(2)]
                # If not enough tails, use first available in filter
                while len(best_tails) < 2 and len(tail_filter) > len(best_tails):
                    for t in tail_filter:
                        if t not in best_tails:
                            best_tails.append(t)
                            break
                predictions.append({'nums': [head + t for t in best_tails], 'score': dau_freq[head]['count'], 
                                   'reason': f"Đầu {head} thiếu hôm qua, ra {dau_freq[head]['count']} lần/7 ngày"})
        elif mode == 'cold':
            # Cold: Low frequency heads
            cold_list = sorted(digits, key=lambda d: dau_freq[d]['count'])[:3]
            for head in cold_list:
                coldest_tails = [t for t in tail_filter if t not in [k for k, v in dau_freq[head]['with_tail'].most_common()]][:2]
                predictions.append({'nums': [head + t for t in coldest_tails], 'score': 10 - dau_freq[head]['count'],
                                   'reason': f"Đầu {head} chỉ ra {dau_freq[head]['count']} lần/7 ngày"})
        else: # Balanced
            # One hot, one cold
            hot_head = sorted(missing_heads, key=lambda d: dau_freq[d]['count'], reverse=True)[0] if missing_heads else None
            cold_head = sorted(digits, key=lambda d: dau_freq[d]['count'])[0]
            if hot_head:
                predictions.append({'nums': [hot_head + t for t in tail_filter[:2]], 'score': dau_freq[hot_head]['count'], 'reason': "Cân bằng: Đầu HOT"})
            predictions.append({'nums': [cold_head + t for t in tail_filter[-2:]], 'score': 10 - dau_freq[cold_head]['count'], 'reason': "Cân bằng: Đầu LẠNH"})

        # Render Smart Predictions
        for widget in self.smart_pred_container.winfo_children(): widget.destroy()
        all_pred_nums = []
        for i, pred in enumerate(predictions):
            frame = tk.Frame(self.smart_pred_container, bg="#1e293b", padx=10, pady=5)
            frame.pack(fill=tk.X, pady=2)
            tk.Label(frame, text=f"🎲 Dự đoán #{i+1} (Score: {pred['score']})", bg="#1e293b", fg="#93c5fd", font=('Segoe UI', 9, 'bold')).pack(anchor='w')
            self._draw_number_badges(frame, pred['nums'])
            tk.Label(frame, text=pred['reason'], bg="#1e293b", fg="#9ca3af", font=('Segoe UI', 8)).pack(anchor='w')
            all_pred_nums.extend(pred['nums'])
        
        sorted_all = sorted(list(set(all_pred_nums)))
        self._set_selectable_text(self.lbl_smart_pairs, f"📋 Dàn tổng hợp: {', '.join(sorted_all)}")
        self.web_predicted_nums = sorted_all

    def _update_web_lo_grid(self, working_data, region):
        for widget in self.web_lo_grid_frame.winfo_children(): widget.destroy()
        tail_filter = self.web_tail_filter_var.get()
        if not tail_filter: return

        # Headers: Date | Tail numbers
        all_lo = sorted([d + t for d in "0123456789" for t in tail_filter], key=int)
        
        # Draw Headers
        tk.Label(self.web_lo_grid_frame, text="Ngày", bg="#374151", fg="white", font=('Segoe UI', 9, 'bold'), width=12, relief='solid').grid(row=0, column=0)
        for i, lo in enumerate(all_lo):
            tk.Label(self.web_lo_grid_frame, text=lo, bg="#374151", fg="white", font=('Segoe UI', 8, 'bold'), width=4, relief='solid').grid(row=0, column=i+1)

        # Draw rows for 20 days
        for r_idx, day in enumerate(working_data[:20]):
            date_str = day.get('date', '—')
            cells = self._get_expanded_cells(day, region)
            present_lo = {v[-2:] for v in cells if len(v) >= 2 and v[-1] in tail_filter}
            
            tk.Label(self.web_lo_grid_frame, text=date_str, bg=self.secondary_bg, fg=self.fg_color, font=('Consolas', 8), relief='solid').grid(row=r_idx+1, column=0, sticky='nsew')
            
            for c_idx, lo in enumerate(all_lo):
                is_hit = lo in present_lo
                is_pred = hasattr(self, 'web_predicted_nums') and lo in self.web_predicted_nums
                
                color = "#10b981" if is_hit else (self.bg_color if not is_pred else "#450a0a")
                fg = "white" if is_hit else ("#6b7280" if not is_pred else "#fca5a5")
                text = "✓" if is_hit else "—"
                if is_pred and is_hit: color = "#ef4444" # Highlight hit predicted
                
                tk.Label(self.web_lo_grid_frame, text=text, bg=color, fg=fg, font=('Consolas', 8, 'bold'), relief='solid').grid(row=r_idx+1, column=c_idx+1, sticky='nsew')

        self.web_lo_grid_frame.bind(
            "<Configure>",
            lambda e: self.web_lo_canvas.configure(scrollregion=self.web_lo_canvas.bbox("all"))
        )
        self.web_lo_grid_frame.update_idletasks()
        self.web_lo_canvas.config(scrollregion=self.web_lo_canvas.bbox("all"))
        # Ensure scroll to top/left after update
        self.web_lo_canvas.yview_moveto(0)
        self.web_lo_canvas.xview_moveto(0)

    def _get_expanded_cells(self, day, region):
        """Extract all prize numbers from a day's data, handling consolidated MN/MT items."""
        if 'items' in day: # MN/MT All stations
            cells = []
            for it in day['items']:
                cells.extend(it.get('all_prizes', []))
            return cells
        else: # MB or single station MN/MT
            return day.get('all_prizes', [])

    def _draw_number_badges(self, parent, nums):
        frame = tk.Frame(parent, bg=parent.cget('bg'))
        frame.pack(anchor='w', pady=2)
        for n in nums:
            # Use a readonly Entry instead of Label for selectability
            ent = tk.Entry(frame, bg="#facc15", fg="#1a1f2e", font=('Consolas', 10, 'bold'), 
                           width=len(str(n))+1, relief='flat', justify='center')
            ent.insert(0, n)
            ent.config(state='readonly')
            ent.pack(side=tk.LEFT, padx=2)


    def create_bc_cd_de_view(self):
        """Làm lại giao diện Tab BC-CD-DE chuyên nghiệp - Stats Focus (No Matrix Grid)."""
        # Top Header - Dark Green
        info_frame = tk.Frame(self.bc_cd_de_frame, bg="#1a472a")
        info_frame.pack(fill=tk.X)
        
        tk.Label(info_frame, text="🚀 GHÉP DÀN & THỐNG KÊ BC - CD - DE", 
                 bg="#1a472a", fg="white", font=('Segoe UI', 14, 'bold')).pack(side=tk.LEFT, padx=20, pady=8)

        # Labels for Top Khan mirrored from Matrix
        self.bc_khan_4d_label = tk.Label(info_frame, text="Khan 4D: --", bg="#1a472a", fg="orange", font=('Segoe UI', 9, 'bold'))
        self.bc_khan_4d_label.pack(side=tk.RIGHT, padx=10, pady=8)
        
        self.bc_khan_3d_label = tk.Label(info_frame, text="Khan 3D: --", bg="#1a472a", fg="gold", font=('Segoe UI', 9, 'bold'))
        self.bc_khan_3d_label.pack(side=tk.RIGHT, padx=10, pady=8)

        # Control panel (Pool selectors)
        ctrl_frame = tk.Frame(self.bc_cd_de_frame, bg="#2d2d2d")
        ctrl_frame.pack(fill=tk.X)
        
        # BC, CD, DE Pool selectors (using existing vars if possible, or ensuring they exist)
        if not hasattr(self, 'bc_pool_var'): self.bc_pool_var = tk.BooleanVar(value=True)
        if not hasattr(self, 'cd_pool_var'): self.cd_pool_var = tk.BooleanVar(value=True)
        if not hasattr(self, 'de_pool_var'): self.de_pool_var = tk.BooleanVar(value=True)
        
        # Trace them to re-render
        self.bc_pool_var.trace_add('write', lambda *a: self.render_bc_cd_de())
        self.cd_pool_var.trace_add('write', lambda *a: self.render_bc_cd_de())
        self.de_pool_var.trace_add('write', lambda *a: self.render_bc_cd_de())
        
        self.bc_show_result_var = tk.BooleanVar(value=False)
        
        for text, var, color in [("BC", self.bc_pool_var, "#e74c3c"), 
                                 ("CD", self.cd_pool_var, "#f1c40f"), 
                                 ("DE", self.de_pool_var, "#2ecc71")]:
            tk.Checkbutton(ctrl_frame, text=text, variable=var, bg="#2d2d2d", fg=color,
                           selectcolor="#34495e", activebackground="#2d2d2d", 
                           font=('Segoe UI', 12, 'bold')).pack(side=tk.LEFT, padx=15, pady=5)
        
        tk.Checkbutton(ctrl_frame, text="Hiện Cột Kết quả", variable=self.bc_show_result_var,
                       bg="#2d2d2d", fg="white", selectcolor="#2d2d2d", activebackground="#2d2d2d",
                       font=('Segoe UI', 11), command=self.render_bc_cd_de).pack(side=tk.LEFT, padx=20)

        # Main container
        content_container = tk.Frame(self.bc_cd_de_frame, bg=self.bg_color)
        content_container.pack(fill=tk.BOTH, expand=True)
        
        # === LEFT SIDE: Selection Table (Minimized) ===
        left_selection_frame = tk.Frame(content_container, bg=self.bg_color, width=500)
        left_selection_frame.pack(side=tk.LEFT, fill=tk.BOTH, padx=(0, 2))
        left_selection_frame.pack_propagate(False)
        
        canvas_container = tk.Frame(left_selection_frame, bg=self.bg_color)
        canvas_container.pack(fill=tk.BOTH, expand=True)
        
        self.bc_v_scroll = ttk.Scrollbar(canvas_container, orient=tk.VERTICAL)
        self.bc_v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Use single canvas for the list (Matrix grid removed)
        self.bc_fixed_canvas = tk.Canvas(canvas_container, bg="#1e1e1e", highlightthickness=0)
        self.bc_fixed_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        self.bc_fixed_canvas.config(yscrollcommand=self.bc_v_scroll.set)
        self.bc_v_scroll.config(command=self.bc_fixed_canvas.yview)
        
        self._setup_canvas_mousewheel(self.bc_fixed_canvas)
        
        self.bc_fixed_inner = tk.Frame(self.bc_fixed_canvas, bg="#1e1e1e")
        self.bc_fixed_canvas_window = self.bc_fixed_canvas.create_window((0, 0), window=self.bc_fixed_inner, anchor='nw')
        
        def _resize_inner(event):
            self.bc_fixed_canvas.itemconfig(self.bc_fixed_canvas_window, width=event.width)
        self.bc_fixed_canvas.bind('<Configure>', _resize_inner)

        # === RIGHT SIDE: Statistics (Expanded) ===
        stats_frame = tk.Frame(content_container, bg="#2d2d2d")
        stats_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Stats Header
        s_header = tk.Frame(stats_frame, bg="#333333")
        s_header.pack(fill=tk.X)
        tk.Label(s_header, text="📊 PHÂN TÍCH & GHÉP DÀN", bg="#333333", fg=self.accent_color, 
                 font=('Segoe UI', 12, 'bold'), pady=5).pack(side=tk.LEFT, padx=10)
        
        tk.Checkbutton(s_header, text="Hiện chi tiết số", variable=self.bc_show_sidebar_nums,
                       bg="#333333", fg="white", selectcolor="#333333", activebackground="#333333",
                       font=('Segoe UI', 9), 
                       command=self.render_bc_cd_de).pack(side=tk.RIGHT, padx=5)

        # 2. Detailed Results & Sets
        s_text_frame = tk.Frame(stats_frame, bg="#1e1e1e")
        s_text_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        s_scroll = ttk.Scrollbar(s_text_frame)
        s_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.bc_stats_text = tk.Text(s_text_frame, wrap=tk.WORD, bg="#1e1e1e", fg="white",
                                     font=('Consolas', 11), yscrollcommand=s_scroll.set,
                                     borderwidth=0, padx=15, pady=10)
        self.bc_stats_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        s_scroll.config(command=self.bc_stats_text.yview)

        # Tags
        self.bc_stats_text.tag_configure('title', font=('Segoe UI', 13, 'bold'), foreground=self.accent_color)
        self.bc_stats_text.tag_configure('subtitle', font=('Segoe UI', 12, 'bold'), foreground="#f39c12")
        self.bc_stats_text.tag_configure('level', font=('Consolas', 11, 'bold'), foreground="#e74c3c")
        self.bc_stats_text.tag_configure('numbers', font=('Consolas', 10), foreground="#bdc3c7")
        self.bc_stats_text.tag_configure('hit_info', font=('Segoe UI', 11, 'bold'), foreground="#2ecc71")
        self.bc_stats_text.tag_configure('highlight', background="#2c3e50")

    def create_level0_view(self):
        """Tạo giao diện Tab Lọc Mức 0 (Nhị Hợp) dựa trên các kỳ trúng hôm nay."""
        self.level0_check_vars = {} # Lưu các checkbox biến
        
        # Top Header
        info_frame = tk.Frame(self.level0_tab_frame, bg="#1a472a")
        info_frame.pack(fill=tk.X)
        
        tk.Label(info_frame, text="🎯 LỌC MỨC 0 (NHỊ HỢP) - Tích chọn kỳ trúng để tính Mức 0", 
                 bg="#1a472a", fg="white", font=('Segoe UI', 14, 'bold')).pack(side=tk.LEFT, padx=20, pady=8)

        # Control Frame for 3D Filter
        self.level0_cang_3d_var = tk.StringVar(value="")
        self.level0_cang_3d_var.trace_add('write', lambda *a: self.calculate_level0_final())
        
        ctrl_frame = tk.Frame(self.level0_tab_frame, bg="#2d2d2d")
        ctrl_frame.pack(fill=tk.X, padx=0, pady=0)
        
        # 1. Dòng cho lọc Càng (3D/4D)
        cang_row = tk.Frame(ctrl_frame, bg="#2d2d2d")
        cang_row.pack(fill=tk.X)

        self.level0_cang_label = tk.Label(cang_row, text="🔢 Càng 3D (Đầu số):", bg="#2d2d2d", fg="#f39c12", 
                                          font=('Segoe UI', 10, 'bold'))
        self.level0_cang_label.pack(side=tk.LEFT, padx=(20, 5), pady=5)
        
        cang_entry = tk.Entry(cang_row, textvariable=self.level0_cang_3d_var, bg="#1e1e1e", 
                              fg="white", insertbackground="white", width=30, font=('Consolas', 11, 'bold'))
        cang_entry.pack(side=tk.LEFT, padx=5, pady=5)
        
        tk.Label(cang_row, text="(Nhập 1 số -> 3D, 2 số -> 4D. Ví dụ: 2,5,8 hoặc 24,45)", bg="#2d2d2d", fg="#bdc3c7", 
                 font=('Segoe UI', 9, 'italic')).pack(side=tk.LEFT, padx=10)

        # 2. Dòng cho Thêm Dàn Thủ Công
        manual_row = tk.Frame(ctrl_frame, bg="#34495e")
        manual_row.pack(fill=tk.X, pady=(1, 0))

        tk.Label(manual_row, text="➕ Thêm Dãy Số:", bg="#34495e", fg="white", font=('Segoe UI', 9, 'bold')).pack(side=tk.LEFT, padx=(20, 5), pady=5)
        self.level0_manual_digits_var = tk.StringVar()
        # Thêm trace để tự cập nhật dàn nhị hợp khi đang gõ
        self.level0_manual_digits_var.trace_add('write', lambda *a: self.update_manual_level0_preview())
        
        tk.Entry(manual_row, textvariable=self.level0_manual_digits_var, bg="#1e1e1e", fg="white", insertbackground="white", 
                 width=15, font=('Consolas', 11)).pack(side=tk.LEFT, padx=5, pady=5)

        tk.Button(manual_row, text="LƯU DÀN", command=self.add_manual_level0_row, bg="#27ae60", fg="white", 
                  font=('Segoe UI', 9, 'bold'), padx=10).pack(side=tk.LEFT, padx=15, pady=5)

        tk.Label(manual_row, text="📋 Nhị hợp để copy:", bg="#34495e", fg="white", font=('Segoe UI', 9)).pack(side=tk.LEFT, padx=(5, 5))
        self.level0_manual_output_var = tk.StringVar()
        tk.Entry(manual_row, textvariable=self.level0_manual_output_var, bg="#1e1e1e", fg="#3498db", insertbackground="white", 
                 width=35, font=('Consolas', 10, 'bold')).pack(side=tk.LEFT, padx=5, pady=5)

        tk.Label(manual_row, text="(Tự đảo thành Nhị hợp. Ví dụ: 376)", bg="#34495e", fg="#ecf0f1", font=('Segoe UI', 8, 'italic')).pack(side=tk.LEFT)

        # Main container
        content_container = tk.Frame(self.level0_tab_frame, bg=self.bg_color)
        content_container.pack(fill=tk.BOTH, expand=True)
        
        # === LEFT Side: Selection Module (50%) ===
        left_frame = tk.Frame(content_container, bg=self.bg_color)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        tk.Label(left_frame, text="📋 DANH SÁCH CÁC KỲ TRÚNG HÔM NAY", 
                 bg="#333333", fg="white", font=('Segoe UI', 10, 'bold')).pack(fill=tk.X)
        
        self.level0_canvas_frame = tk.Frame(left_frame, bg="#1e1e1e")
        self.level0_canvas_frame.pack(fill=tk.BOTH, expand=True)
        
        self.level0_v_scroll = ttk.Scrollbar(self.level0_canvas_frame, orient=tk.VERTICAL)
        self.level0_v_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.level0_canvas = tk.Canvas(self.level0_canvas_frame, bg="#1e1e1e", highlightthickness=0)
        self.level0_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        self.level0_v_scroll.config(command=self.level0_canvas.yview)
        self.level0_canvas.config(yscrollcommand=self.level0_v_scroll.set)
        
        self.level0_inner_frame = tk.Frame(self.level0_canvas, bg="#1e1e1e")
        self.level0_canvas.create_window((0, 0), window=self.level0_inner_frame, anchor='nw')
        
        self.level0_inner_frame.bind("<Configure>", lambda e: self.level0_canvas.configure(scrollregion=self.level0_canvas.bbox("all")))
        
        # === RIGHT Side: Results Module (50%) ===
        right_frame = tk.Frame(content_container, bg="#2d2d2d")
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        s_header = tk.Frame(right_frame, bg="#333333")
        s_header.pack(fill=tk.X)
        tk.Label(s_header, text="📊 KẾT QUẢ PHÂN TÍCH MỨC 0", bg="#333333", fg=self.accent_color, 
                 font=('Segoe UI', 12, 'bold'), pady=5).pack(side=tk.LEFT, padx=10)

        # Scrolled Text
        s_text_frame = tk.Frame(right_frame, bg="#1e1e1e")
        s_text_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        s_scroll = ttk.Scrollbar(s_text_frame)
        s_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.level0_stats_text = tk.Text(s_text_frame, wrap=tk.WORD, bg="#1e1e1e", fg="white",
                                         font=('Consolas', 11), yscrollcommand=s_scroll.set,
                                         borderwidth=0, padx=10, pady=10)
        self.level0_stats_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        s_scroll.config(command=self.level0_stats_text.yview)

        # Tags
        self.level0_stats_text.tag_configure('title', font=('Segoe UI', 13, 'bold'), foreground=self.accent_color)
        self.level0_stats_text.tag_configure('subtitle', font=('Segoe UI', 12, 'bold'), foreground="#f39c12")
        self.level0_stats_text.tag_configure('level0', font=('Consolas', 11, 'bold'), foreground="#e74c3c")
        self.level0_stats_text.tag_configure('numbers', font=('Consolas', 10), foreground="#bdc3c7")
        self.level0_stats_text.tag_configure('hit_info', font=('Segoe UI', 11, 'bold'), foreground="#2ecc71")
        self.level0_stats_text.tag_configure('analysis', font=('Segoe UI', 10, 'italic'), foreground="#95a5a6")

    def render_level0_analysis(self):
        """Logic: Tìm các kỳ trúng hôm nay -> Reset checkbox -> Hiển thị danh sách."""
        # Tự động điền Càng 3D mặc định nếu ô nhập trống (không phụ thuộc mode 2D/3D)
        if not self.level0_cang_3d_var.get().strip():
            top_khan = getattr(self, 'exceeding_tram_3d', [])
            if not top_khan:
                top_khan = self._get_fallback_top_digits(is_thousands=False)
            if top_khan:
                self.level0_cang_3d_var.set(",".join([str(d) for d in top_khan]))
            
        region = self.region_var.get()
        data_source = self.master_data if region == "Miền Bắc" else self.station_data
        if not data_source: return
        
        backtest_offset = self.backtest_var.get()
        data_slice = data_source[backtest_offset : backtest_offset + self.MAX_DISPLAY_DAYS]
        if not data_slice: return

        # Clear Left UI
        for w in self.level0_inner_frame.winfo_children(): w.destroy()
        self.level0_check_vars = {}
        self.current_winning_rows = [] # Store detected hit rows

        # 1. Lấy kết quả hôm nay
        today_row = data_slice[0]
        mode = self.mode_var.get()
        w = 3 if mode == "3D" else 2
        
        if region != "Miền Bắc" and 'items' in today_row:
            today_hits = []
            for item in today_row['items']:
                today_hits.extend(self.get_prize_numbers(item, width=w, tail_only=True))
        else:
            today_hits = self.get_prize_numbers(today_row, width=w, tail_only=True)
        
        self.today_hits_set = set(today_hits)
        self.today_date_str = today_row.get('date', 'N/A')

        # 2. Tìm các kỳ đã trúng hôm nay
        source_type = self.source_var.get()
        universal_source = {row.get('date'): (row.get('tt_number', '') if source_type == "Thần Tài" else "".join(row.get('dt_numbers', []))) 
                          for row in self.master_data}

        # Grid Headers cho cột bên trái
        header_f = tk.Frame(self.level0_inner_frame, bg="#1a472a")
        header_f.pack(fill=tk.X, pady=(0, 2))
        
        # Cấu hình grid cho header_f
        header_f.columnconfigure(0, minsize=65) # Cột Chọn
        header_f.columnconfigure(1, minsize=60) # Cột Cột
        header_f.columnconfigure(2, minsize=110) # Cột Ngày
        header_f.columnconfigure(3, minsize=100) # Cột Số Trúng
        header_f.columnconfigure(4, minsize=60) # Cột Dây

        for idx, (txt, w_val) in enumerate([("Chọn", 0), ("Cột", 1), ("Ngày", 2), ("Số Trúng", 3), ("Dây", 4)]):
            tk.Label(header_f, text=txt, bg="#1a472a", fg="white", font=('Segoe UI', 10, 'bold'), 
                     relief='flat', borderwidth=1).grid(row=0, column=idx, sticky='nsew', padx=1)
        for k in range(1, self.MAX_MATRIX_COLS + 1):
            if k >= len(data_slice): break
            target_row = data_slice[k]
            target_date = target_row.get('date')
            src_str = universal_source.get(target_date, '')
            if not src_str: continue
            
            digits = set(src_str)
            if mode == "3D": 
                target_combos = sorted({a+b+c for a in digits for b in digits for c in digits})
            else: 
                target_combos = sorted({a+b for a in digits for b in digits})
            
            hits_in_this_row = [h for h in today_hits if h in target_combos]
            
            if hits_in_this_row:
                # Logic Ăn Liên Tục: Kiểm tra lùi về quá khứ từ hôm qua (index 1)
                continuous_days = 1
                for prev_idx in range(1, len(data_slice) - k):
                    # Lấy kết quả ngày prev_idx
                    p_hits = []
                    p_row = data_slice[prev_idx]
                    if region != "Miền Bắc" and 'items' in p_row:
                        for it in p_row['items']:
                            p_hits.extend(self.get_prize_numbers(it, width=w, tail_only=True))
                    else:
                        p_hits = self.get_prize_numbers(p_row, width=w, tail_only=True)
                    
                    if any(h in target_combos for h in p_hits):
                        continuous_days += 1
                    else:
                        break
                
                row_data = {
                    'n_idx': k,
                    'date': target_date,
                    'combos': target_combos,
                    'hits': sorted(hits_in_this_row),
                    'streak': continuous_days,
                    'is_manual': False
                }
                self.current_winning_rows.append(row_data)

        # Gộp các hàng thủ công vào danh sách
        for m_row in self.manual_level0_rows:
            self.current_winning_rows.append(m_row)

        # 3. Tạo Row UI cho toàn bộ danh sách gộp
        for item in self.current_winning_rows:
            k = item['n_idx']
            target_date = item['date']
            hits_in_this_row = item.get('hits', [])
            continuous_days = item.get('streak', 1)
            is_manual = item.get('is_manual', False)
            
            var = tk.BooleanVar(value=True) 
            var.trace_add('write', lambda *a: self.calculate_level0_final())
            self.level0_check_vars[k] = var
            
            is_streak = continuous_days > 1
            if is_manual:
                bg_c = "#16a085"
            else:
                bg_c = "#2980b9" if is_streak else ("#2d2d2d" if len(self.level0_check_vars) % 2 == 0 else "#1e1e1e")
            fg_c = "white"
            
            row_f = tk.Frame(self.level0_inner_frame, bg=bg_c)
            row_f.pack(fill=tk.X, pady=1)
            row_f.columnconfigure(0, minsize=65); row_f.columnconfigure(1, minsize=60); row_f.columnconfigure(2, minsize=110); row_f.columnconfigure(3, minsize=100); row_f.columnconfigure(4, minsize=60)
            
            tk.Checkbutton(row_f, variable=var, bg=bg_c, selectcolor="#f39c12").grid(row=0, column=0, pady=2)
            n_text = f"N{k}" if not is_manual else "Tay"
            tk.Label(row_f, text=n_text, bg=bg_c, fg=fg_c, font=('Segoe UI', 10, 'bold')).grid(row=0, column=1, sticky='nsew')
            tk.Label(row_f, text=target_date, bg=bg_c, fg=fg_c, font=('Segoe UI', 10)).grid(row=0, column=2, sticky='nsew')
            
            hit_text = ", ".join(hits_in_this_row) if not is_manual else f"{len(item['combos'])}s Nhị hợp"
            tk.Label(row_f, text=hit_text, bg=bg_c, fg="#f1c40f" if (is_streak or is_manual) else "#2ecc71", font=('Consolas', 10, 'bold')).grid(row=0, column=3, sticky='nsew')
            
            if is_manual:
                btn_del = tk.Button(row_f, text="✖", bg="#e74c3c", fg="white", font=('Segoe UI', 8, 'bold'),
                                    command=lambda mid=k: self.remove_manual_level0_row(mid), padx=5, pady=0)
                btn_del.grid(row=0, column=4, padx=2)
            else:
                streak_text = f"{continuous_days}đ" if is_streak else ""
                tk.Label(row_f, text=streak_text, bg=bg_c, fg="white", font=('Segoe UI', 10, 'bold')).grid(row=0, column=4, sticky='nsew')

        # 3. Tính toán lần đầu
        self.calculate_level0_final()

    def calculate_level0_final(self):
        """Tính toán Mức 0 dựa trên các kỳ đang được tích chọn."""
        if not hasattr(self, 'current_winning_rows') or not self.current_winning_rows:
            self.level0_stats_text.delete('1.0', tk.END)
            self.level0_stats_text.insert(tk.END, "Vui lòng tải dữ liệu và đảm bảo có kỳ trúng thưởng.", 'analysis')
            return

        self.level0_stats_text.delete('1.0', tk.END)
        
        mode = self.mode_var.get()
        union_set = set()
        selected_count = 0
        
        # Tiêu đề kết quả
        self.level0_stats_text.insert(tk.END, f"📅 NGÀY PHÂN TÍCH: {self.today_date_str}\n", 'title')
        self.level0_stats_text.insert(tk.END, f"🎯 Kết quả trúng ({mode}): {', '.join(sorted(self.today_hits_set))}\n", 'hit_info')
        self.level0_stats_text.insert(tk.END, f"------------------------------------------\n\n")
        
        self.level0_stats_text.insert(tk.END, "✅ CÁC KỲ ĐANG CHỌN ĐỂ TÍNH:\n", 'subtitle')

        # 1. Lấy danh sách Càng từ ô nhập liệu (Hỗ trợ cả dấu phẩy và dấu cách)
        raw_cang = self.level0_cang_3d_var.get().replace(',', ' ')
        selected_cang = [c.strip() for c in raw_cang.split() if c.strip().isdigit()]
        
        # Tự động phát hiện 3D hay 4D dựa trên độ dài Càng
        is_4d = any(len(c) == 2 for c in selected_cang)
        if hasattr(self, 'level0_cang_label'):
            self.level0_cang_label.config(text="🔢 Càng 4D (Nối đầu):" if is_4d else "🔢 Càng 3D (Đầu số):")
            
        # 2. Tính toán Tần suất (Mức số) - LUÔN TÍNH TRÊN 2D
        number_freq = defaultdict(int)
        for item in self.current_winning_rows:
            kid = item['n_idx']
            if self.level0_check_vars.get(kid, tk.BooleanVar(value=False)).get():
                selected_count += 1
                for num in item['combos']:
                    # Lấy 2 số cuối (Nhị hợp)
                    base_num = num[-2:] if len(num) >= 2 else num.zfill(2)
                    number_freq[base_num] += 1

        # 3. Phân nhóm theo Mức (2D)
        all_possible_2d = {f"{i:02d}" for i in range(100)}
        level_groups = defaultdict(list)
        for num in all_possible_2d:
            freq = number_freq[num]
            level_groups[freq].append(num)
            
        # 4. HIỂN THỊ KẾT QUẢ TỔNG HỢP (ĐƯA LÊN ĐẦU ĐỂ DỄ NHÌN)
        self.level0_stats_text.insert(tk.END, f"🧩 TỔNG HỢP KẾT QUẢ ({selected_count} kỳ đã chọn):\n", 'title')
        if selected_cang:
            cang_type = "CÀNG 4D" if is_4d else "CÀNG 3D"
            self.level0_stats_text.insert(tk.END, f"⚠️ ĐANG GHÉP {cang_type}: {','.join(selected_cang)}\n", 'subtitle')
        self.level0_stats_text.insert(tk.END, f"------------------------------------------\n")
        
        # Helper function để format dàn số và ĐẾM SỐ LƯỢNG
        def format_and_count(nums_2d):
            if not selected_cang:
                return ",".join(sorted(nums_2d)), len(nums_2d), "2D"
            res_list = []
            for c in selected_cang:
                for n in sorted(nums_2d):
                    res_list.append(f"{c}{n}")
            return ",".join(res_list), len(res_list), ("4D" if is_4d else "3D")

        # Mức 0 (Loại)
        level0_nums = sorted(level_groups[0])
        txt0, cnt0, t0 = format_and_count(level0_nums)
        self.level0_stats_text.insert(tk.END, f"🚫 MỨC 0 - CÁC SỐ LOẠI ({cnt0} số {t0}):\n", 'title')
        self.level0_stats_text.insert(tk.END, f" (Sản phẩm của lọc Nhị hợp kết hợp Càng)\n", 'analysis')
        self.level0_stats_text.insert(tk.END, f"{txt0}\n\n", 'level0')
        
        # Các Mức 1, 2, 3...
        max_level = max(level_groups.keys()) if level_groups else 0
        for freq in range(1, max_level + 1):
            nums = sorted(level_groups[freq])
            if nums:
                txtf, cntf, tf = format_and_count(nums)
                self.level0_stats_text.insert(tk.END, f"📊 MỨC {freq} ({cntf} số {tf}):\n", 'subtitle')
                self.level0_stats_text.insert(tk.END, f"{txtf}\n\n", 'numbers')

        # 5. HIỂN THỊ CHI TIẾT CÁC KỲ
        self.level0_stats_text.insert(tk.END, f"------------------------------------------\n")
        self.level0_stats_text.insert(tk.END, "✅ CHI TIẾT CÁC KỲ ĐANG CHỌN ĐỂ TÍNH:\n", 'subtitle')
        
        for item in self.current_winning_rows:
            k = item['n_idx']
            if self.level0_check_vars.get(k, tk.BooleanVar(value=False)).get():
                # Hiển thị chi tiết kỳ đang chọn
                is_streak = item.get('streak', 1) > 1
                prefix = "🔥 [ĂN DÂY] " if is_streak else " 🔹 "
                self.level0_stats_text.insert(tk.END, f"{prefix}N{k} - Ngày {item['date']} (Trúng: {', '.join(item['hits'])}):\n", 'subtitle' if not is_streak else 'level0')
                self.level0_stats_text.insert(tk.END, f"    Dàn gốc ({len(item['combos'])}s): {', '.join(item['combos'])}\n\n", 'numbers')

    def update_manual_level0_preview(self):
        """Cập nhật ô copy Nhị hợp khi người dùng đang gõ."""
        digits_str = self.level0_manual_digits_var.get().strip()
        digits = sorted(list(set([d for d in digits_str if d.isdigit()])))
        if not digits:
            self.level0_manual_output_var.set("")
            return
        # Tạo dàn Nhị hợp
        combos_2d = sorted({"".join(p) for p in product(digits, repeat=2)})
        self.level0_manual_output_var.set(",".join(combos_2d))

    def add_manual_level0_row(self):
        """Tạo dàn Nhị hợp từ dãy số người dùng nhập thủ công."""
        digits_str = self.level0_manual_digits_var.get().strip()
        # Parse chuỗi phân cách dấu phẩy
        combos_2d = [c.strip() for c in combos_str.split(',') if c.strip()]
        name_str = f"Dàn {digits_str}"
        
        # Thêm vào danh sách hàng thủ công
        manual_id = f"M{int(datetime.now().timestamp())}"
        self.manual_level0_rows.append({
            'n_idx': manual_id, 'date': name_str, 'combos': combos_2d, 
            'hits': [], 'streak': 1, 'is_manual': True 
        })
        
        # Reset ô nhập (nhưng giữ lại ô copy cho người dùng copy)
        self.level0_manual_digits_var.set("")
        # Lưu dữ liệu preview tạm thời để người dùng copy xong mới mất (hoặc để đó luôn)
        self.level0_manual_output_var.set(combos_str) 
        
        self.render_level0_analysis()

    def remove_manual_level0_row(self, manual_id):
        """Xóa một dàn thủ công khỏi danh sách."""
        self.manual_level0_rows = [r for r in self.manual_level0_rows if r.get('n_idx') != manual_id]
        self.render_level0_analysis()

    def _sync_bc_vscroll(self, *args):
        self.bc_fixed_canvas.yview(*args)
    
    def _schedule_bc_update(self, rows):
        """Debounced update for BC/CD/DE analysis to prevent recursive calls."""
        if self._bc_update_pending:
            return  # Skip if update is already scheduled
        
        self._bc_update_pending = True
        # Schedule update after 10ms to batch multiple checkbox changes (faster response)
        self.root.after(10, lambda: self._execute_bc_update(rows))
    
    def _execute_bc_update(self, rows):
        """Execute the actual BC/CD/DE analysis update."""
        try:
            self.analyze_bc_statistics(rows)
        finally:
            self._bc_update_pending = False

    def render_bc_cd_de(self):
        """Build the combined BC-CD-DE selection list and trigger analysis."""
        region = self.region_var.get()
        source_type = self.source_var.get()
        backtest_offset = self.backtest_var.get()
        days_fetch = self.days_fetch_var.get()
        station = self.station_var.get() if region != "Miền Bắc" else "MB"
        
        # Performance: Skip full UI rebuild if settings haven't changed
        current_state = (region, source_type, backtest_offset, days_fetch, station)
        if hasattr(self, '_bc_last_render_state') and self._bc_last_render_state == current_state:
            if hasattr(self, 'current_bc_rows') and self.current_bc_rows:
                self.analyze_bc_statistics(self.current_bc_rows)
            return
        
        data_source = self.master_data if region == "Miền Bắc" else self.station_data
        if not data_source: return
        
        # Update last rendered state
        self._bc_last_render_state = current_state

        # Sync Khan labels
        for target, source in [(self.bc_khan_3d_label, getattr(self, 'matrix_khan_3d_label', None)),
                               (self.bc_khan_4d_label, getattr(self, 'matrix_khan_4d_label', None))]:
            if source: target.config(text=source.cget('text'))

        # Clear UI
        for w in self.bc_fixed_inner.winfo_children(): w.destroy()
        
        backtest_offset = self.backtest_var.get()
        data_slice = data_source[backtest_offset : backtest_offset + self.MAX_DISPLAY_DAYS]
        
        source_type = self.source_var.get()
        universal_source_by_date = {row.get('date', ''): (row.get('tt_number', '') if source_type == "Thần Tài" else "".join(row.get('dt_numbers', []))) 
                             for row in self.master_data}

        # Prep rows
        rows_data = []
        for row in data_slice:
            date_key = row.get('date')
            src_str = universal_source_by_date.get(date_key, '')
            digits = set(src_str)
            combos = sorted({a+b for a in digits for b in digits}) if digits else []
            
            date_entry = {'date': date_key, 'source': src_str if src_str else "N/A", 'combos': combos, 'items': []}
            
            if region != "Miền Bắc":
                st_list = row.get('items', []) if 'items' in row else [row]
                for it in st_list:
                    db = it.get('db','')
                    if db and len(db) >= 4:
                        st_name = it.get('station', self.station_var.get() if st_list==[row] else "")
                        st_short = "".join(w[:1].upper() for w in st_name.replace(".", " ").split() if w)
                        date_entry['items'].append({
                            'station': st_name, 'short': st_short,
                            'db': db, 'bc': db[-4:-2], 'cd': db[-3:-1], 'de': db[-2:]
                        })
            else:
                db = row.get('xsmb_full', '')
                if db and len(db) >= 4:
                    date_entry['items'].append({
                        'station': "MB", 'short': "",
                        'db': db, 'bc': db[-4:-2], 'cd': db[-3:-1], 'de': db[-2:]
                    })
            if date_entry['items']: rows_data.append(date_entry)
        
        self.current_bc_rows = rows_data

        # Headers
        header_frame = tk.Frame(self.bc_fixed_inner, bg="#333333")
        header_frame.pack(fill=tk.X)
        
        def _h(txt, w_px, color="#333333"):
            f = tk.Frame(header_frame, bg=color, width=w_px, height=25)
            f.pack(side=tk.LEFT, padx=1, pady=1)
            f.pack_propagate(False)
            tk.Label(f, text=txt, bg=color, fg="white", font=('Segoe UI', 9, 'bold'), anchor='center').pack(expand=True, fill='both')

        _h("STT", 32)
        _h("Ngày", 60)
        _h("GĐB", 80)
        _h("Nguồn", 100)
        if self.bc_show_result_var.get(): _h("Kết quả", 100, color="#1a5276")
        _h("BC", 32, color="#e74c3c")
        _h("CD", 32, color="#f39c12") 
        _h("DE", 32, color="#2ecc71")

        # --- [NEW] Pre-calculate Pending Status for Highlighting ---
        pending_status = {"BC": set(), "CD": set(), "DE": set()}
        positions = ["BC", "CD", "DE"]
        
        for pos in positions:
            for r_idx, r_data in enumerate(rows_data):
                combos = set(r_data['combos'])
                has_hit = False
                # Logic kiểm tra giống hệt analyze_statistics
                num_cols = min(r_idx + 1, self.MAX_MATRIX_COLS)
                for k in range(1, num_cols + 1):
                    # Kiểm tra chéo ngược lại quá khứ
                    target_idx = r_idx - k + 1
                    if target_idx >= 0 and target_idx < len(rows_data):
                        target_row = rows_data[target_idx]
                        for it in target_row['items']:
                            actual_pair = it.get(pos.lower(), "-")
                            if actual_pair in combos:
                                has_hit = True; break
                    if has_hit: break
                
                if not has_hit:
                    # Lưu lại Date Key của dòng chưa ra
                    pending_status[pos].add(r_data['date'])
        # -----------------------------------------------------------

        # Rows
        for r_idx, r_data in enumerate(rows_data):
            row_bg = "#2d2d2d" if r_idx % 2 == 0 else "#1e1e1e"
            date_key = r_data['date']
            combos_tuple = tuple(r_data['combos'])
            
            r_frame = tk.Frame(self.bc_fixed_inner, bg=row_bg)
            r_frame.pack(fill=tk.X)
            
            def _c(txt, w_px, fg="#bdc3c7", font_sz=9, bold=False):
                f = tk.Frame(r_frame, bg=row_bg, width=w_px, height=22 * len(r_data['items']))
                f.pack(side=tk.LEFT, padx=1, pady=1)
                f.pack_propagate(False)
                tk.Label(f, text=txt, bg=row_bg, fg=fg, font=('Segoe UI', font_sz, 'bold' if bold else 'normal'), anchor='center').pack(expand=True, fill='both')

            _c(str(len(rows_data)-r_idx), 32)
            _c(date_key[:5], 60, fg="#95a5a6")
            
            db_txt = "\n".join([f"{it['short']}:{it['db']}" if it['short'] else it['db'] for it in r_data['items']])
            _c(db_txt, 80, fg="white", font_sz=8)
            _c(r_data['source'], 100, fg="#f1c40f", bold=True)
            if self.bc_show_result_var.get():
                res_txt = "\n".join([f"{it['bc']}.{it['cd']}.{it['de']}" for it in r_data['items']])
                _c(res_txt, 100, fg="#5dade2", font_sz=8)

            # Checkboxes for BC, CD, DE (Modified with Highlighting)
            for t_type, var_dict, color in [("BC", self.manual_selected_bc_vars, "#e74c3c"), 
                                            ("CD", self.manual_selected_cd_vars, "#f1c40f"), 
                                            ("DE", self.manual_selected_vars, "#2ecc71")]:
                key = (date_key, combos_tuple)
                if key not in var_dict:
                    var_dict[key] = tk.BooleanVar(value=False)
                    # Add trace only once when creating the variable
                    var_dict[key].trace_add('write', lambda *a, rows=rows_data: self._schedule_bc_update(rows))
                
                # --- LOGIC TÔ MÀU ---
                is_pending = date_key in pending_status[t_type]
                
                # Màu mặc định (xen kẽ) hoặc Màu nổi bật (Cam đậm) nếu chưa ra
                # #d35400 là màu cam đất, nổi trên nền tối nhưng không chói
                cell_bg = "#d35400" if is_pending else row_bg 
                
                cb_frame = tk.Frame(r_frame, bg=cell_bg, width=32, height=22 * len(r_data['items']))
                cb_frame.pack(side=tk.LEFT, padx=1, pady=1)
                cb_frame.pack_propagate(False)
                # Make checkbox more visible with white foreground and proper state
                cb = tk.Checkbutton(cb_frame, variable=var_dict[key], 
                                   bg=cell_bg, fg="white", selectcolor=cell_bg, # selectcolor trùng bg để đẹp hơn
                                   activebackground=cell_bg, activeforeground="white",
                                   highlightthickness=0, bd=0, 
                                   indicatoron=True, cursor="hand2",
                                   command=lambda: self._schedule_bc_update(rows_data))
                cb.pack(expand=True)

        self.bc_fixed_canvas.config(scrollregion=self.bc_fixed_canvas.bbox("all"))
        self.analyze_bc_statistics(rows_data)


    def analyze_bc_statistics(self, all_rows):
        """Analyze statistics 1:1 Matrix logic for BC-CD-DE (Redesigned with Horizontal Pendings)."""
        # Guard against multiple simultaneous calls
        if getattr(self, '_is_analyzing_bc', False):
            return
        
        self._is_analyzing_bc = True
        try:
            # CRITICAL: Clear widget first to prevent duplicate results
            self.bc_stats_text.delete('1.0', tk.END)
            
            if not all_rows: 
                return
            
            show_nums = self.bc_show_sidebar_nums.get()

            # 1. SELECTION SUMMARY (DÀN ĐANG CHỌN) - Moved to Top
            selected_summaries = {} # date -> {"bc": bool, "cd": bool, "de": bool, "combos": []}
            valid_dates = {r['date'] for r in all_rows}
            
            for date, combo in self.manual_selected_bc_vars:
                if date in valid_dates and self.manual_selected_bc_vars[(date, combo)].get():
                    if date not in selected_summaries: selected_summaries[date] = {"bc": True, "cd": False, "de": False, "combos": combo}
                    else: selected_summaries[date]["bc"] = True
            
            for date, combo in self.manual_selected_cd_vars:
                if date in valid_dates and self.manual_selected_cd_vars[(date, combo)].get():
                    if date not in selected_summaries: selected_summaries[date] = {"bc": False, "cd": True, "de": False, "combos": combo}
                    else: selected_summaries[date]["cd"] = True

            for date, combo in self.manual_selected_vars:
                if date in valid_dates and self.manual_selected_vars[(date, combo)].get():
                    if date not in selected_summaries: selected_summaries[date] = {"bc": False, "cd": False, "de": True, "combos": combo}
                    else: selected_summaries[date]["de"] = True

            if selected_summaries:
                self.bc_stats_text.insert(tk.END, f"🎯 DÀN ĐANG CHỌN ({len(selected_summaries)} ngày):\n", 'title')
                sorted_dates = sorted(selected_summaries.keys(), key=lambda x: (x.split('/')[-1], x.split('/')[1], x.split('/')[0]), reverse=True)
                for idx, date in enumerate(sorted_dates, 1):
                    info = selected_summaries[date]
                    lbls = [p for p in ["BC", "CD", "DE"] if info[p.lower()]]
                    self.bc_stats_text.insert(tk.END, f" {idx}. {date} ({'+'.join(lbls)}) ({len(info['combos'])}s):\n", 'subtitle')
                    if show_nums:
                        self.bc_stats_text.insert(tk.END, f"   {', '.join(sorted(info['combos']))}\n\n", 'numbers')
                    else:
                        self.bc_stats_text.insert(tk.END, "\n")
                self.bc_stats_text.insert(tk.END, "—"*40 + "\n\n")

            # 💡 CHÚ THÍCH: Ô màu CAM bên trái là dàn CHƯA RA.
            self.bc_stats_text.insert(tk.END, "💡 CHÚ THÍCH: Ô màu CAM bên trái là dàn CHƯA RA.\n", 'analysis')
            self.bc_stats_text.insert(tk.END, "—"*40 + "\n\n")

            # 3. TRIGGER JOIN LOGIC
            self._on_bc_selection_changed_internal()
        finally:
            self._is_analyzing_bc = False


    def _on_bc_selection_changed_internal(self, *args):
        """Unified Join Logic for BC-CD-DE (High Performance + Pattern Expansion + Threading)."""
        valid_rows = getattr(self, 'current_bc_rows', [])
        if not valid_rows:
            return

        # Prevent multiple calculation threads
        if getattr(self, '_is_calculating_bc_join', False):
            return
        self._is_calculating_bc_join = True

        # Show loading indicator in side text
        self.bc_stats_text.insert(tk.END, "⏳ ĐANG GHÉP DÀN & TÍNH TOÁN MỨC... VUI LÒNG ĐỢI...\n", 'analysis')
        self.bc_stats_text.see(tk.END)

        def run_join_threading():
            try:
                valid_dates = {r['date'] for r in valid_rows}
                source_lookup = {r['date']: r['combos'] for r in valid_rows}

                total_2d_counter = Counter()
                total_3d_counter = Counter()
                total_4d_counter = Counter()
                
                act_bc = self.bc_pool_var.get()
                act_cd = self.cd_pool_var.get()
                act_de = self.de_pool_var.get()

                selected_map = {}
                for date in valid_dates:
                    combos = tuple(source_lookup.get(date, []))
                    if not combos: continue
                    key = (date, combos)
                    has_bc = act_bc and self.manual_selected_bc_vars.get(key, tk.BooleanVar(value=False)).get()
                    has_cd = act_cd and self.manual_selected_cd_vars.get(key, tk.BooleanVar(value=False)).get()
                    has_de = act_de and self.manual_selected_vars.get(key, tk.BooleanVar(value=False)).get()
                    if has_bc or has_cd or has_de:
                        selected_map[date] = (has_bc, has_cd, has_de, list(combos))

                for date, (has_bc, has_cd, has_de, combos) in selected_map.items():
                    if has_bc:
                        r3, r4 = set(), set()
                        for bc in combos:
                            for d in "0123456789": r3.add(bc + d)
                            for i in range(100): r4.add(bc + f"{i:02d}")
                        total_3d_counter.update(r3); total_4d_counter.update(r4)
                    
                    if has_cd:
                        r3, r4 = set(), set()
                        for cd in combos:
                            for d in "0123456789": 
                                r3.add(cd + d)
                                r3.add(d + cd)
                            for i in range(100): 
                                r4.add(f"{i//10}" + cd + f"{i%10}")
                        total_3d_counter.update(r3); total_4d_counter.update(r4)
                    
                    if has_de:
                        r2, r3, r4 = set(), set(), set()
                        for de in combos:
                            r2.add(de)
                            for d in "0123456789": r3.add(d + de)
                            for i in range(100): r4.add(f"{i:02d}" + de)
                        total_2d_counter.update(r2); total_3d_counter.update(r3); total_4d_counter.update(r4)

                lvl_data = defaultdict(lambda: {'2d': set(), '3d': set(), '4d': set()})
                all_lvls = {0}
                for num, freq in total_2d_counter.items():
                    lvl_data[freq]['2d'].add(num); all_lvls.add(freq)
                for num, freq in total_3d_counter.items():
                    lvl_data[freq]['3d'].add(num); all_lvls.add(freq)
                for num, freq in total_4d_counter.items():
                    lvl_data[freq]['4d'].add(num); all_lvls.add(freq)
                
                max_lvl_total = max(all_lvls)
                self.root.after(0, lambda: self._update_bc_join_results_ui(lvl_data, max_lvl_total))
            except Exception as e:
                print(f"Error in BC join thread: {e}")
            finally:
                self._is_calculating_bc_join = False

        threading.Thread(target=run_join_threading, daemon=True).start()

    def _update_bc_join_results_ui(self, lvl_data, max_lvl_total):
        """Final UI update for joined results."""
        if max_lvl_total > 0:
            self.bc_stats_text.insert(tk.END, "💎 KẾT QUẢ GHÉP DÀN (Chỉ số chọn):\n", 'title')
            for sect, key, lab in [("🔥 Dàn 4 Càng (4D):", '4d', "4D"), 
                                   ("⭐ Dàn 3 Càng (3D):", '3d', "3D"), 
                                   ("🍀 Dàn 2 Càng (Nhị Hợp):", '2d', "2D")]:
                
                has_any = any(lvl_data[l][key] for l in range(max_lvl_total, 0, -1))
                if has_any:
                    self.bc_stats_text.insert(tk.END, f"\n{sect}\n", 'subtitle')
                    displayed_lvls = 0
                    for l in range(max_lvl_total, 0, -1):
                        nums = sorted(lvl_data[l][key])
                        if nums:
                            self.bc_stats_text.insert(tk.END, f" ⚡ MỨC {l}:\n", 'level')
                            self.bc_stats_text.insert(tk.END, f"   {lab} ({len(nums)} số): {', '.join(nums)}\n\n", 'numbers')
                            displayed_lvls += 1
                            if displayed_lvls >= 15: 
                                self.bc_stats_text.insert(tk.END, "... (Còn tiếp các mức thấp hơn)\n", 'analysis')
                                break
        self.bc_stats_text.see(tk.END)

    def draw_bc_hit_chart(self, hit_counts):
        """Draw a professional bar chart on self.bc_hit_chart_canvas showing hit counts."""
        canvas = self.bc_hit_chart_canvas
        if not canvas: return
        canvas.delete("all")
        w, h = canvas.winfo_width(), canvas.winfo_height()
        if w < 10: w, h = 300, 100
        
        padding_l, padding_r, padding_t, padding_b = 25, 10, 10, 20
        chart_w, chart_h = w - padding_l - padding_r, h - padding_t - padding_b
        max_hits = max(hit_counts.values()) if hit_counts.values() else 1
        if max_hits == 0: max_hits = 1
        
        num_bars = self.MAX_MATRIX_COLS
        bar_w = (chart_w / num_bars) * 0.8
        gap = (chart_w / num_bars) * 0.2
        
        def get_bar_color(count, max_c):
            if count >= max_c * 0.8: return "#27ae60"
            if count >= max_c * 0.5: return "#f1c40f"
            return "#3498db"
            
        canvas.create_line(padding_l, padding_t, padding_l, h - padding_b, fill="#555")
        canvas.create_line(padding_l, h - padding_b, w - padding_r, h - padding_b, fill="#555")

        for k in range(1, num_bars + 1):
            count = hit_counts.get(k, 0)
            bar_h = (count / max_hits) * chart_h
            x1 = padding_l + (k-1) * (bar_w + gap) + gap/2
            y1 = h - padding_b - bar_h
            x2 = x1 + bar_w
            y2 = h - padding_b
            color = get_bar_color(count, max_hits)
            canvas.create_rectangle(x1, y1, x2, y2, fill=color, outline="")
            if k == 1 or k == num_bars or k % 5 == 0:
                canvas.create_text((x1 + x2)/2, h - padding_b + 8, text=f"N{k}", fill="#bdc3c7", font=('Segoe UI', 6))
            if count > 0:
                canvas.create_text((x1 + x2)/2, y1 - 5, text=str(count), fill="white", font=('Segoe UI', 6))

    def _render_mapping_results(self, pools):
        """Deprecated in favor of combined join logic in _on_bc_selection_changed_internal."""
        pass


    

    # --- MATRIX TOOLTIP HANDLERS ---
    def _on_matrix_mouse_move(self, event, pos_type):
        """Coordinate-based tooltip for the Matrix Canvas."""
        canvas = event.widget
        # Local coordinates to Canvas scrollable coordinates
        cx = canvas.canvasx(event.x)
        cy = canvas.canvasy(event.y)
        
        # Constants from render_matrix_unified
        ROW_H, Header_H = 25, 26
        COL_W_STT, COL_W_DATE, COL_W_SRC = 30, 50, 100
        
        config = self.matrix_configs.get(pos_type)
        if not config or cy < Header_H:
            self._on_matrix_mouse_leave(None)
            return
            
        # Calculate Row idx
        row_idx = int((cy - Header_H) // ROW_H)
        data = getattr(self, f"matrix_{pos_type.lower()}_days_data", [])
        
        if not (0 <= row_idx < len(data)):
            self._on_matrix_mouse_leave(None)
            return
            
        day_data = data[row_idx]
        date = day_data['date']
        
        # Determine Column
        show_result = config["show_source_var"].get() if config["show_source_var"] else False
        col_w_de = 35 if config["show_de_sync"] else 0
        col_w_res = 160 if show_result else 0
        col_w_n = 30
        
        # Start of N columns
        n_start_x = COL_W_STT + COL_W_DATE + COL_W_SRC + col_w_de + col_w_res
        
        tip_text = ""
        if cx < n_start_x:
            # Hovering on STT, Date, Source, or Result
            if cx < COL_W_STT: tip_text = f"STT: {len(data)-row_idx}"
            elif cx < COL_W_STT + COL_W_DATE: tip_text = f"Ngày: {date}"
            elif cx < COL_W_STT + COL_W_DATE + COL_W_SRC: tip_text = f"Nguồn: {day_data['source']}"
            else: tip_text = f"Kết quả: {', '.join(day_data['check_result_full'])}"
        else:
            # Hovering on Matrix cells (N1..N28)
            k = int((cx - n_start_x) // col_w_n) + 1
            if 1 <= k <= self.MAX_MATRIX_COLS:
                target_idx = row_idx - k + 1
                if 0 <= target_idx < len(data):
                    target_date = data[target_idx]['date']
                    res_list = data[target_idx]['check_result']
                    combos_set = set(day_data['combos'])
                    hits = [v for v in res_list if v and v in combos_set]
                    
                    hit_info = f"({', '.join(hits)})" if hits else "—"
                    tip_text = f"N{k}: Đối soát ngày {target_date}\nKết quả: {hit_info}"
                else:
                    tip_text = f"N{k}: (Vùng rỗng)"
        
        if tip_text:
            self.matrix_tooltip.config(text=tip_text)
            # Position tooltip near mouse but slightly offset
            # Need global coordinates
            gx = self.root.winfo_pointerx() + 15
            gy = self.root.winfo_pointery() + 15
            self.matrix_tooltip.place(x=gx - self.root.winfo_rootx(), y=gy - self.root.winfo_rooty())
            self.matrix_tooltip.lift()
        else:
            self._on_matrix_mouse_leave(None)

    def _on_matrix_mouse_leave(self, event):
        """Hide the Matrix tooltip."""
        self.matrix_tooltip.place_forget()

def isStraightMod10AnyOrder(d):
    if not isinstance(d, list) or len(d) != 5: return False
    s = set(d)
    if len(s) != 5: return False
    for b in range(10):
        ok = True
        for i in range(5):
            if (b + i) % 10 not in s:
                ok = False
                break
        if ok: return True
    return False

def classifyXiTo(d):
    if not isinstance(d, list) or len(d) != 5: return "—"
    cnt = Counter(d)
    counts = sorted(cnt.values(), reverse=True)
    if counts[0] == 5: return "N" # Ngũ quý
    if counts[0] == 4: return "T" # Tứ quý
    if counts[0] == 3 and counts[1] == 2: return "C" # Cù lũ
    if isStraightMod10AnyOrder(d): return "S" # Sảnh
    if counts[0] == 3: return "3" # Sám
    if counts[0] == 2 and counts[1] == 2: return "2" # Thú
    if counts[0] == 2: return "1" # Đôi
    return "R" # Rác

def classifyNgau(d):
    if not isinstance(d, list) or len(d) != 5: return "—"
    if len(set(d)) == 1:
        return "H" if d[0] == 0 else "K"
    
    best = -1
    found = False
    combs = [(0,1,2), (0,1,3), (0,1,4), (0,2,3), (0,2,4), (0,3,4), (1,2,3), (1,2,4), (1,3,4), (2,3,4)]
    for a, b, c in combs:
        if (d[a] + d[b] + d[c]) % 10 != 0: continue
        found = True
        rem = [i for i in range(5) if i not in (a, b, c)]
        score = (d[rem[0]] + d[rem[1]]) % 10
        if score == 0: return "H"
        if score > best: best = score
    if not found: return "K"
    return str(best)




def main():
    root = tk.Tk()
    app = SieuGaApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()
