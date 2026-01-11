import streamlit as st
import requests
import json
import logging
import urllib3
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from src.constants import HEADERS, DAI_API, API_MIRRORS

# Suppress InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if 'current_domain' not in st.session_state:
    st.session_state.current_domain = API_MIRRORS[0]

def rotate_api_domain():
    try:
        idx = API_MIRRORS.index(st.session_state.current_domain)
        st.session_state.current_domain = API_MIRRORS[(idx + 1) % len(API_MIRRORS)]
        logging.info(f"Switched API mirror to: {st.session_state.current_domain}")
    except: pass

def get_mirrored_url(url):
    for dom in API_MIRRORS:
        if dom in url:
            return url.replace(dom, st.session_state.current_domain)
    return url

@st.cache_data(ttl=3600)
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
    if not soup: return data
    
    try:
        divs = soup.find_all("div", class_="result_div", id="result_123")
        for div in divs[:total_days]:
            ds = div.find("span", id="result_date")
            date_raw = ds.text.strip() if ds else ""
            if not date_raw: continue
            if "ngày" in date_raw:
                date_raw = date_raw.split("ngày")[-1].strip()
            date = date_raw.replace("-", "/")
            
            tbl = div.find("table", id="result_tab_123")
            if tbl:
                tbody = tbl.find("tbody")
                row = tbody.find("tr") if tbody else None
                cells = row.find_all("td") if row else []
                if len(cells) == 3:
                    nums = [c.text.strip() for c in cells]
                    if all(n.isdigit() for n in nums):
                        data.append({"date": date, "dt_numbers": nums})
    except Exception as e:
        logging.warning(f"Error parsing Điện Toán data: {e}")
    return data

@st.cache_data(ttl=3600)
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
    if not soup: return data
    
    try:
        divs = soup.find_all("div", class_="result_div", id="result_tt4")
        for div in divs[:total_days]:
            ds = div.find("span", id="result_date")
            date_raw = ds.text.strip() if ds else ""
            if not date_raw: continue
            if "ngày" in date_raw:
                date_raw = date_raw.split("ngày")[-1].strip()
            date = date_raw.replace("-", "/")
            
            tbl = div.find("table", id="result_tab_tt4")
            if tbl:
                cell = tbl.find("td", id="rs_0_0")
                num = cell.text.strip() if cell else ""
                if num.isdigit() and len(num) == 4:
                    data.append({"date": date, "tt_number": num})
    except Exception as e:
        logging.warning(f"Error parsing Thần Tài data: {e}")
    return data

@st.cache_data(ttl=3600)
def fetch_xsmb_full(total_days: int) -> List[Dict]:
    """Fetch full MB data from API with local mirror fallback."""
    base_url = f"https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum={total_days}&gameCode=miba"
    for _ in range(len(API_MIRRORS)):
        url = get_mirrored_url(base_url)
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            data = r.json()
            if not data.get("success"):
                rotate_api_domain(); continue
            
            issue_list = data.get("t", {}).get("issueList", [])
            results = []
            for issue in issue_list:
                try:
                    detail_str = issue.get("detail", "")
                    prizes = json.loads(detail_str)
                    results.append({
                        "date": issue.get("turnNum", ""),
                        "xsmb_full": prizes[0] if len(prizes) > 0 else "",
                        "all_prizes": prizes
                    })
                except: continue
            return results
        except:
            rotate_api_domain(); continue
    return []

@st.cache_data(ttl=3600)
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
                rotate_api_domain(); continue
            
            issue_list = data.get("t", {}).get("issueList", [])
            results = []
            for issue in issue_list[:total_days]:
                try:
                    detail_str = issue.get("detail", "")
                    prizes = json.loads(detail_str)
                    results.append({
                        "date": issue.get("turnNum", ""),
                        "db": prizes[0] if len(prizes) > 0 else "",
                        "all_prizes": prizes
                    })
                except: continue
            return results
        except:
            rotate_api_domain(); continue
    return []
