import logging

# ====================================
# API & NETWORKING
# ====================================
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

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

API_MIRRORS = ["www.kqxs88.live", "www.kqxs88.net", "www.kqxs88.top", "www.kqxs88.vip", "www.kqxs88.info"]

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

# ====================================
# NUMBER LOGIC & DICTIONARIES
# ====================================
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
# K.KHONG logic should ideally be computed or added here
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

# ====================================
# PRE-COMPUTED PATTERNS
# ====================================
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
_all_kep_3d = set()
for _v in KEP_PATTERNS_3D.values(): _all_kep_3d.update(_v)
KEP_PATTERNS_3D["CON_LAI"] = [f"{_i:03d}" for _i in range(1000) if f"{_i:03d}" not in _all_kep_3d]
