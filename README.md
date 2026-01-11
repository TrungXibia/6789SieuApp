# 🦅 SieuGa Web - Comprehensive Lottery Analysis

Ứng dụng phân tích xổ số chuyên nghiệp được xây dựng trên nền tảng **Streamlit Web**, mang phong cách **Cyber Dark** hiện đại và mạnh mẽ. 

Đây là phiên bản "Toàn diện di trú" từ ứng dụng Desktop Tkinter cũ, tối ưu hóa cho hiệu suất web và trải nghiệm người dùng di động/PC.

## ✨ Tính năng nổi bật
- **⚡ Performance**: Sử dụng `@st.cache_data` và `st.session_state` để tải dữ liệu API cực nhanh.
- **🎯 Bảng Matrix Chuyên nghiệp**: 
  - Đối soát N1-N28 với logic Hit/Miss thời gian thực.
  - Tự động tô màu **Xanh (Trúng)** và **Cam (Dàn chưa ra)** bằng Pandas Styler.
- **📊 Tần suất Rolling 7**: Thống kê tần suất Chạm và Cặp theo nhịp gối đầu 7 ngày.
- **📈 Bệt Chạm & Gợi ý**: Phân tích mức số và gợi ý dàn dựa trên tần suất Hot.
- **🎨 Giao diện Cyber Dark**: Dark Mode cưỡng bức với thiết kế sắc nét (Onyx + Emerald).

## 📂 Cấu trúc dự án
- `app.py`: Giao diện chính và điều hướng Tabs.
- `src/constants.py`: Từ điển bộ số và cấu hình API.
- `src/scraper.py`: Logic lấy dữ liệu đa nguồn (XSMB, MN, MT, ĐT, TT).
- `src/processor.py`: "Bộ não" tính toán Matrix và Tần suất.

## � Cài đặt & Chạy Local

1. **Cài đặt thư viện**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Chạy ứng dụng**:
   ```bash
   streamlit run app.py
   ```

## 🌐 Triển khai GitHub & Streamlit Cloud (Miễn phí)

### BƯỚC 1: Đẩy code lên GitHub
```bash
git init
git add .
git commit -m "Initial commit: SieuGa Web Migration"
git remote add origin https://github.com/YOUR_USERNAME/SieuGaWeb.git
git branch -M main
git push -u origin main
```

### BƯỚC 2: Host trên Streamlit Cloud
1. Truy cập [share.streamlit.io](https://share.streamlit.io).
2. Kết nối với tài khoản GitHub của bạn.
3. Chọn repo `SieuGaWeb` và file `app.py`.
4. Nhấn **Deploy** để nhận link Web chia sẻ cho mọi người!
