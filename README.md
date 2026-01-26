# Hệ thống Pipeline Dữ liệu Bất động sản Việt Nam

## Tổng quan

Dự án này là một **pipeline dữ liệu**, được thiết kế để **thu thập (scrape), làm sạch, chuẩn hóa và lưu trữ** dữ liệu tin đăng bất động sản từ các nền tảng lớn tại Việt Nam (**Batdongsan** và **Onehousing**).

Hệ thống sử dụng **Selenium** (thông qua `seleniumbase` để né bot detection) cho việc scrape, **Pandas** cho xử lý dữ liệu chuyên sâu, **SQLite** để lưu trữ và **FastAPI** để xây dựng giao diện quản lý web. Pipeline có tích hợp **lập lịch tự động** và **circuit breaker** để xử lý lỗi.

---

## Tính năng chính

* **Hỗ trợ đa website:** Hiện hỗ trợ Batdongsan.com.vn và Onehousing.vn.
* **Scraping ẩn danh (Stealth):** Sử dụng chế độ UC (Undetected ChromeDriver) của `seleniumbase` để vượt qua cơ chế phát hiện bot.
* **Tự động hóa:**

  * **Quản lý trạng thái:** Theo dõi tiến trình trong `pipeline_state.json`. Có thể tiếp tục chạy lại chính xác từ vị trí đã dừng.
  * **Circuit Breaker:** Tự động tạm dừng scraping khi xảy ra lỗi nghiêm trọng (mạng/bộ nhớ) hoặc nhiều lần thất bại liên tiếp để tránh bị chặn IP.
  * **Đặt lịch tự động:** Tích hợp **APScheduler** để chạy pipeline hàng tuần (thứ Sáu lúc 21:00) và tự động phục hồi khi crash.
* **Làm sạch dữ liệu nâng cao:**

  * **Chuẩn hóa địa chỉ:** Ánh xạ địa chỉ thô sang đơn vị hành chính chính thức (Tỉnh/Huyện/Xã/Đường) bằng các bảng tra cứu SQL.
  * **Feature Engineering:** Trích xuất chiều ngang mặt tiền, độ rộng hẻm, hình dạng đất, tình trạng pháp lý bằng Regex và fuzzy matching.
  * **Điền dữ liệu còn thiếu:** Tính toán các kích thước bị thiếu dựa trên diện tích và tỷ lệ hình dạng.
* **Giao diện Web:** Dashboard FastAPI để truy vấn dữ liệu lịch sử, kiểm tra trạng thái hệ thống và tải báo cáo Excel.

---

## Cấu trúc dự án

```text
├── Batdongsan/               # Logic riêng cho Batdongsan
│   ├── orchestrator.py       # Điều phối luồng xử lý
│   ├── selenium_manager.py   # Quản lý worker & khởi tạo driver
│   ├── cleaning.py           # Logic Regex & chuẩn hóa
│   └── ...
├── Onehousing/               # Logic riêng cho Onehousing
│   ├── fetch_listings.py     # Logic parse dữ liệu
│   └── ...
├── commons/                  # Tiện ích dùng chung
│   ├── config.py             # Cấu hình toàn cục (Path, URL, Ngưỡng)
│   ├── state_manager.py      # Circuit Breaker & Resume
│   ├── writers.py            # Ghi/đọc CSV
│   └── retry.py              # Decorator retry
├── database/                 # Tầng database
│   ├── database_manager.py   # Tương tác SQLite
│   └── schema.py             # Định nghĩa bảng SQL
├── Dữ liệu địa giới hành chính/ # BẮT BUỘC: SQL dump cho chuẩn hóa địa chỉ
├── output/                   # CSV, Log và SQLite DB sinh ra
├── templates/                # Template HTML Jinja2 cho UI
├── main.py                   # Entry point CLI
├── ui.py                     # Entry point Web Server & Scheduler
└── address_standardizer.py   # Logic match địa chỉ cốt lõi
```

---

## Yêu cầu hệ thống

### Yêu cầu phần mềm

* **Python:** 3.10+
* **Trình duyệt:** Google Chrome (bắt buộc cho Selenium).
* **Hệ điều hành:** Windows / Linux / MacOS

  * *Lưu ý cho Linux Server:* Có thể cần `Xvfb` nếu chạy headless, tuy nhiên `seleniumbase` hỗ trợ headless khá tốt.

### Dữ liệu phụ thuộc bên ngoài

Class `AddressStandardizer` yêu cầu các file SQL dump. Đảm bảo thư mục `Dữ liệu địa giới hành chính` có:

* `provinces_*.sql`
* `districts_*.sql`
* `wards_*.sql`
* `streets_*.sql`

---

## Cài đặt

1. **Clone repository:**

```bash
git clone <repo_url>
cd <repo_name>
```

2. **Cài dependencies:**

```bash
pip install -r requirements.txt
```

3. **Kiểm tra cấu hình:**
   Mở `commons/config.py`. Đảm bảo `MAX_WORKERS` phù hợp với CPU (mặc định: 2).

---

## Sử dụng: Command Line (CLI)

Dùng `main.py` để chạy thủ công hoặc test.

### 1. Chạy toàn bộ pipeline (scrape + clean):

```bash
python main.py --mode full
```

### 2. Tiếp tục job bị crash/dừng:

Đọc `output/pipeline_state.json` và bỏ qua các trang đã scrape.

```bash
python main.py --mode full --resume
```

### 3. Chỉ chạy bước làm sạch dữ liệu:

Giả sử CSV thô đã tồn tại trong `output/`.

```bash
python main.py --mode clean
```

---

## Sử dụng: Web Server & Tự động hóa (Host)

Khi deploy, chạy file `ui.py`. File này khởi động FastAPI server và Background Scheduler.

### Khởi động server:

```bash
uvicorn ui:app --host 0.0.0.0 --port 8000 --reload
```

### Tính năng Dashboard

1. **Trang chủ (`/`):** Xem trạng thái hệ thống (Idle/Running) và tải dữ liệu theo khoảng ngày.
2. **API trạng thái (`/system-status`):** Trả về JSON cho biết scraper có đang chạy hay không.
3. **Lập lịch tự động:**

   * **Chạy hàng tuần:** Tự động scrape vào **21:00 thứ Sáu (Asia/Ho_Chi_Minh)**.
   * **Tự phục hồi:** Nếu server restart khi job đang chạy, hệ thống sẽ phát hiện trạng thái “suspended” và tự động resume.

---

## Kiến trúc kỹ thuật

### 1. Pipeline Scraping

Pipeline được chia thành 2 phase để đảm bảo tính toàn vẹn dữ liệu:

* **Phase 1 (URLs):** Duyệt phân trang tìm kiếm để thu thập URL tin đăng → `listing_urls.csv`.
* **Phase 2 (Details):** Đọc URL và scrape chi tiết theo mô hình producer–consumer đa luồng → `listing_details.csv`.

  * *Circuit Breaker:* Nếu gặp `ConnectionRefused` hoặc `MemoryError`, hệ thống sẽ mở breaker, lưu state và dừng an toàn để resume sau.

### 2. Làm sạch & Chuẩn hóa dữ liệu

Dữ liệu thô rất nhiễu, pipeline áp dụng nhiều bước biến đổi:

* **OneHousing:** Làm sạch bằng `OneHousingDataCleaner`.
* **Batdongsan:** Làm sạch bằng `DataCleaner` và `AddressStandardizer`.

  * **Logic địa chỉ:** Phân tách địa chỉ. Nếu thiếu “Phường X” nhưng có “Đường Y”, hệ thống sẽ truy vấn DB hành chính để tìm phường chứa đường đó.

### 3. Database Schema

Dữ liệu được lưu tại `output/real_estate.db`:

* `bds_raw`: HTML/JSON thô từ Batdongsan.
* `onehousing_raw`: Dữ liệu thô từ Onehousing.
* `cleaned`: Bảng “Gold” đã chuẩn hóa, dùng cho export.

---

## Hướng dẫn triển khai (Host)

Để chạy pipeline liên tục trên Linux server:

1. **Cài Google Chrome:**

```bash
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install ./google-chrome-stable_current_amd64.deb
```

2. **Tạo service Systemd (Khuyến nghị):**

Tạo file `/etc/systemd/system/realestate_scraper.service`:

```ini
[Unit]
Description=Real Estate Scraper API & Scheduler
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/path/to/repo
ExecStart=/path/to/venv/bin/uvicorn ui:app --host 0.0.0.0 --port 8000
Restart=always

[Install]
WantedBy=multi-user.target
```

3. **Khởi động service:**

```bash
sudo systemctl enable realestate_scraper
sudo systemctl start realestate_scraper
```
