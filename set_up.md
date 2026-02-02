Dưới đây là hướng dẫn chi tiết từng bước để thiết lập và chạy server.

Máy chủ (host server) cần được cài đặt sẵn **Python (3.10 trở lên)**, **Git**, **Git Bash** và **Ngrok**.

---

# Hướng dẫn Setup và Deploy Server

## 1. Clone Repository (Tải code nguồn)

Đầu tiên, ta cần tải code nguồn dự án từ GitHub về máy chủ.

Mở Git Bash và chạy lệnh:

```bash
git clone https://github.com/PhamMaiLinh-2403/estates-app.git
```

Sau đó, mở Terminal (hoặc Command Prompt/PowerShell) và di chuyển vào thư mục dự án vừa clone:

```terminal
# Di chuyển vào thư mục dự án vừa clone
cd estates-app
```

## 2. Thiết lập Môi trường ảo (Virtual Environment)

Để tránh xung đột thư viện với hệ thống, chúng ta cần tạo một môi trường ảo Python.

**Chạy lệnh sau để tạo môi trường ảo (tên là `venv`):**

```terminal
python -m venv venv
```

## 3. Kích hoạt Môi trường ảo và Cài đặt thư viện

Ta cần kích hoạt môi trường ảo trước khi cài đặt các thư viện cần thiết.

**Đối với Windows:**
```terminal 
.\venv\Scripts\activate
```

*(Sau khi chạy, bạn sẽ thấy chữ `(venv)` xuất hiện ở đầu dòng lệnh).*

**Cài đặt các thư viện phụ thuộc:**
Dựa trên mã nguồn bạn cung cấp, chạy lệnh sau để cài đặt tất cả các thư viện cần thiết:

```bash
pip install --upgrade pip
pip install pandas numpy rapidfuzz selenium seleniumbase filelock apscheduler fastapi uvicorn jinja2 python-multipart requests beautifulsoup4 fake-useragent openpyxl
```

**Cài đặt Driver cho SeleniumBase (Quan trọng):**
Dự án sử dụng `seleniumbase` để vượt qua các lớp bảo mật, bạn cần cài đặt driver trình duyệt:
```bash
sbase install chromedriver
```

## 4. Cấu hình Ngrok Auth Token

Để public server ra internet thông qua Ngrok, bạn cần xác thực tài khoản.

1.  Đăng nhập vào [dashboard.ngrok.com](https://dashboard.ngrok.com).
2.  Lấy **Authtoken** của bạn.
3.  Chạy lệnh sau trong terminal:

```bash
# Thay thế <TOKEN_CUA_BAN> bằng mã token lấy từ web ngrok
ngrok config add-authtoken <TOKEN_CUA_BAN>
```

## 5. Khởi chạy Ngrok (Terminal 1)

Bạn cần mở một cổng kết nối (Tunnel) từ internet vào cổng 8000 của máy chủ.

**Lưu ý:** Hãy mở một cửa sổ Terminal mới (hoặc giữ cửa sổ hiện tại và dùng `screen`/`tmux` nếu dùng Linux server) để chạy lệnh này, vì nó cần chạy liên tục.

```bash
ngrok http 8000
```

Sau khi chạy, màn hình sẽ hiện ra địa chỉ Forwarding (ví dụ: `https://a1b2-c3d4.ngrok-free.app`). **Hãy copy địa chỉ này**, đây là đường dẫn để truy cập vào Web UI của bạn từ xa.

## 6. Khởi chạy ứng dụng UI (Terminal 2)

Quay lại cửa sổ Terminal nơi bạn đã **activate môi trường ảo** (bước 3). Chúng ta sẽ chạy ứng dụng FastAPI (`ui.py`).

Vì `ui.py` sử dụng thư viện `FastAPI`, cách tốt nhất để chạy production là dùng `uvicorn`.

Chạy lệnh sau:

```bash
# Chạy app trên host 0.0.0.0 để ngrok có thể forward, cổng 8000
uvicorn ui:app --host 0.0.0.0 --port 8000
```

*(Nếu bạn muốn server tự động reload khi sửa code, thêm cờ `--reload` vào cuối lệnh).*

---

### Tổng kết các lệnh (Cheat Sheet)

Bạn sẽ cần duy trì 2 cửa sổ Terminal:

**Terminal 1 (Ngrok):**
```bash
ngrok http 8000
```

**Terminal 2 (App Python):**
```bash
# Windows
.\venv\Scripts\activate
uvicorn ui:app --host 0.0.0.0 --port 8000

# Linux/Mac
source venv/bin/activate
uvicorn ui:app --host 0.0.0.0 --port 8000
```

Bây giờ bạn có thể truy cập vào đường link HTTPS mà Ngrok cung cấp để sử dụng Tool.
