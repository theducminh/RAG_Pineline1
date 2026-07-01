# RAG-Pipeline: Hệ thống Data Pipeline Bất Động Sản

Đây là dự án tự động thu thập và xử lý dữ liệu bất động sản, sau đó đưa vào cơ sở dữ liệu Vector để phục vụ cho AI (RAG Chatbot). Hệ thống được quản lý và chạy tự động hàng ngày bằng Apache Airflow.

## 🏗 Kiến trúc tổng quan
Hệ thống hoạt động theo 4 bước cơ bản (ETL):
1. **Extract (Cào dữ liệu):** Lấy dữ liệu nhà đất thô từ Chợ Tốt và HuggingFace.
2. **Transform (Làm sạch):** Dùng Pandas để lọc bỏ dữ liệu rác, tính toán giá/m2 và chuẩn hóa thông tin.
3. **Embedding ():** Đưa dữ liệu qua HuggingFace API để biến văn bản thành các vector số (AI mới hiểu được).
4. **Load (Lưu trữ):** Đẩy dữ liệu đã xử lý lên Supabase (PostgreSQL + pgvector).

---

## 💻 Hướng dẫn Cài đặt & Chạy (Dành cho người mới)

Dự án này được cấu hình tốt nhất để chạy trên môi trường **Linux** hoặc **WSL2 (Ubuntu) trên Windows**.

### Bước 1: Tải code về máy
Mở Terminal (Ubuntu/WSL2) và chạy lệnh sau:
```
git clone https://github.com/theducminh/RAG_Pineline.git
cd RAG_Pineline
```

### Bước 2: Khởi tạo môi trường ảo Python
Để không làm rác máy, bạn cần tạo một môi trường Python riêng cho dự án này:

```
# Cài đặt venv (nếu máy chưa có)
sudo apt install python3.10-venv -y

# Tạo môi trường ảo có tên là .airflow_env
python3 -m venv .airflow_env

# Kích hoạt môi trường (LƯU Ý: Phải chạy lệnh này mỗi khi mở terminal mới)
source .airflow_env/bin/activate
```

### Bước 3: Cài đặt thư viện
Khi đã thấy chữ (.airflow_env) ở đầu dòng lệnh terminal, tiến hành cài đặt:

```
pip install -r requirements.txt
pip install "apache-airflow==2.9.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.10.txt"
```

### Bước 4: Cấu hình Khóa bảo mật (API Keys)
Tạo một file mới tên là .env nằm ngay trong thư mục gốc của dự án (ngang hàng với thư mục dags/). Mở file đó lên và điền các thông tin sau:

```
HUGGINGFACE_API_KEY=hf_xxxx...
SUPABASE_URL=[https://xxxx.supabase.co](https://xxxx.supabase.co)
SUPABASE_SERVICE_ROLE_KEY=eyJhxxxx...
SUPABASE_DB_URL=postgresql://postgres.xxxx:[PASSWORD]@[aws-0-ap-southeast-1.pooler.supabase.com:6543/postgres]
```

(Hỏi người quản lý dự án để lấy các đoạn mã xxxx thực tế).

### Bước 5: Khởi động Hệ thống
Tiến hành chạy Airflow:

```
./start_airflow.sh
```

(Nếu hệ thống hỏi mật khẩu máy tính để bật PostgreSQL, hãy nhập vào).

### Bước 6: Truy cập giao diện quản lý
Mở trình duyệt web và vào địa chỉ: http://localhost:8081

Tài khoản đăng nhập mặc định: admin / Mật khẩu: admin

Tại đây, bạn có thể bật (unpause) DAG có tên etl_house để hệ thống bắt đầu tự động chạy.

🛑 Cách tắt Hệ thống an toàn
Airflow chạy ngầm rất nhiều tiến trình. Tuyệt đối không tắt ngang Terminal (hoặc bấm Ctrl+C). Để tắt dọn dẹp sạch sẽ và giải phóng RAM, hãy mở một terminal mới (vào lại thư mục dự án) và chạy:

```
./stop_airflow.sh
```