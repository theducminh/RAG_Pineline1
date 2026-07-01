#!/bin/bash

# 1. Định vị gốc dự án
PROJECT_ROOT=$(pwd)

# 2. KIỂM TRA MÔI TRƯỜNG ẢO (Fail-fast)
echo "🐍 Đang kiểm tra môi trường ảo (Virtual Environment)..."
if [ -z "$VIRTUAL_ENV" ]; then
    echo "❌ LỖI CHÍ MẠNG: Bạn chưa kích hoạt môi trường ảo (python virtual environments)!"
    exit 1
fi
echo "✅ Môi trường ảo hợp lệ: $VIRTUAL_ENV"

# 3. Bơm biến môi trường
export AIRFLOW_HOME="$PROJECT_ROOT/airflow_home"
export PYTHONPATH="$PROJECT_ROOT"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_ROOT/dags"   # <--- THÊM DÒNG NÀY ĐỂ ÉP TÌM DAG Ở BÊN NGOÀI
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__CORE__EXECUTOR="LocalExecutor"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow_user:airflow_pass@localhost:5432/airflow_db"

# 4. KIỂM TRA VÀ TỰ ĐỘNG KHỞI TẠO DATABASE
echo "🔍 Đang kiểm tra trạng thái Database..."

# 4.0. ĐỀ NỔ ĐỘNG CƠ POSTGRESQL (Dành cho WSL hay bị tắt ngầm)
if ! pg_isready -h localhost -p 5432 &> /dev/null; then
    echo "⚠️  PostgreSQL đang tắt! Đang tự động gọi sudo để khởi động..."
    echo "🔑 (Nhập mật khẩu Ubuntu nếu hệ thống yêu cầu)"
    sudo service postgresql start
    
    # Đợi 2 giây cho động cơ quay đều trước khi đâm vào connect
    sleep 2 
    
    # Check lại lần nữa cho chắc cốp
    if ! pg_isready -h localhost -p 5432 &> /dev/null; then
        echo "❌ LỖI CHÍ MẠNG: Không thể khởi động PostgreSQL. Mày tự check lại lệnh 'sudo service postgresql status' đi. Script dừng!"
        exit 1
    fi
    echo "✅ Động cơ PostgreSQL đã chạy mượt!"
fi

# 4.1. TỰ ĐỘNG SỬA POSTGRESQL (Nếu sai pass hoặc chưa có DB)
if ! PGPASSWORD='airflow_pass' psql -h localhost -U airflow_user -d airflow_db -c '\q' &> /dev/null; then
    echo "⚠️  Postgres chưa có User/DB 'airflow_user' hoặc sai pass. Đang tự động cấp quyền..."
    # Gọi quyền root của Postgres để ép tạo DB (Sẽ yêu cầu nhập mật khẩu WSL của cậu)
    sudo -u postgres psql -c "DROP DATABASE IF EXISTS airflow_db;" &>/dev/null
    sudo -u postgres psql -c "DROP USER IF EXISTS airflow_user;" &>/dev/null
    sudo -u postgres psql -c "CREATE USER airflow_user WITH PASSWORD 'airflow_pass';"
    sudo -u postgres psql -c "CREATE DATABASE airflow_db OWNER airflow_user;"
    sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;"
    sudo -u postgres psql -c "ALTER DATABASE airflow_db OWNER TO airflow_user;"
    echo "✅ Đã tạo xong Postgres User và DB dưới nền."
fi

# 4.2. KIỂM TRA AIRFLOW METADATA
# Dùng db migrate thay cho db init vì db init đã bị deprecated
if ! airflow db check-migrations &> /dev/null; then
    echo "⚠️  CẢNH BÁO: Airflow Metadata chưa được khởi tạo!"
    echo "Hệ thống cần chạy 'airflow db migrate' và tạo tài khoản Admin để tiếp tục."
    
    # Hỏi ý kiến người dùng
    read -p "❓ Bạn có muốn tự động khởi tạo DB và tạo User Admin mẫu không? (y/n): " confirm
    if [[ "$confirm" == [yY] || "$confirm" == [yY][eE][sS] ]]; then
        echo "⚙️  Đang đổ móng Database (db migrate)..."
        airflow db migrate
        
        echo "👤 Đang tạo tài khoản Admin (Ví dụ mẫu)..."
        airflow users create \
            --username admin \
            --firstname John \
            --lastname Doe \
            --role Admin \
            --email admin@example.com \
            --password admin
            
        echo "✅ Khởi tạo hoàn tất! Chuyển sang bước tiếp theo..."
    else
        echo "🛑 Đã hủy. Bạn vui lòng tự chạy bằng tay. Script dừng tại đây."
        exit 1
    fi
else
    echo "✅ Database hợp lệ. Bỏ qua bước kiểm tra."
fi

# 5. Dọn dẹp file kẹt do lần tắt đột ngột trước
rm -f "$AIRFLOW_HOME"/*.pid

# 6. Khởi động hệ thống
echo "🚀 Đang khởi động Airflow Webserver và Scheduler..."
airflow webserver --port 8081 &
airflow scheduler &

echo "🌐 Giao diện UI đang chạy ở: http://localhost:8081"
wait