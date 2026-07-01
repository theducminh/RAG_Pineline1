#!/bin/bash

export AIRFLOW_HOME="$(pwd)/airflow_home"

echo "🛑 Đang gửi tín hiệu dừng chuẩn (Graceful Shutdown)..."
# Chỉ nhắm vào đúng tiến trình cần tắt, chừa cái script này ra
pkill -TERM -f "airflow webserver"
pkill -TERM -f "airflow scheduler"

echo "⏳ Đợi hệ thống dọn dẹp..."
sleep 3

echo "🧹 Tiêu diệt tàn dư (nếu còn)..."
kill -9 $(lsof -t -i:8081) 2>/dev/null
kill -9 $(lsof -t -i:8793) 2>/dev/null
pkill -9 -f "airflow webserver"
pkill -9 -f "airflow scheduler"

echo "🗑️ Xóa file PID rác..."
rm -f "$AIRFLOW_HOME"/*.pid

echo "✅ Đã tắt hoàn toàn và trả lại sự trong sáng cho WSL!"