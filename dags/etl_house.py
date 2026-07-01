
#dags/etl_house.py
import sys
import os
import shutil
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# =========================
# THIẾT LẬP ĐƯỜNG DẪN PROJECT
# =========================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# =========================================
# WRAPPER FUNCTIONS (Hàm bọc cho Airflow)
# =========================================

def _extract_task(ti, **kwargs):
    """Bước 1: Crawl dữ liệu và trả về path CSV thô.
    [THAY ĐỔI] limit có thể truyền khi Trigger DAG:
      - Hằng ngày: dùng mặc định 200.
      - Backfill 1 lần: Trigger DAG w/ config  ->  {"limit": 3000}
    """
    from extract_data.extract_house import extract_house

    dag_run = kwargs.get('dag_run')
    conf = (dag_run.conf or {}) if dag_run else {}
    # [THAY ĐỔI] Thứ tự ưu tiên: config khi trigger > biến môi trường ETL_LIMIT (set trong start_airflow.sh) > mặc định 200
    limit = int(conf.get('limit') or os.getenv('ETL_LIMIT') or kwargs.get('limit', 200))
    print(f"📥 Extract với limit={limit}")

    raw_path = extract_house(limit_rows=limit)
    if not raw_path:
        raise ValueError("Extract failed: No CSV path returned")
    return raw_path

def _upload_raw_task(ti):
    """Bước 2: Upload file thô lên Supabase Storage (Datalake)."""
    from load_data.upload_to_supabase_storage3 import upload_to_storage

    raw_path = ti.xcom_pull(task_ids="extract_house_task")
    return upload_to_storage(raw_path, bucket="datalake-house", dest_folder="raw")

def _transform_task(ti):
    """Bước 3: Làm sạch dữ liệu và tạo cột ai_summary cho RAG."""
    from transform_data.transform_house_pandas import clean_house

    raw_path = ti.xcom_pull(task_ids="extract_house_task")
    clean_path = clean_house(raw_path)
    # Đẩy path đã clean vào XCom để task load và analyze lấy dùng
    ti.xcom_push(key="cleaned_csv_path", value=clean_path)
    return clean_path

def _load_vector_task(ti):
    """Bước 4: Tạo Vector Embedding và đẩy lên Supabase Vector DB."""
    from load_data.copy_into_postgres3 import load_to_supabase # Hàm nạp kèm Vector

    clean_path = ti.xcom_pull(task_ids="transform_house_task", key="cleaned_csv_path")
    if not clean_path:
        raise ValueError("Load failed: No cleaned CSV path found in XCom")
    return load_to_supabase(clean_path, table="ai_extraction_logs")

def _analyze_task(ti):
    """Bước 5: Tạo biểu đồ phân tích cho Admin CMS."""
    from analyze.house_analysis import analyze_house

    clean_path = ti.xcom_pull(task_ids="transform_house_task", key="cleaned_csv_path")
    return analyze_house(clean_path)

def _cleanup_task(ti):
    """Bước Cuối: Xóa file CSV local sau khi đẩy hết lên Remote an toàn."""
    """Chỉ xóa folder cũ hơn 3 ngày để tránh Race Condition"""
    base_dir = os.path.join(PROJECT_ROOT, "data_input/house")
    cutoff = datetime.now() - timedelta(days=3)
    
    if not os.path.exists(base_dir): return
    
    for folder in os.listdir(base_dir):
        path = os.path.join(base_dir, folder)
        try:
            f_date = datetime.strptime(folder, "%Y-%m-%d")
            if f_date < cutoff:
                shutil.rmtree(path)
                print(f"🧹 Xóa folder cũ: {folder}")
        except: continue

# =========================
# CẤU HÌNH DAG
# =========================
default_args = {
    "owner": "duc_ngo_hust",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False
}

with DAG(
    dag_id="real_estate_rag_pipeline_v2",
    default_args=default_args,
    description="Pipeline ETL nạp dữ liệu BĐS kèm Vector Embedding cho RAG Chatbot",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["hust", "rag", "vector_db", "supabase"]
) as dag:

    # 1. Cào dữ liệu
    t1 = PythonOperator(
        task_id="extract_house_task",
        python_callable=_extract_task,
        op_kwargs={"limit": 200}
    )

    # 2. Lưu trữ file thô (Chạy song song với Transform để backup)
    t2 = PythonOperator(
        task_id="upload_raw_to_storage",
        python_callable=_upload_raw_task
    )

    # 3. Xử lý và chuẩn bị AI Context
    t3 = PythonOperator(
        task_id="transform_house_task",
        python_callable=_transform_task
    )

    # 4. Tạo Vector và nạp vào CSDL (Điểm mấu chốt của đồ án)
    t4 = PythonOperator(
        task_id="load_to_vector_db",
        python_callable=_load_vector_task,
        execution_timeout=timedelta(minutes=5)
    )

    # 5. Phân tích dữ liệu
    t5 = PythonOperator(
        task_id="analyze_house_task",
        python_callable=_analyze_task
    )

    # 6. Task Dọn dẹp
    t6 = PythonOperator(
        task_id="cleanup_local_files",
        python_callable=_cleanup_task,
        trigger_rule="all_done" # Chạy kể cả khi t4, t5 lỗi để dọn dẹp
    )

    # THIẾT LẬP LUỒNG CHẠY
    # t1 xong thì t2 và t3 chạy song song. t3 xong thì t4 và t5 chạy.
    t1 >> [t2, t3]
    t3 >> [t4, t5]
    [t2, t4, t5] >> t6
