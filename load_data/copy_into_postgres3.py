import os
import csv
import time
import random
import psycopg2
from psycopg2.extras import execute_values
import json
from dotenv import load_dotenv
from huggingface_hub import InferenceClient

# Đọc cấu hình từ .env
load_dotenv()

EXPECTED_DIM = 384          # khớp output all-MiniLM-L6-v2
EMBED_BATCH_SIZE = 32       # số text gửi mỗi lần gọi HF
DB_BATCH_SIZE = 100         # số dòng ghi DB mỗi execute_values
MAX_RETRIES = 4             # số lần thử lại khi HF lỗi / rate-limit


# =========================================================
# HÀM ÉP KIỂU (đưa ra ngoài để không định nghĩa lại mỗi dòng)
# =========================================================
def _to_float(val):
    try:
        return float(val) if val not in (None, "") else None
    except (ValueError, TypeError):
        return None


def _to_str(val):
    return val.strip() if val and str(val).strip() else None


def _to_bool(val):
    return str(val).lower() == "true"


# =========================================================
# NHÚNG VECTOR CÓ RETRY + BACKOFF (thay cho sleep(1) cứng)
# =========================================================
def embed_texts(client, texts):
    """Gọi HuggingFace với retry + exponential backoff. Trả list[list[float]] hoặc raise."""
    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            emb = client.feature_extraction(texts)
            return emb.tolist() if hasattr(emb, "tolist") else emb
        except Exception as e:  # noqa: BLE001 - bắt mọi lỗi mạng/429 để backoff
            last_err = e
            if attempt == MAX_RETRIES - 1:
                break
            # backoff lũy thừa + jitter; chỉ ngủ khi thực sự lỗi
            delay = (2 ** attempt) + random.uniform(0, 1)
            print(f"⚠️  HF lỗi (lần {attempt + 1}/{MAX_RETRIES}), thử lại sau {delay:.1f}s: {e}")
            time.sleep(delay)
    raise RuntimeError(f"HF API thất bại sau {MAX_RETRIES} lần: {last_err}")


# =========================================================
# PHA A: UPSERT các trường có cấu trúc (KHÔNG đụng embedding)
#  - Dòng mới: embedding để NULL -> Pha B sẽ nhúng.
#  - Dòng cũ đổi nội dung: reset embedding = NULL -> Pha B nhúng lại.
#  - Dòng cũ không đổi: GIỮ nguyên embedding -> không gọi API lại (incremental).
# =========================================================
def _upsert_structured(cursor, table, rows):
    insert_query = f"""
        INSERT INTO {table} (
            source_url, raw_content, extracted_json, confidence_score,
            province_code, district_code, property_type,
            price, area, price_per_m2, status, is_sell
        ) VALUES %s
        ON CONFLICT (source_url) DO UPDATE SET
            raw_content      = EXCLUDED.raw_content,
            extracted_json   = EXCLUDED.extracted_json,
            confidence_score = EXCLUDED.confidence_score,
            province_code    = EXCLUDED.province_code,
            district_code    = EXCLUDED.district_code,
            property_type    = EXCLUDED.property_type,
            price            = EXCLUDED.price,
            area             = EXCLUDED.area,
            price_per_m2     = EXCLUDED.price_per_m2,
            status           = EXCLUDED.status,
            is_sell          = EXCLUDED.is_sell,
            embedding        = CASE
                WHEN {table}.raw_content IS DISTINCT FROM EXCLUDED.raw_content
                THEN NULL
                ELSE {table}.embedding
            END,
            crawled_at = now();
    """

    values = []
    for r in rows:
        src = _to_str(r.get("source_url"))
        if not src:
            continue
        values.append((
            src,
            _to_str(r.get("raw_content")),
            _to_str(r.get("extracted_json")),
            _to_float(r.get("confidence_score")),
            _to_str(r.get("province_code")),
            _to_str(r.get("district_code")),
            _to_str(r.get("property_type")),
            _to_float(r.get("price")),
            _to_float(r.get("area")),
            _to_float(r.get("price_per_m2")),
            _to_str(r.get("status")),
            _to_bool(r.get("is_sell")),
        ))

    upserted = 0
    for i in range(0, len(values), DB_BATCH_SIZE):
        chunk = values[i:i + DB_BATCH_SIZE]
        execute_values(cursor, insert_query, chunk)
        upserted += len(chunk)
    return upserted


# =========================================================
# PHA B: chỉ nhúng các dòng CHƯA có embedding (incremental)
# =========================================================
def _embed_missing(conn, cursor, table):
    cursor.execute(
        f"SELECT source_url, raw_content FROM {table} "
        f"WHERE embedding IS NULL AND raw_content IS NOT NULL;"
    )
    pending = cursor.fetchall()
    total = len(pending)
    if total == 0:
        print("✅ Không có dòng nào cần nhúng (tất cả đã có embedding).")
        return 0, 0

    print(f"🧠 Cần nhúng {total} dòng (mới hoặc đổi nội dung)...")

    token = os.getenv("HUGGINGFACE_API_KEY")
    client = InferenceClient(model="sentence-transformers/all-MiniLM-L6-v2", token=token)

    update_query = f"""
        UPDATE {table} AS t
        SET embedding = v.emb::vector, crawled_at = now()
        FROM (VALUES %s) AS v(source_url, emb)
        WHERE t.source_url = v.source_url;
    """

    embedded, failed = 0, 0
    for i in range(0, total, EMBED_BATCH_SIZE):
        batch = pending[i:i + EMBED_BATCH_SIZE]
        urls = [b[0] for b in batch]
        texts = [b[1] for b in batch]

        try:
            vectors = embed_texts(client, texts)
        except RuntimeError as e:
            # Lỗi vĩnh viễn: KHÔNG bỏ âm thầm -> log rõ, để embedding NULL, lần chạy sau tự thử lại
            failed += len(batch)
            print(f"❌ Bỏ qua batch (sẽ thử lại lần sau). {len(batch)} dòng. Lỗi: {e}")
            print(f"   source_url chưa nhúng: {urls}")
            continue

        update_values = []
        for url, vec in zip(urls, vectors):
            if not isinstance(vec, list) or len(vec) != EXPECTED_DIM:
                failed += 1
                print(f"⚠️  Vector sai chiều ({len(vec) if hasattr(vec, '__len__') else '?'}) cho {url}, bỏ qua.")
                continue
            update_values.append((url, json.dumps(vec)))

        if update_values:
            try:
                execute_values(cursor, update_query, update_values)
                conn.commit()
                embedded += len(update_values)
                print(f"✅ Đã nhúng {embedded}/{total} dòng...")
            except Exception as e:  # noqa: BLE001
                conn.rollback()
                failed += len(update_values)
                print(f"❌ Lỗi UPDATE embedding: {e}")

    return embedded, failed


# =========================================================
# HÀM CHÍNH (giữ nguyên chữ ký để DAG gọi)
# =========================================================
def load_to_supabase(csv_path, table="ai_extraction_logs"):
    if not csv_path or not os.path.exists(csv_path):
        print(f"❌ Không tìm thấy file: {csv_path}")
        return None

    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        print("❌ LỖI: Cần bổ sung SUPABASE_DB_URL vào file .env")
        return None

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    print(f"🚀 Bắt đầu nạp {len(rows)} bản ghi vào bảng {table}...")

    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()

    try:
        # ---- PHA A: nạp dữ liệu có cấu trúc (nhanh, mọi dòng) ----
        upserted = _upsert_structured(cursor, table, rows)
        conn.commit()
        print(f"📦 Pha A: đã UPSERT {upserted} dòng (trường có cấu trúc).")

        # ---- PHA B: chỉ nhúng vector cho dòng còn thiếu (incremental) ----
        embedded, failed = _embed_missing(conn, cursor, table)
    finally:
        cursor.close()
        conn.close()

    print(f"🎯 Hoàn tất! UPSERT: {upserted} | Nhúng mới: {embedded} | Thất bại (NULL, thử lại sau): {failed}.")

    if upserted == 0:
        raise RuntimeError(f"❌ CRITICAL: Không UPSERT được bản ghi nào từ {csv_path}")

    return True
