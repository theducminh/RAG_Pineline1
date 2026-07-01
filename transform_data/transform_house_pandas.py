import os
import sys
import pandas as pd
import csv
import json

# =========================================================
# THIẾT LẬP ĐƯỜNG DẪN TUYỆT ĐỐI (TRÁNH LỖI AIRFLOW DOCKER)
# =========================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

# =========================================================
# HÀM LÀM SẠCH TÊN ĐỊA LÝ & LOAD MASTER DATA (2 CẤP)
# =========================================================
def clean_location_name(name):
    """Loại bỏ các tiền tố hành chính để so sánh chuẩn xác"""
    if pd.isna(name): return ""
    name = str(name).strip().lower()
    prefixes = ['thành phố ', 'tỉnh ', 'quận ', 'huyện ', 'thị xã ', 'phường ', 'xã ', 'thị trấn ']
    for p in prefixes:
        if name.startswith(p):
            name = name.replace(p, "", 1)
            break
    return name.strip()

def load_location_dicts(provinces_csv, districts_csv):
    prov_dict, dist_dict = {}, {}

    # 1. Load Provinces (Tỉnh/Thành)
    if os.path.exists(provinces_csv):
        df_p = pd.read_csv(provinces_csv, dtype=str)
        for _, row in df_p.iterrows():
            code = str(row['code']).strip()
            if pd.notna(row['name']): prov_dict[clean_location_name(row['name'])] = code
            if pd.notna(row['full_name']): prov_dict[clean_location_name(row['full_name'])] = code

    # 2. Load Districts (Nay đang chứa dữ liệu Xã/Phường)
    if os.path.exists(districts_csv):
        df_d = pd.read_csv(districts_csv, dtype=str)
        for _, row in df_d.iterrows():
            code = str(row['code']).strip()
            p_code = str(row['province_code']).strip()
            if pd.notna(row['name']): 
                dist_dict[f"{p_code}_{clean_location_name(row['name'])}"] = code
            if pd.notna(row['full_name']): 
                dist_dict[f"{p_code}_{clean_location_name(row['full_name'])}"] = code

    return prov_dict, dist_dict

# Chỉ còn nạp 2 file (Khai tử file Wards)
PROV_CSV = os.path.join(PROJECT_ROOT, 'data_input/master_data/provinces.csv')
DIST_CSV = os.path.join(PROJECT_ROOT, 'data_input/master_data/districts.csv')

PROV_DICT, DIST_DICT = load_location_dicts(PROV_CSV, DIST_CSV)
print(f"📊 Đã tải Master Data 2 Cấp: {len(PROV_DICT)} Tỉnh, {len(DIST_DICT)} Xã/Phường.")

# =========================================================
# [THÊM MỚI] LỌC OUTLIER GIÁ (chống tin "giá ảo" làm lệch benchmark định giá)
#  - Bỏ price_per_m2 <= 0.
#  - Trong từng nhóm (is_sell, property_type): cắt 1% trên/dưới của price_per_m2.
#  - Nhóm < 20 mẫu: giữ nguyên để tránh cắt nhầm khi dữ liệu còn ít.
# =========================================================
def filter_price_outliers(df):
    if 'price_per_m2' not in df.columns or df.empty:
        return df

    before = len(df)
    df = df[df['price_per_m2'] > 0]

    def clip_group(g):
        if len(g) < 20:
            return g
        lo = g['price_per_m2'].quantile(0.01)
        hi = g['price_per_m2'].quantile(0.99)
        return g[(g['price_per_m2'] >= lo) & (g['price_per_m2'] <= hi)]

    df = df.groupby(['is_sell', 'property_type'], group_keys=False).apply(clip_group)
    print(f"🧹 Lọc outlier giá: {before} -> {len(df)} dòng (giữ lại {len(df)}).")
    return df


def clean_house(raw_path):
    if not raw_path or not os.path.exists(raw_path):
        print(f"❌ Đường dẫn raw_path không tồn tại: {raw_path}")
        return None

    try:
        df = pd.read_csv(raw_path)
        if df.empty: return None
        print(f"🔍 Số dòng ban đầu cào về: {len(df)}")

        df = df.dropna(subset=['id']).drop_duplicates(subset=['id'])

        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        df['area'] = pd.to_numeric(df['area'], errors='coerce').fillna(0)
        df['price_per_m2'] = df.apply(lambda row: round(row['price'] / row['area'], 2) if row['area'] > 0 else 0.0, axis=1)

        df['source_url'] = df['id'].apply(lambda x: f"https://chotot.com/{x}" if str(x).isdigit() else f"https://huggingface.co/tinixai/{x}")

        def map_property_type(t):
            t_str = str(t).lower()
            if 'chung cư' in t_str or 'căn hộ' in t_str or 'studio' in t_str: return 'APARTMENT'
            if 'đất' in t_str: return 'PLOT'
            return 'HOUSE'
        df['property_type'] = df['property_type_name'].apply(map_property_type)

        # =========================================================
        # MAP ĐỊA CHỈ TỪ CHỮ SANG MÃ CODE (MÔ HÌNH 2 CẤP)
        # =========================================================
        
        # 1. Tỉnh/Thành
        df['province_code'] = df['province_name'].apply(
            lambda x: PROV_DICT.get(clean_location_name(x))
        )
        
        def get_ward_as_district(row):
            if pd.isna(row['province_code']): 
                return None
            
            p_code = str(row['province_code']).strip()
            w_name = clean_location_name(row['ward_name']) if pd.notna(row['ward_name']) else ""
            d_name = clean_location_name(row['district_name']) if pd.notna(row['district_name']) else ""
            
            import unicodedata
            def strip_accents(s):
                s = str(s).replace('đ', 'd').replace('Đ', 'D')
                return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn').replace(' ', '').lower()

            # 1. Khớp chính xác 100%
            if w_name and f"{p_code}_{w_name}" in DIST_DICT: return DIST_DICT[f"{p_code}_{w_name}"]
            if d_name and f"{p_code}_{d_name}" in DIST_DICT: return DIST_DICT[f"{p_code}_{d_name}"]

            # 2. Khớp tương đối (Bỏ dấu, bỏ khoảng trắng)
            w_no_accent = strip_accents(w_name)
            d_no_accent = strip_accents(d_name)
            
            # Gộp cả Tiêu đề và Mô tả để AI tự đọc tìm Phường (Chống lại việc Chợ Tốt trả về rỗng)
            full_text = strip_accents(str(row.get('title', '')) + " " + str(row.get('description', '')))

            for key, code in DIST_DICT.items():
                if key.startswith(f"{p_code}_"):
                    dict_name_no_accent = strip_accents(key.replace(f"{p_code}_", ""))
                    
                    # Ưu tiên quét từ API
                    if w_no_accent and len(w_no_accent) > 2 and (w_no_accent in dict_name_no_accent or dict_name_no_accent in w_no_accent): return code
                    if d_no_accent and len(d_no_accent) > 2 and (d_no_accent in dict_name_no_accent or dict_name_no_accent in d_no_accent): return code
                    
                    # QUÉT VÉT CẠN TỪ VĂN BẢN (Text Mining)
                    if len(dict_name_no_accent) > 3 and dict_name_no_accent in full_text:
                        return code
                        
            # 3. Fallback: Lấy bừa 1 mã cùng Tỉnh để giữ Data cho LLM đọc
            for key, code in DIST_DICT.items():
                if key.startswith(f"{p_code}_"): return code

            return None
            
        df['district_code'] = df.apply(get_ward_as_district, axis=1)

        # Cắt bỏ các bài viết bị mồ côi (Mất cả Tỉnh)
        df = df.dropna(subset=['province_code'])
        print(f"✅ Số dòng giữ lại sau khi map địa chỉ: {len(df)}")
        
        if df.empty:
            print("❌ LỖI DATA: Toàn bộ dữ liệu đã bị xóa do không map được Tỉnh! (Hãy check lại file CSV Master Data)")
            return None

        # =========================================================

        str_cols = ["title", "property_type_name", "street_name", "ward_name", "district_name", "house_direction", "legal_status", "description"]
        df[str_cols] = df[str_cols].fillna("Không xác định")

        def build_raw_content(row):
            # 1. Phiên dịch Trạng thái giao dịch (is_sell)
            # (Giả định cột is_sell đã được hàm detect_is_sell tạo ra trước đó)
            is_sell_str = "Bán" if row.get('is_sell') else "Cho thuê"
            
            # 2. Phiên dịch Loại hình BĐS (property_type)
            p_type = str(row.get('property_type', '')).upper()
            if p_type == 'APARTMENT':
                type_str = "Căn hộ chung cư"
            elif p_type == 'PLOT':
                type_str = "Đất nền / Đất trống"
            else:
                type_str = "Nhà riêng / Nhà phố"
                
            # 3. Gom dọn Địa chỉ (Tránh bị chuỗi ", , Hà Nội" nếu thiếu đường/quận)
            street = str(row.get('street_name', '')).strip()
            district = str(row.get('district_name', '')).strip() # Giữ nguyên text Quận/Huyện cũ cho AI đọc
            province = str(row.get('province_name', '')).strip()
            
            addr_parts = [p for p in [street, district, province] if p and p.lower() != 'nan' and p != 'Không xác định']
            address = ", ".join(addr_parts) if addr_parts else "Không xác định"
            
            # 4. Đóng gói thành chuỗi văn bản giàu ngữ nghĩa cho AI
            content = (
                f"Hình thức: {is_sell_str} {type_str}. "
                f"Tiêu đề: {row.get('title', 'Không xác định')}. "
                f"Vị trí: {address}. "
                f"Diện tích: {row.get('area', 0)} m2. "
                f"Mức giá: {row.get('price', 0)} VNĐ. "
                f"Mô tả chi tiết: {row.get('description', 'Không có mô tả')}"
            )
            
            # Giới hạn 2000 ký tự để không bị tràn Token của mô hình Embedding
            return content[:2000]

        df['raw_content'] = df.apply(build_raw_content, axis=1)

        def build_extracted_json(row):
            detail_dict = {
                "title": row.get("title"), "bedroom_count": row.get("bedroom_count"),
                "bathroom_count": row.get("bathroom_count"), "frontage_width": row.get("frontage_width")
            }
            return json.dumps({k: v for k, v in detail_dict.items() if pd.notna(v)}, ensure_ascii=False)
        
        df['extracted_json'] = df.apply(build_extracted_json, axis=1)
        def detect_is_sell(row):
            text = str(row.get('title', '')) + " " + str(row.get('description', ''))
            text_lower = text.lower()
            
            # Rule 1: Từ khóa rành rành là cho thuê
            if 'cho thuê' in text_lower or 'phòng trọ' in text_lower or 'ccmn' in text_lower:
                return False
                
            # Rule 2: Dựa vào giá (Dưới 200 triệu thường 99% là Cho thuê/tháng)
            if float(row['price']) > 0 and float(row['price']) < 200000000:
                return False
                
            # Mặc định là Bán
            return True

        df['is_sell'] = df.apply(detect_is_sell, axis=1)

        # [THÊM MỚI] Lọc tin giá ảo sau khi đã có is_sell + property_type + price_per_m2
        df = filter_price_outliers(df)
        if df.empty:
            print("❌ Sau khi lọc outlier không còn dòng nào hợp lệ!")
            return None

        df['confidence_score'] = 0.95
        df['status'] = 'SUCCESS'

        if 'vector_embedding' in df.columns:
            df = df.rename(columns={'vector_embedding': 'embedding'})
        elif 'embedding' not in df.columns:
            df['embedding'] = None

        # 🚨 KHAI TỬ ward_code
        target_columns = [
            'source_url', 'raw_content', 'extracted_json', 'confidence_score',
            'province_code', 'district_code', 
            'property_type', 'price', 'area', 'price_per_m2',
            'status', 'embedding', 'is_sell'
        ]
        
        out_df = df[[c for c in target_columns if c in df.columns]]
        clean_path = raw_path.replace("raw_", "clean_")
        # [SỬA LỖI] Bỏ escapechar='\\' -> dùng cơ chế nhân đôi dấu nháy mặc định (doublequote)
        # để stdlib csv.DictReader bên load đọc lại ĐÚNG cột (trước đây lệch cột -> source_url rỗng -> UPSERT 0).
        out_df.to_csv(clean_path, index=False, quoting=csv.QUOTE_ALL)
        print(f"🎯 File Cleaned sẵn sàng: {clean_path}")
        return clean_path

    except Exception as e:
        print(f"❌ Lỗi nội bộ Transform Pandas: {e}")
        import traceback
        traceback.print_exc()
        return None
