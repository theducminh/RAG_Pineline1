# extract_data/extract_house.py
import os, json, datetime, requests, csv, time, hashlib

def extract_chotot(limit_rows):
    """Cào dữ liệu từ Gateway API của Chợ Tốt"""
    print(f"🔄 Đang cào {limit_rows} tin từ Chợ Tốt...")
    ids = []
    page = 0
    # [THAY ĐỔI] Số trang co giãn theo limit (mỗi trang 20 tin) thay vì chặn cứng 10 trang (~200 tin)
    max_pages = (limit_rows // 20) + 5
    # Cần logic retry/backoff ở production, nhưng hiện tại giữ cấu trúc cũ
    while len(ids) < limit_rows:
        url_list = f"https://gateway.chotot.com/v1/public/ad-listing?region_v2=12000&cg=1000&o={page*20}&limit=20"
        try:
            resp = requests.get(url_list, headers={"User-Agent": "Mozilla/5.0"}).json()
            ads = resp.get("ads", [])
            if not ads: break
            for ad in ads:
                if "list_id" in ad: ids.append(ad["list_id"])
        except Exception as e:
            print(f"Lỗi lấy danh sách Chợ Tốt: {e}")
            break
        page += 1
        if page > max_pages: break   # [THAY ĐỔI] trần an toàn co giãn, tránh lặp vô hạn

    rows = []
    for ad_id in ids[:limit_rows]:
        try:
            url_detail = f"https://gateway.chotot.com/v1/public/ad-listing/{ad_id}"
            res = requests.get(url_detail, headers={"User-Agent": "Mozilla/5.0"}).json()
            detail = res.get("ad", {})
            params = detail.get("parameters", [])
            
            get_v = lambda label: next((p.get("value") for p in params if p.get("label") == label), None)

            # 🚨 MẸO ETL: Cứ lấy đủ 3 cấp (Region, Area, Ward) từ nguồn ngoài. 
            # Việc ép về 2 cấp sẽ do tầng Transform đảm nhiệm để không làm mất Text Context cho AI.
            row = {
                "id": str(detail.get("list_id")),
                "title": detail.get("subject"),
                "description": detail.get("body"),
                "property_type_name": detail.get("property_type_name"),
                "province_name": detail.get("region_name"),
                "district_name": detail.get("area_name"), # Sẽ được Transform nhét vào raw_content
                "ward_name": detail.get("ward_name"),     # Sẽ được Transform dùng làm khóa ngoại
                "street_name": detail.get("street_name"),
                "project_name": detail.get("project_name"),
                "price": detail.get("price"),
                "area": detail.get("area"),
                "lat": detail.get("latitude"),
                "lng": detail.get("longitude"),
                "bedroom_count": get_v("Số phòng ngủ"),
                "bathroom_count": get_v("Số phòng vệ sinh"),
                "floor_count": get_v("Số tầng"),
                "house_direction": get_v("Hướng cửa chính"),
                "legal_status": get_v("Giấy tờ pháp lý"),
                "road_width": get_v("Độ rộng đường trước nhà"),
                "frontage_width": get_v("Chiều ngang"),
                "house_depth": get_v("Chiều dài"),
                "published_at": datetime.datetime.fromtimestamp(detail.get("list_time") / 1000).isoformat(),
                "images_count": len(detail.get("images", []))
            }
            rows.append(row)
            time.sleep(0.4) 
        except Exception as e:
            print(f"Lỗi tại Chợ Tốt ID {ad_id}: {e}")
            
    return rows

def extract_huggingface(limit_rows):
    """Kéo dữ liệu từ dataset Hugging Face bằng Streaming để tránh OOM"""
    print(f"🔄 Đang kéo {limit_rows} tin từ Hugging Face (tinixai/vietnam-real-estates)...")
    try:
        from datasets import load_dataset
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        print("❌ Chưa cài thư viện datasets. Hãy chạy: pip install datasets")
        return []

    # Bắt buộc dùng streaming=True để không load file Parquet khổng lồ vào RAM
    ds = load_dataset("tinixai/vietnam-real-estates", split="train", streaming=True, token=os.getenv('HUGGINGFACE_API_KEY'))
    
    rows = []
    for item in ds.take(limit_rows):
        # 🚨 ĐÃ TỐI ƯU HASH ID: Đưa thêm ward_name vào để thuật toán băm tạo ID chuẩn xác nhất theo cấp Xã/Phường
        core_string = f"{item.get('name')}_{item.get('price')}_{item.get('area')}_{item.get('district_name')}_{item.get('ward_name')}"
        stable_id = f"hf_{hashlib.md5(core_string.encode('utf-8')).hexdigest()[:15]}"
        
        rows.append({
            "id": stable_id,
            "title": item.get("name"),
            "description": item.get("description"),
            "property_type_name": item.get("property_type_name"),
            "province_name": item.get("province_name"),
            "district_name": item.get("district_name"),
            "ward_name": item.get("ward_name"),
            "street_name": item.get("street_name"),
            "project_name": item.get("project_name"),
            "price": item.get("price"),
            "area": item.get("area"),
            "lat": None, 
            "lng": None, 
            "bedroom_count": item.get("bedroom_count"),
            "bathroom_count": item.get("bathroom_count"),
            "floor_count": item.get("floor_count"),
            "house_direction": item.get("house_direction"),
            "legal_status": "Không xác định", 
            "road_width": item.get("road_width"),
            "frontage_width": item.get("frontage"),
            "house_depth": item.get("depth"),
            "published_at": item.get("published_at"),
            "images_count": 0
        })
    return rows

def extract_house(limit_rows=100):
    today = datetime.date.today().isoformat()
    download_dir = f"data_input/house/{today}"
    os.makedirs(download_dir, exist_ok=True)
    
    # [THAY ĐỔI] Lấy 1 nửa từ Chợ Tốt; phần THIẾU (do Chợ Tốt giới hạn nguồn) bù bằng HuggingFace
    # -> tổng số dòng bám sát limit_rows yêu cầu thay vì hụt.
    target_chotot = limit_rows // 2
    chotot_rows = extract_chotot(target_chotot)
    remaining = max(limit_rows - len(chotot_rows), 0)
    print(f"📊 Chợ Tốt lấy được {len(chotot_rows)} dòng; HuggingFace sẽ bù {remaining} dòng.")
    hf_rows = extract_huggingface(remaining)

    rows = chotot_rows + hf_rows

    if not rows:
        raise RuntimeError("❌ CRITICAL: Không cào được bất kỳ dữ liệu nào từ Chợ Tốt và Hugging Face!")

    out_csv = os.path.join(download_dir, f"raw_{int(time.time())}.csv")
    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"✅ Đã lưu {len(rows)} dòng dữ liệu gộp vào: {out_csv}")
    return out_csv

if __name__ == "__main__":
    # Chạy độc lập để test
    path = extract_house(limit_rows=20)
