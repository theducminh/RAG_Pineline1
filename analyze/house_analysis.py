# analyze/analyze_house.py

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def analyze_house(csv_path, output_dir="output_data/house_analysis"):
    """
    Phân tích dữ liệu thực tế: Giá, Diện tích, Phân bố địa lý.
    Xuất biểu đồ phục vụ báo cáo đồ án và giao diện Admin CMS.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Đọc dữ liệu (đã được clean từ bước transform)
    df = pd.read_csv(csv_path)
    if df.empty:
        print("[WARN] Không có dữ liệu để phân tích.")
        return None

    # Thiết lập phong cách biểu đồ
    plt.rcParams['figure.facecolor'] = 'white'
    sns.set_theme(style="whitegrid")

    # 1. Phân bổ giá (Price Million)
    if "price_million" in df.columns:
        plt.figure(figsize=(10, 6))
        sns.histplot(df["price_million"].dropna(), bins=30, kde=True, color="blue")
        plt.title("Phân bổ giá niêm yết (Triệu VND)")
        plt.xlabel("Giá (Triệu)")
        plt.ylabel("Số lượng tin")
        plt.savefig(os.path.join(output_dir, "price_distribution.png"))
        plt.close()

    # 2. Top 10 Quận có nhiều tin đăng nhất (Độ phủ dữ liệu)
    if "district_name" in df.columns:
        plt.figure(figsize=(12, 6))
        df["district_name"].value_counts().head(10).plot(kind='bar', color='teal')
        plt.title("Top 10 Quận/Huyện có mật độ tin đăng cao nhất")
        plt.ylabel("Số lượng tin")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, "top_districts.png"))
        plt.close()

    # 3. Mối tương quan giữa Diện tích và Giá (Scatter Plot)
    if "area" in df.columns and "price_million" in df.columns:
        plt.figure(figsize=(10, 6))
        # Lọc bỏ outlier (giá ảo) để biểu đồ đẹp hơn
        df_plot = df[df['price_million'] < df['price_million'].quantile(0.95)]
        sns.scatterplot(data=df_plot, x="area", y="price_million", alpha=0.6)
        plt.title("Tương quan Diện tích vs Giá")
        plt.xlabel("Diện tích (m2)")
        plt.ylabel("Giá (Triệu VND)")
        plt.savefig(os.path.join(output_dir, "area_price_correlation.png"))
        plt.close()

    # 4. Lưu lại danh sách "Giá tốt" (Dựa trên giá/m2 thấp hơn trung bình khu vực)
    if "price_per_m2" in df.columns:
        avg_ppm2 = df["price_per_m2"].mean()
        good_deals = df[df["price_per_m2"] < avg_ppm2].sort_values("price_per_m2").head(20)
        good_deals.to_csv(os.path.join(output_dir, "potential_good_deals.csv"), index=False)

    print(f"📊 Phân tích hoàn tất. Kết quả lưu tại: {output_dir}")
    return os.path.abspath(output_dir)