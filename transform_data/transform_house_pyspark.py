# transform_data/transform_house_pyspark.py
import os
import shutil
import glob
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, concat_ws, lit, max_by, row_number, struct, substring, coalesce
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, IntegerType, TimestampType

def clean_house(raw_path):
    if not raw_path or not os.path.exists(raw_path):
        print(f"❌ Đường dẫn không hợp lệ: {raw_path}")
        return None

    # 1. Khởi tạo Spark Session (local mode cho đồ án)
    spark = SparkSession.builder \
        .appName("TransformHouseData") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    # Tắt log rác của Spark trên Airflow console
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 2. Đọc dữ liệu thô (Chống xô lệch cột tuyệt đối)
        # schema = StructType([
        #     StructField("id", LongType(), True),
        #     StructField("title", StringType(), True),
        #     StructField("description", StringType(), True),
        #     StructField("property_type_name", StringType(), True),
        #     StructField("province_name", StringType(), True),
        #     StructField("district_name", StringType(), True),
        #     StructField("ward_name", StringType(), True),
        #     StructField("street_name", StringType(), True),
        #     StructField("project_name", StringType(), True),
        #     StructField("price", DoubleType(), True),
        #     StructField("area", DoubleType(), True),
        #     StructField("lat", DoubleType(), True),
        #     StructField("lng", DoubleType(), True),
        #     StructField("bedroom_count", IntegerType(), True),
        #     StructField("bathroom_count", IntegerType(), True),
        #     StructField("floor_count", IntegerType(), True),
        #     StructField("house_direction", StringType(), True),
        #     StructField("legal_status", StringType(), True),
        #     StructField("road_width", DoubleType(), True),
        #     StructField("frontage_width", DoubleType(), True),
        #     StructField("house_depth", DoubleType(), True),
        #     StructField("published_at", TimestampType(), True),
        #     StructField("images_count", IntegerType(), True)
        # ])
        df = spark.read \
            .option("header", True) \
            .option("multiLine", True) \
            .option("quote", '"') \
            .option("escape", '"') \
            .csv(raw_path, inferSchema=True)
        
        # Loại bỏ các dòng không có ID hợp lệ, trùng
        df = df.filter(col("id").isNotNull())
        df = df.dropDuplicates(["id"]).orderBy(col("published_at").desc())

        # 3. Transform số liệu
        df = df.withColumn("price_million", col("price") / 1000000) \
               .withColumn("price_per_m2", col("price") / col("area"))

        # 4. Quét và dập lỗi Null (Tránh NaN Contagion)
        cols_to_fill_str = [
            "title", "property_type_name", "street_name", "ward_name", 
            "district_name", "house_direction", "legal_status", "description"
        ]
        # Fill giá trị mặc định cho String
        df = df.fillna("Không xác định", subset=cols_to_fill_str)

        # Ép kiểu an toàn cho các cột số để đưa vào AI Summary
        df = df.withColumn("area_str", coalesce(col("area").cast("string"), lit("0")))
        df = df.withColumn("price_str", coalesce(col("price_million").cast("string"), lit("0")))
        df = df.withColumn("bed_str", coalesce(col("bedroom_count").cast("string"), lit("0")))

        # 5. Tạo AI Context Vector an toàn bằng concat_ws
        # concat_ws sẽ tự động bỏ qua null nếu có trường nào lọt lưới, không làm sập cả chuỗi
        df = df.withColumn(
            "ai_summary",
            substring(
                concat_ws(". ",
                    concat_ws(": ", lit("Bất động sản"), col("title")),
                    concat_ws(": ", lit("Loại hình"), col("property_type_name")),
                    concat_ws(": ", lit("Địa chỉ"), concat_ws(", ", col("street_name"), col("ward_name"), col("district_name"))),
                    concat_ws(": ", lit("Diện tích"), concat_ws("", col("area_str"), lit("m2"))),
                    concat_ws(": ", lit("Giá"), concat_ws(" ", col("price_str"), lit("triệu"))),
                    concat_ws(": ", lit("Thông tin thêm"), concat_ws(", ", 
                        concat_ws(" ", col("bed_str"), lit("PN")),
                        concat_ws(" ", lit("hướng"), col("house_direction")),
                        concat_ws(" ", lit("pháp lý"), col("legal_status"))
                    )),
                    concat_ws(": ", lit("Mô tả chi tiết"), col("description"))
                ),
                1, 1000 # Cắt chuỗi để model Embedding không bị ngộp (Max token)
            )
        )

        # 6. Ghi file và tương thích hóa với Airflow Downstream
        clean_dir = raw_path.replace("raw_", "clean_dir_") 
        clean_path = raw_path.replace("raw_", "clean_")

        # Ghi ra thư mục tạm với 1 partition duy nhất
        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .option("quote", '"') \
            .option("escape", '"') \
            .option("quoteAll", True) \
            .option("multiLine", True) \
            .csv(clean_dir)
        
        # Bóc tách file CSV thực sự ra khỏi thư mục của Spark
        part_file = glob.glob(os.path.join(clean_dir, "*.csv"))[0]
        shutil.copy(part_file, clean_path)
        
        # Dọn dẹp chiến trường
        shutil.rmtree(clean_dir)

        print(f"✅ Transform PySpark hoàn tất: {clean_path}")
        return clean_path

    except Exception as e:
        print(f"❌ Lỗi khi Transform bằng PySpark: {e}")
        return None
    finally:
        # Quan trọng: Đóng session để nhả RAM cho task tiếp theo của Airflow
        spark.stop()

if __name__ == "__main__":
    # Test local
    # clean_house("data_input/house/2026-04-17/raw_1776361924.csv")
    pass