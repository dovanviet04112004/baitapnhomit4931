from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, when, lower

spark = SparkSession.builder \
    .appName("CleanBooksToScrape") \
    .getOrCreate()

df_raw = spark.read \
    .option("multiLine", "true") \
    .json("hdfs://localhost:9000/data/raw/books_toscrape_raw.json")

print("Schema ban đầu:")
df_raw.printSchema()

# Chuẩn hoá các cột chính từ raw
if "in_stock" in df_raw.columns:
    df = df_raw.select(
        trim(col("title")).alias("title"),
        regexp_replace(col("price"), "[^0-9.]", "").cast("double").alias("price"),
        trim(col("rating")).alias("rating_raw"),
        trim(col("category")).alias("category"),
        col("availability"),
        col("in_stock").cast("boolean").alias("in_stock"),
    )
else:
    df = df_raw.select(
        trim(col("title")).alias("title"),
        regexp_replace(col("price"), "[^0-9.]", "").cast("double").alias("price"),
        trim(col("rating")).alias("rating_raw"),
        trim(col("category")).alias("category"),
        col("availability"),
        when(col("availability").contains("In stock"), True).otherwise(False).alias("in_stock"),
    )

# Đổi rating chữ -> số 1..5
df = df.withColumn(
    "rating",
    when(lower(col("rating_raw")) == "one", 1)
    .when(lower(col("rating_raw")) == "two", 2)
    .when(lower(col("rating_raw")) == "three", 3)
    .when(lower(col("rating_raw")) == "four", 4)
    .when(lower(col("rating_raw")) == "five", 5)
    .otherwise(None).cast("int"),
)

clean_df = df.select("title", "price", "rating", "in_stock", "category") \
    .dropna(subset=["title", "price"])

clean_df = clean_df.dropDuplicates(["title", "category"])

clean_df.write \
    .mode("overwrite") \
    .json("hdfs://localhost:9000/data/clean/books")

print("✅ Đã xử lý và lưu dữ liệu sạch (Spark)")

spark.stop()
