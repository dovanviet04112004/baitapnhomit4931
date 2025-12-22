from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("BooksToScrapeToES") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4") \
    .config("spark.es.nodes", "localhost") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .config("spark.es.net.ssl", "false") \
    .getOrCreate()

# Đọc dữ liệu ĐÃ CLEAN từ HDFS (output của clean_books.py)
df = spark.read.json("hdfs://localhost:9000/data/clean/books")

# Chọn cột cần thiết, đổi tên cho ES nếu muốn
df_clean = df.select(
    col("title"),
    col("price"),
    col("rating"),
    col("in_stock"),
    col("category"),
)

# Lưu về HDFS dạng Parquet
df_clean.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/data/cleaned/books_cleaned.parquet"
)

# Ghi vào Elasticsearch (index: books)
df_clean.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "books") \
    .mode("overwrite") \
    .save()

spark.stop()
