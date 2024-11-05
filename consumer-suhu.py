from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("SensorSuhuStreaming") \
    .getOrCreate()

# Definisikan skema data suhu
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("suhu", IntegerType(), True)
])

# Baca data dari Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu") \
    .load()

# Parsing data JSON dari Kafka dan pilih kolom suhu yang melebihi 80°C
sensor_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("suhu") > 80)

# Cetak hasil yang melebihi 80°C sebagai peringatan
query = sensor_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
