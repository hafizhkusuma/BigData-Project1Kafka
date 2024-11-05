Zulfa Hafizh Kusuma / 5027221038
<br>Muhammad Rifqi Oktaviansyah / 5027221067

Write Up Big Data Apache Kafka 
Run zookeeper dan kafka server

![image](https://github.com/user-attachments/assets/a1c427be-c232-4001-8f3b-10b9934c3cdd) 
Gambar 1. Run Zookeeper Server

![image](https://github.com/user-attachments/assets/0d08cfcb-51f1-4b22-8332-b94a952ee054)
Gambar 2. Run Kafka Server 

Digunakan kode dibawah untuk mengirim data suhu dari producer ke broker 
# suhu.py
```
import time
import random
from kafka import KafkaProducer
import json


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


sensors = ['S1', 'S2', 'S3']


try:
    while True:
        for sensor_id in sensors:
            suhu = random.uniform(60, 100)  
            data = {
                'sensor_id': sensor_id,
                'suhu': round(suhu, 2)  
            }
            producer.send('sensor-suhu', data)
            print(f"Data dikirim: {data}")
        time.sleep(1)  
except KeyboardInterrupt:
    print("Producer dihentikan.")
finally:
    producer.close()
```
![image](https://github.com/user-attachments/assets/d407a27c-ed90-4eee-b9dc-328cf4d2e0ba)
![image](https://github.com/user-attachments/assets/b4c8f1f9-a69a-4caf-b337-47029d736f39)

Gambar 3. Hasil Run Kode

Mengolah data dengan pyspark 
Buat file baru dengan nama consumer-suhu.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Inisialisasi SparkSession
```
spark = SparkSession.builder \
    .appName("SensorSuhuStreaming") \
    .getOrCreate()
```

# Definisikan skema data suhu
```
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("suhu", IntegerType(), True)
])
```

# Baca data dari Kafka
```
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu") \
    .load()
```
# Parsing data JSON dari Kafka dan pilih kolom suhu yang melebihi 80°C
```
sensor_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("suhu") > 80)
```

# Cetak hasil yang melebihi 80°C sebagai peringatan
```
query = sensor_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
```

Kode tersebut menggunakan hasil sensor dari kafka dan mengolahnya dengan pyspark. Proses tersebut adalah untuk memfilter suhu yang berada diatas 80% dan print hasil di terminal. 

![image](https://github.com/user-attachments/assets/034bd990-5e64-46c4-815f-449c73a618b3)
<br>Gambar 4. Tampilan terminal 

![image](https://github.com/user-attachments/assets/7f83987c-e3e7-484c-98d0-f8bb6d644ac4)
<br>Gambar 5. Tampilan terminal 2
