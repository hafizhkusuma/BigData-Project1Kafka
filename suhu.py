from kafka import KafkaProducer
import json
import time
import random

# Inisialisasi Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Fungsi untuk mensimulasikan data suhu
def generate_sensor_data(sensor_id):
    suhu = random.randint(60, 100)  # Simulasi suhu antara 60°C dan 100°C
    return {'sensor_id': sensor_id, 'suhu': suhu}

# Mengirim data suhu setiap detik ke topik Kafka
try:
    while True:
        for sensor_id in ['S1', 'S2', 'S3']:  # Tiga sensor berbeda
            data = generate_sensor_data(sensor_id)
            producer.send('sensor-suhu', data)
            print(f"Mengirim data: {data}")
        time.sleep(1)  # Tunggu 1 detik sebelum mengirim data berikutnya
except KeyboardInterrupt:
    producer.close()
