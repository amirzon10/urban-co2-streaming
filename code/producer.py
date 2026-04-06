import time
import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    api_version=(0, 10, 1),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    # 1. Simulate traffic (5 to 80 cars)
    cars = random.randint(5, 80)
    
    # 2. Base CO2 + Traffic Impact (higher cars = higher CO2)
    # Base is 380, each car adds roughly 1.5 units, plus some noise
    co2_level = 380 + (cars * 1.5) + random.uniform(-10, 10)
    
    data = {
        'sensor_id': 'DUBLIN_CITY_01',
        'aqi_value': round(co2_level, 2),
        'vehicle_count': cars,
        'timestamp': time.time()
    }
    
    producer.send('sensor-readings', data)
    print(f"Sent Data: {data}")
    time.sleep(1) # Send every second