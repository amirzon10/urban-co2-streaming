import faust
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# 1. InfluxDB Config
INFLUX_TOKEN = "5o97p0vTpifC_rltYOdVXD36aoVPKCvvW033upGq9M23d4-FEKfkOCFLL1dojvlE9diY3PXhy-PST-a6SvAhcw==" 
INFLUX_ORG = "myorg"
INFLUX_BUCKET = "iot_data"
INFLUX_URL = "http://localhost:8086"

client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# 2. Faust App
app = faust.App('iot-analytics-v2', broker='kafka://localhost:9092')

# --- DATA SCHEMA ---
class Reading(faust.Record):
    sensor_id: str
    aqi_value: float      
    vehicle_count: int    
    timestamp: float

# --- TOPIC DEFINITION (This was missing!) ---
topic = app.topic('sensor-readings', value_type=Reading)

# 3. Analytics Logic
@app.agent(topic)
async def process(readings):
    batch = []
    async for reading in readings:
        batch.append(reading)
        
        # Process every 10 events
        if len(batch) >= 10:
            avg_aqi = sum(r.aqi_value for r in batch) / len(batch)
            avg_vehicles = sum(r.vehicle_count for r in batch) / len(batch)
            
            # Use the first sensor_id in the batch for the tag
            sensor_id = batch[0].sensor_id
            status = "CRITICAL" if avg_aqi > 450 else "NORMAL"
            
            try:
                # Send BOTH metrics to InfluxDB
                point = Point("environment_metrics") \
                    .tag("sensor_id", sensor_id) \
                    .tag("status", status) \
                    .field("avg_co2", avg_aqi) \
                    .field("avg_vehicles", avg_vehicles)
                
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
                print(f"✅ Window Processed | Avg CO2: {avg_aqi:.2f} | Avg Cars: {int(avg_vehicles)} | Status: {status}")
            except Exception as e:
                print(f"❌ Database Write Error: {e}")
            
            # Clear the batch for the next window
            batch = []

if __name__ == '__main__':
    app.main()