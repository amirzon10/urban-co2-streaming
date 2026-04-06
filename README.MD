# Project Setup & Reproducibility

This project can be run either using pre-configured Docker images or by building the system from scratch.

## Option 1: Pre-configured Environment

Use this option to reproduce the environment exactly as submitted.

### Load Docker Images
Run the following commands (images must be in the `exports` folder):

docker load -i exports/saved_influx.tar  
docker load -i exports/saved_grafana.tar  

### Start the System
Navigate to the `docker` directory and run:

docker compose up -d  

### Result
- InfluxDB is pre-initialized with the `iot_data` bucket and API token  
- Grafana is pre-configured with the dashboard and data source  
- No additional setup is required  

## Option 2: Build from Scratch

### Start the Stack
From the `docker` directory:

docker compose up -d  

### Initialize InfluxDB
1. Open http://localhost:8086  
2. Create:
   - User  
   - Organization: MyOrg  
   - Bucket: iot_data  
3. Generate an API token under Data → API Tokens  

Add the token to:
- src/producer.py  
- src/processor.py  

### Install Dependencies

pip install -r src/requirements.txt  

### Run the Services

Start the stream processor:

python src/processor.py worker  

Start the producer:

python src/producer.py  

### Configure Grafana
1. Open http://localhost:3000  
2. Add InfluxDB as a data source (Flux)  
3. Use URL: http://code-influxdb-1:8086  
4. Import `urban-emissions-hub.json` from the `exports` folder  

## Export Files

Available at (NCI access only):  
https://studentncirl-my.sharepoint.com/:f:/g/personal/x22120921_student_ncirl_ie/IgDRtNIZt8GhQ6-hnKXD5FUfAYvSevUEPGPCDkBGY2zNMiU?e=wQAMDU
