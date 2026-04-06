Project Setup & Reproducibility

This project provides two setup options:

Using pre-configured Docker images (recommended for quick replication)
Building the system from scratch
Option 1: Pre-configured Environment (Recommended)

Use this option to reproduce the environment exactly as submitted.

Load Docker Images

Ensure the saved images are located in the exports directory, then run:

docker load -i exports/saved_influx.tar
docker load -i exports/saved_grafana.tar
Launch the System

Navigate to the /docker directory and execute:

docker compose up -d
Result
InfluxDB: Pre-initialized with the iot_data bucket and required API token
Grafana: Pre-configured with the Urban Emissions Hub dashboard and connected data source

No additional configuration is required.

Option 2: Build from Scratch

Use this option to manually set up the full infrastructure.

Launch the Stack

From the /docker directory:

docker compose up -d
Initialize InfluxDB
Open: http://localhost:8086
Create:
User
Organization: MyOrg
Bucket: iot_data
Generate an API token:
Navigate to Data → API Tokens
Create an All Access Token

Important:
Add the generated token to the INFLUX_TOKEN variable in:

src/producer.py
src/processor.py
Install Dependencies
pip install -r src/requirements.txt
Start Services
Stream Processor
python src/processor.py worker
Traffic Simulation (Producer)
python src/producer.py
Configure Grafana
Open: http://localhost:3000
Add a new data source:
Type: InfluxDB
Query Language: Flux
URL: http://code-influxdb-1:8086
Import the dashboard:
File: urban-emissions-hub.json (located in /exports)
Export Files

The exported Docker images and dashboard files are available at:

Only accessible to NCI domain members:
https://studentncirl-my.sharepoint.com/:f:/g/personal/x22120921_student_ncirl_ie/IgDRtNIZt8GhQ6-hnKXD5FUfAYvSevUEPGPCDkBGY2zNMiU?e=wQAMDU
