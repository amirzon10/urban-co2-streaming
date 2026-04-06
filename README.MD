# Project README: Setup & Reproducibility
## Option 1: imaging
Use this if you want to see the pre-configured environment exactly as submitted.

Load the Saved Images (images in export folder):

Bash
docker load -i exports/saved_influx.tar
docker load -i exports/saved_grafana.tar

Launch the Containers:
Navigate to the /docker folder and run:

Bash
docker compose up -d

Result: 
* InfluxDB: Already initialized with the bucket iot_data and the internal API token used by the scripts.

* Grafana: Already contains the "Urban Emissions Hub" dashboard and the pre-connected Data Source. No manual setup required.

## Option 2: from scracht
Use this if you want to build the infrastructure from the original images.

Launch the Stack:

Bash
docker compose up -d

Initialize InfluxDB (Manual Setup Required):

Open localhost:8086.

Create a user, organization (MyOrg), and bucket (iot_data).

Go to Data -> API Tokens and generate an "All Access Token."

Crucial: You must copy this token into the INFLUX_TOKEN variable inside both producer.py and processor.py.

Install Dependencies:

Bash
pip install -r src/requirements.txt


Start Stream Processor:

Bash
python src/processor.py worker



Start Traffic Simulation:

Bash
python src/producer.py

Connect Grafana:

Open localhost:3000.

Add "InfluxDB" as a Data Source (select Flux).

Use URL: http://code-influxdb-1:8086.

Import the urban-emissions-hub.json from the /exports folder.


# find export folder
Only members of the NCI domain can access it:
[text](https://studentncirl-my.sharepoint.com/:f:/g/personal/x22120921_student_ncirl_ie/IgDRtNIZt8GhQ6-hnKXD5FUfAYvSevUEPGPCDkBGY2zNMiU?e=wQAMDU)