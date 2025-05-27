# MQTT 

## Setup and installation
Run the following commands to setup and install the necessary packages:
```bash
python3 -m venv mqtt_venv
source mqtt_venv/bin/activate
pip3 install paho-mqtt
pip3 install notebook pandas numpy matplotlib
chmod +x start_publishers.sh
```

## Running the tests

```bash
./start_publishers.sh
```