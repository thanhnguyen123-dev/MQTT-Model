import paho.mqtt.client as mqtt
import time

# Define MQTT broker details
BROKER_ADDRESS = "localhost"
BROKER_PORT = 1883

REQUEST_TOPICS_TO_SUBSCRIBE = [
    ("request/qos", 0),
    ("request/delay", 0),
    ("request/messagesize", 0),
    ("request/instancecount", 0),
    ("request/go", 0) # Added the 'go' topic as per assignment suggestion
]

CLIENT_ID = "assignment_publisher_listener" # Give it a distinct ID

# --- Configuration Storage (simple global variables for now) ---
# You'll refine this later, perhaps in a class
current_config = {
    "qos": 0,
    "delay": 100, # Default delay
    "messagesize": 0,
    "instancecount": 1,
    "go": False
}

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"Listener connected to MQTT Broker at {BROKER_ADDRESS}!")
        # Subscribe to all request topics
        # client.subscribe takes a list of topic-qos tuples or a single topic-qos
        for topic, qos_level in REQUEST_TOPICS_TO_SUBSCRIBE:
            client.subscribe(topic, qos=qos_level)
            print(f"Subscribed to control topic: '{topic}' with QoS {qos_level}")
    else:
        print(f"Listener failed to connect, return code {reason_code}\n")

def on_message(client, userdata, msg):
    global current_config # If using global variables
    payload = msg.payload.decode()
    topic = msg.topic
    print(f"Received command on topic: '{topic}' with payload: '{payload}'")

    # Basic parsing and storing of commands (you'll expand this)
    if topic == "request/qos":
        try:
            current_config["qos"] = int(payload)
            print(f"Updated qos to: {current_config['qos']}")
        except ValueError:
            print(f"Invalid QoS value: {payload}")
    elif topic == "request/delay":
        try:
            current_config["delay"] = int(payload)
            print(f"Updated delay to: {current_config['delay']}")
        except ValueError:
            print(f"Invalid delay value: {payload}")
    # Add similar elif blocks for messagesize, instancecount, and go
    # For 'go', you might just set a boolean flag
    elif topic == "request/go":
        print("Received 'go' signal!")
        # Here you would trigger the publishing burst in a more advanced version
        # For now, just acknowledging it is fine.


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID)
client.on_connect = on_connect
client.on_message = on_message # Make sure this is assigned

try:
    client.connect(BROKER_ADDRESS, BROKER_PORT, keepalive=60)
    print(f"'{CLIENT_ID}' attempting to connect and listen for commands...")
    client.loop_forever() # This will block and keep listening

except KeyboardInterrupt:
    print(f"'{CLIENT_ID}' listener stopped by user.")
except Exception as e:
    print(f"An error occurred in '{CLIENT_ID}': {e}")
finally:
    client.disconnect()
    print(f"'{CLIENT_ID}' disconnected.")