import paho.mqtt.client as mqtt

# Define MQTT broker details
BROKER_ADDRESS = "localhost"  # Or "127.0.0.1"
BROKER_PORT = 1883
REQUEST_TOPICS = [
    "request/qos", 
    "request/delay", 
    "request/messagesize", 
    "request/instancecount"
]# Must match the publisher's topic

# Optional: Client ID. If not set, the library generates one.
# Important: If running multiple subscribers or publishers on the same machine
# for testing, ensure their client IDs are unique.
CLIENT_ID = "simple_subscriber_client"


# --- Callback Functions ---
def on_connect(client, userdata, flags, reason_code, properties):
    """
    Callback for when the client receives a CONNACK response from the server.
    """
    if reason_code == 0:
        print(f"Connected to MQTT Broker at {BROKER_ADDRESS}!")
        # Subscribe to the topic once connected
        # Arguments: topic, qos
        for topic in REQUEST_TOPICS:
            client.subscribe(topic, qos=0)
            print(f"Subscribed to topic: '{topic}'")
    else:
        print(f"Failed to connect, return code {reason_code}\n")

def on_message(client, userdata, msg):
    """
    Callback for when a PUBLISH message is received from the server.
    """
    print(f"Received message: '{msg.payload.decode()}' on topic '{msg.topic}' "
          f"with QoS {msg.qos}")

def on_subscribe(client, userdata, mid, reason_code_list, properties):
    """
    Callback for when the SUBSCRIBE request has been processed.
    """
    if len(reason_code_list) > 0 and reason_code_list[0].is_failure:
        print(f"Failed to subscribe: {reason_code_list[0]}")
    else:
        print(f"Successfully subscribed with MID {mid}")

# Create a new MQTT client instance
# Using CallbackAPIVersion.VERSION2 is recommended for new projects
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID)

# Assign callback functions
client.on_connect = on_connect
client.on_message = on_message
client.on_subscribe = on_subscribe # Optional, but good for feedback

try:
    # Connect to the broker
    # Arguments: host, port, keepalive (seconds)
    client.connect(BROKER_ADDRESS, BROKER_PORT, keepalive=60)

    # Start the blocking network loop.
    # This loop processes network traffic, dispatches callbacks, and handles reconnecting.
    # It will block until client.disconnect() is called or an unhandled error occurs.
    print("Starting subscriber loop, waiting for messages...")
    client.loop_forever()

except KeyboardInterrupt:
    print("Subscriber interrupted by user.")
except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Disconnect cleanly
    client.disconnect()
    print("Disconnected from broker.")