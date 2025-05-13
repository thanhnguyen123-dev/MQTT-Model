import paho.mqtt.client as mqtt
import time
import threading
import sys 

class PublisherInstance:
    """
    Represents a single instance of an MQTT publisher client
    that listens for commands and publishes data bursts.
    """
    def __init__(self, instance_id, broker_address="localhost", broker_port=1883):
        if not 1 <= instance_id <= 10:
            raise ValueError("Instance ID must be between 1 and 10")

        self.instance_id = instance_id
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.client_id = f"publisher_{self.instance_id:02d}" 

        # --- Configuration Storage ---
        self.qos = 0
        self.delay = 100  # Default delay in ms
        self.messagesize = 0
        self.instance_count = 1 # How many instances should be active
        self.active_thread = None # To keep track of the publishing thread

        # --- Topics ---
        self.request_topics_to_subscribe = [
            ("request/qos", 0),
            ("request/delay", 0),
            ("request/messagesize", 0),
            ("request/instancecount", 0),
            ("request/go", 0)
        ]

        # MQTT Client Setup
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        

    def on_connect(self, client, userdata, flags, reason_code, properties):
        """Callback for when the client connects to the broker."""
        if reason_code == 0:
            print(f"Instance {self.instance_id}: Connected to MQTT Broker at {self.broker_address}!")
            # Subscribe to all request topics
            try:
                result, mid = client.subscribe(self.request_topics_to_subscribe)
                if result == mqtt.MQTT_ERR_SUCCESS:
                     print(f"Instance {self.instance_id}: Subscribing to control topics...")
                     # You could check individual results in the on_subscribe callback if needed
                else:
                     print(f"Instance {self.instance_id}: Failed to initiate subscription request, error code {result}")

            except Exception as e:
                 print(f"Instance {self.instance_id}: Error during subscribe call: {e}")

        else:
            print(f"Instance {self.instance_id}: Failed to connect, return code {reason_code}\n")

    def on_message(self, client, userdata, msg):
        """Callback for when a message is received on a subscribed topic."""
        payload = msg.payload.decode()
        topic = msg.topic
        print(f"Instance {self.instance_id}: Received command on topic: '{topic}' with payload: '{payload}'")

        # --- Update Configuration ---
        if topic == "request/qos":
            try:
                new_qos = int(payload)
                if new_qos in [0, 1, 2]:
                    self.qos = new_qos
                    print(f"Instance {self.instance_id}: Updated qos to: {self.qos}")
                else:
                    print(f"Instance {self.instance_id}: Invalid QoS value (must be 0, 1, or 2): {payload}")
            except ValueError:
                print(f"Instance {self.instance_id}: Invalid QoS value (not an integer): {payload}")
        elif topic == "request/delay":
            try:
                self.delay = int(payload)
                print(f"Instance {self.instance_id}: Updated delay to: {self.delay} ms")
            except ValueError:
                print(f"Instance {self.instance_id}: Invalid delay value: {payload}")
        elif topic == "request/messagesize":
            try:
                self.messagesize = int(payload)
                print(f"Instance {self.instance_id}: Updated messagesize to: {self.messagesize}")
            except ValueError:
                print(f"Instance {self.instance_id}: Invalid messagesize value: {payload}")
        elif topic == "request/instancecount":
             try:
                self.instance_count = int(payload)
                print(f"Instance {self.instance_id}: Updated instancecount to: {self.instance_count}")
             except ValueError:
                print(f"Instance {self.instance_id}: Invalid instancecount value: {payload}")

        # --- Handle 'go' Signal ---
        elif topic == "request/go":
            print(f"Instance {self.instance_id}: Received 'go' signal!")
            # Check if this instance should be active
            if self.instance_id <= self.instance_count:
                print(f"Instance {self.instance_id}: Is active (ID {self.instance_id} <= Count {self.instance_count}). Starting publish burst.")
                # Start the publishing burst in a separate thread
                # Ensure only one burst thread runs at a time per instance
                if self.active_thread and self.active_thread.is_alive():
                    print(f"Instance {self.instance_id}: Warning - Previous burst thread still running.")
                else:
                    # Pass necessary config values to the thread function
                    self.active_thread = threading.Thread(
                        target=self.publishing_burst_thread,
                        args=(self.qos, self.delay, self.messagesize),
                        daemon=True # Allows main program to exit even if thread is running
                    )
                    self.active_thread.start()
            else:
                 print(f"Instance {self.instance_id}: Is inactive (ID {self.instance_id} > Count {self.instance_count}). Doing nothing.")

    def publishing_burst_thread(self, qos_to_use, delay_to_use, messagesize_to_use):
        """
        Function executed in a separate thread to publish data for 30 seconds.
        """
        print(f"Instance {self.instance_id}: Thread started for publishing burst.")
        start_time = time.time()
        counter = 0
        x_sequence = 'x' * messagesize_to_use
        data_topic = f"counter/{self.instance_id}/{qos_to_use}/{delay_to_use}/{messagesize_to_use}"
        print(f"Instance {self.instance_id}: Publishing data to topic: {data_topic}")

        try:
            while time.time() - start_time < 30:
                # Check if client is still connected before publishing
                if not self.client.is_connected():
                    print(f"Instance {self.instance_id}: Client disconnected during burst. Stopping thread.")
                    break

                current_timestamp = time.time()
                payload_string = f"{counter}:{current_timestamp}:{x_sequence}"

                result = self.client.publish(topic=data_topic, payload=payload_string, qos=qos_to_use)
                print(f"Instance {self.instance_id}: Published message {counter} to topic {data_topic} with payload {payload_string}")
                # Optional: Check result.rc, especially for QoS 1/2
                # if result.rc != mqtt.MQTT_ERR_SUCCESS:
                #    print(f"Instance {self.instance_id}: Error publishing msg {counter}, code: {result.rc}")

                counter += 1

                if delay_to_use > 0:
                    time.sleep(delay_to_use / 1000.0)

        except Exception as e:
             print(f"Instance {self.instance_id}: Error during publishing burst: {e}")
        finally:
            print(f"Instance {self.instance_id}: Finished 30-second burst. Published {counter} messages.")


    def run(self):
        """Connects the client and starts the main loop."""
        try:
            print(f"Instance {self.instance_id} (Client ID: {self.client_id}): Connecting to broker...")
            self.client.connect(self.broker_address, self.broker_port, keepalive=60)
            print(f"Instance {self.instance_id}: Starting MQTT loop...")
            self.client.loop_forever() # Blocks until disconnect

        except KeyboardInterrupt:
            print(f"Instance {self.instance_id}: Listener stopped by user.")
        except Exception as e:
            print(f"An error occurred in Instance {self.instance_id}: {e}")
        finally:
            print(f"Instance {self.instance_id}: Disconnecting...")
            self.client.disconnect()
            print(f"Instance {self.instance_id}: Disconnected.")

# --- Main Execution ---
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python publisher.py <instance_id>")
        print("  <instance_id>: An integer between 1 and 10.")
        sys.exit(1)

    try:
        instance_id_arg = int(sys.argv[1])
        # Create and run the publisher instance
        publisher = PublisherInstance(instance_id=instance_id_arg)
        publisher.run()
    except ValueError:
        print(f"Error: Instance ID must be an integer. Received: {sys.argv[1]}")
        sys.exit(1)
    except Exception as e:
         print(f"Failed to start publisher instance {sys.argv[1]}: {e}")
         sys.exit(1)
