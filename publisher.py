import paho.mqtt.client as mqtt
import time
import threading
import sys 

class Publisher:
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
        self.delay = 100 
        self.messagesize = 0
        self.instance_count = 1 
        self.active_thread = None 

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
                if result != mqtt.MQTT_ERR_SUCCESS:
                    print(f"Instance {self.instance_id}: Failed to initiate subscription request, error code {result}")

            except Exception as e:
                 print(f"Instance {self.instance_id}: Error during subscribe call: {e}")

        else:
            print(f"Instance {self.instance_id}: Failed to connect, return code {reason_code}\n")


    def on_message(self, client, userdata, msg):
        """Callback for when a message is received on a subscribed topic."""
        payload = msg.payload.decode()
        topic = msg.topic

        # --- Update Configuration ---
        if topic == "request/qos":
            try:
                new_qos = int(payload)
                self.qos = new_qos
                print(f"Instance {self.instance_id}: Updated qos to: {self.qos} ({topic})")
            except ValueError:
                print(f"Instance {self.instance_id}: Invalid QoS value (not an integer): {payload}")
        elif topic == "request/delay":
            try:
                self.delay = int(payload)
                print(f"Instance {self.instance_id}: Updated delay to: {self.delay} ms ({topic})")
            except ValueError:
                print(f"Instance {self.instance_id}: Invalid delay value: {payload}")
        elif topic == "request/messagesize":
            try:
                self.messagesize = int(payload)
                print(f"Instance {self.instance_id}: Updated messagesize to: {self.messagesize} ({topic})")
            except ValueError:
                print(f"Instance {self.instance_id}: Invalid messagesize value: {payload}")
        elif topic == "request/instancecount":
             try:
                self.instance_count = int(payload)
                print(f"Instance {self.instance_id}: Updated instancecount to: {self.instance_count} ({topic})")
             except ValueError:
                print(f"Instance {self.instance_id}: Invalid instancecount value: {payload}")
        elif topic == "request/go":
            print(f"Instance {self.instance_id}: Received a signal, ready to publish! ({topic})")
            # Check if this instance should be active
            if self.instance_id <= self.instance_count:
                print(f"Instance {self.instance_id}: Is active (ID {self.instance_id} <= Count {self.instance_count}). Starting publish burst.")
                # Start the publishing burst in a separate thread
                # Ensure only one burst thread runs at a time per instance
                if self.active_thread and self.active_thread.is_alive():
                    print(f"Instance {self.instance_id}: Warning - Previous burst thread still running.")
                else:
                    self.active_thread = threading.Thread(
                        target=self.publishing_burst_thread,
                        args=(self.qos, self.delay, self.messagesize),
                        daemon=True 
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

                self.client.publish(topic=data_topic, payload=payload_string, qos=qos_to_use)

                counter += 1
                time.sleep(delay_to_use / 1000)

        except Exception as e:
             print(f"Instance {self.instance_id}: Error during publishing burst: {e}")
        finally:
            print(f"Instance {self.instance_id}: Finished 30-second burst. Published {counter} messages.")


    def run(self):
        """Connects the client and starts the main loop."""
        try:
            self.client.connect(self.broker_address, self.broker_port, keepalive=60)
            self.client.loop_forever() 

        except KeyboardInterrupt:
            print(f"Instance {self.instance_id}: Listener stopped by user.")
        except Exception as e:
            print(f"An error occurred in Instance {self.instance_id}: {e}")
        finally:
            self.client.disconnect()
            print(f"Instance {self.instance_id}: Disconnected.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python publisher.py <instance_id>")
        print("  <instance_id>: An integer between 1 and 10.")
        sys.exit(1)

    try:
        instance_id_arg = int(sys.argv[1])
        publisher = Publisher(instance_id=instance_id_arg)
        publisher.run()
    except ValueError:
        print(f"Error: Instance ID must be an integer. Received: {sys.argv[1]}")
        sys.exit(1)
    except Exception as e:
         print(f"Failed to start publisher instance {sys.argv[1]}: {e}")
         sys.exit(1)
