import paho.mqtt.client as mqtt
import time
import threading
import sys 
from utils import Utils

class Publisher:
    """
    Represents a single instance of an MQTT publisher client
    that listens for commands and publishes data bursts.
    """
    WAIT_DURATION = 30

    def __init__(self, instance_id, broker_address="localhost", broker_port=1883):
        if not 1 <= instance_id <= 10:
            raise ValueError("Instance ID must be between 1 and 10")

        self.instance_id = instance_id
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.client_id = f"publisher_{self.instance_id:02d}" 

        # Default configuration, will be updated later
        self.qos = 0
        self.delay = 100 
        self.messagesize = 0
        self.instance_count = 1 
        self.active_thread = None 

        # Subscribed topics
        self.request_topics_to_subscribe = [
            ("request/qos"),
            ("request/delay"),
            ("request/messagesize"),
            ("request/instancecount"),
            ("request/go")
        ]

        # MQTT Client Setup
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        

    def on_connect(self, client, userdata, flags, reason_code, properties):
        """Callback for when the client connects to the broker."""
        print(f"Instance {self.instance_id}: Connected to MQTT Broker!")
        for topic in self.request_topics_to_subscribe:
            self.client.subscribe(topic)


    def on_message(self, client, userdata, msg):
        """Callback for when a message is received on a subscribed topic."""

        payload = msg.payload.decode()
        topic = msg.topic

        if topic == "request/qos":
            try:
                self.qos = int(payload)
            except ValueError:
                print(f"Instance {self.instance_id}: Invalid QoS value: {payload}")
        elif topic == "request/delay":
            try:
                self.delay = int(payload)
            except ValueError:
                print(f"Instance {self.instance_id}: Invalid delay value: {payload}")
        elif topic == "request/messagesize":
            try:
                self.messagesize = int(payload)
            except ValueError:
                print(f"Instance {self.instance_id}: Invalid messagesize value: {payload}")
        elif topic == "request/instancecount":
            try:
                self.instance_count = int(payload)
            except ValueError:
                print(f"Instance {self.instance_id}: Invalid instancecount value: {payload}")
        elif topic == "request/go":
            if self.instance_id <= self.instance_count:
                if self.active_thread and self.active_thread.is_alive():
                    print(f"Instance {self.instance_id}: Warning - Previous burst thread still running.")
                else:
                    self.active_thread = threading.Thread(
                        target=self.publish_message,
                        args=(self.qos, self.delay, self.messagesize),
                        daemon=True 
                    )
                    self.active_thread.start()



    def publish_message(self, qos_to_use, delay_to_use, messagesize_to_use):
        """
        Function executed in a separate thread to publish data for 30 seconds.
        """
        start_time = time.time()
        counter = 0
        x_sequence = 'x' * messagesize_to_use
        data_topic = f"counter/{self.instance_id}/{qos_to_use}/{delay_to_use}/{messagesize_to_use}"

        formatted_time = Utils.format_time(start_time)
        print(f"Instance {self.instance_id}: Publishing data to topic: {data_topic} at time: {formatted_time}")

        try:
            while time.time() - start_time < Publisher.WAIT_DURATION:
                if not self.client.is_connected():
                    print(f"Instance {self.instance_id}: Client disconnected during burst. Stopping thread.")
                    break

                current_timestamp = time.time()
                payload_string = f"{counter}:{current_timestamp}:{x_sequence}"

                self.client.publish(topic=data_topic, payload=payload_string, qos=qos_to_use)

                counter += 1
                time.sleep(delay_to_use / 1000)
            
            print(f"Instance {self.instance_id}: Publishing burst completed at time: {Utils.format_time(time.time())}")
        except Exception as e:
            print(f"Instance {self.instance_id}: Error during publishing burst: {e}")


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
