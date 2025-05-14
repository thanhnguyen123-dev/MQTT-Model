import paho.mqtt.client as mqtt
import time
import sys

class Analyser:
    """
    Controls MQTT publisher instances by iterating through a full suite of tests,
    subscribing to 'counter/#' and '$SYS/#', and filtering relevant messages.
    """
    def __init__(self, broker_address="localhost", broker_port=1883):
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.client_id_base = "analyser_main" # Base client ID
        self.client = None

        # --- Test Parameters ---
        self.pub_qos_levels = [0, 1, 2]
        self.delays_ms = [0, 100]
        self.message_sizes_bytes = [0, 1000, 4000]
        self.instance_counts_active = [1, 5, 10]
        self.analyser_sub_qos_levels = [0, 1, 2] # Analyser's subscription QoS for data

        # --- State for current test ---
        self.received_messages_current_test = []
        self.sys_messages_current_test = {} # To store relevant $SYS messages
        self.connected = False

        # Attributes to store the parameters of the currently running test
        # These will be updated by run_all_tests and used by _on_message for filtering
        self.current_test_pub_qos = None
        self.current_test_delay = None
        self.current_test_msg_size = None
        self.current_test_instance_count = None
        self.current_analyser_data_sub_qos = None # QoS for counter/# subscription

    def parse_data_topic(self, topic_str):
        """
        Parses a data topic string like 'counter/instance/qos/delay/msg_size'
        Returns a dictionary with parsed values or None if parsing fails.
        """
        parts = topic_str.split('/')
        if len(parts) == 5 and parts[0] == 'counter':
            try:
                return {
                    "instance_id": int(parts[1]),
                    "pub_qos": int(parts[2]),
                    "delay": int(parts[3]),
                    "msg_size": int(parts[4])
                }
            except ValueError:
                return None
        return None


    def on_connect(self, client, userdata, flags, reason_code, properties):
        """Callback when connection is established."""
        if reason_code == 0:
            print(f"Analyser ({client._client_id.decode()}): Connected successfully!")
            self.connected = True
            client.subscribe("counter/#", qos=self.current_analyser_data_sub_qos)
            client.subscribe("$SYS/#", qos=0)   
        else:
            print(f"Analyser ({client._client_id.decode()}): Failed to connect, return code {reason_code}\n")
            self.connected = False

    def on_message(self, client, userdata, msg):
        """Callback when a message is received."""
        topic = msg.topic
        payload = msg.payload.decode()

        if topic.startswith("$SYS/"):
            if topic not in self.sys_messages_current_test:
                self.sys_messages_current_test[topic] = []
            self.sys_messages_current_test[topic].append({'payload': payload, 'timestamp': time.time()})
            return

        if topic.startswith("counter/"):
            parsed_topic_data = self.parse_data_topic(topic)
            if not parsed_topic_data:
                # print(f"  Ignoring malformed data topic: {topic}")
                return

            # Filter: Only process messages that match the current test parameters
            if (parsed_topic_data["pub_qos"] == self.current_test_pub_qos and
                parsed_topic_data["delay"] == self.current_test_delay and
                parsed_topic_data["msg_size"] == self.current_test_msg_size and
                parsed_topic_data["instance_id"] <= self.current_test_instance_count):
                
                # print(f"  Received relevant data: Topic='{topic}', Payload='{payload}'")
                self.received_messages_current_test.append({
                    'topic': topic, # Full topic for detailed analysis
                    'parsed_instance_id': parsed_topic_data["instance_id"],
                    'payload': payload,
                    'timestamp_received': time.time(),
                    'qos_delivered': msg.qos # QoS the message was delivered with to analyser
                })
            # else:
                # Optional: Log if messages are received but filtered out
                # print(f"  Filtering out data from topic: {topic} (doesn't match current test params)")
        # else:
            # print(f"  Received unexpected message on topic: {topic}")


    def on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        """Callback for subscription confirmation."""
        # reason_code_list contains result for each topic in the subscribe call
        # For multiple subscriptions in one call, need to check which one this mid refers to.
        # For simplicity, if any failure, print a general warning.
        # A more robust handler would map 'mid' to the topic(s) subscribed.
        for i, rc in enumerate(reason_code_list):
            if rc.is_failure:
                # This part is tricky as Paho doesn't directly tell you which topic failed for a given mid
                # when subscribing to multiple topics in one call or separate calls.
                # We just know a subscription associated with this mid had an issue.
                print(f"Analyser ({client._client_id.decode()}): Warning - A subscription failed or was partially successful (MID: {mid}, Reason: {rc}).")
                break
        # else:
            # print(f"Analyser ({client._client_id.decode()}): Subscription(s) acknowledged (MID: {mid})")


    def on_publish(self, client, userdata, mid, reason_code, properties):
        """Callback for publish confirmation."""
        pass # Keep it simple

    def publish_command(self, topic, payload, qos=1, retain=False):
        """Helper function to publish commands."""
        if not self.client or not self.client.is_connected():
            print("Error: Analyser client not connected. Cannot publish command.")
            return False
        
        msg_info = self.client.publish(topic, str(payload), qos=qos, retain=retain)
        
        if qos > 0:
            try:
                msg_info.wait_for_publish(timeout=5.0)
                if msg_info.rc != mqtt.MQTT_ERR_SUCCESS:
                    print(f"Warning: Publish command {topic}={payload} (QoS {qos}) not confirmed. RC: {msg_info.rc}")
                    return False
            except Exception as e: # Catch broader exceptions for wait_for_publish
                print(f"Warning: Exception waiting for publish confirmation for {topic}={payload}: {e}")
                return False
        return True

    def run_all_tests(self):
        """Connects, runs all test cases, and disconnects."""
        
        for sub_qos in self.analyser_sub_qos_levels:
            print(f"\n{'='*10} Starting Test Suite for Analyser Subscription QoS = {sub_qos} {'='*10}")
            self.current_analyser_data_sub_qos = sub_qos # Set QoS for 'counter/#' for this suite

            if self.client and self.client.is_connected():
                self.client.loop_stop()
                self.client.disconnect()
                print(f"Analyser: Disconnected previous client instance.")
            
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"{self.client_id_base}_subqos{sub_qos}")
            self.client.on_connect = self.on_connect
            self.client.on_message = self.on_message
            self.client.on_subscribe = self.on_subscribe
            self.client.on_publish = self.on_publish
            self.connected = False

            try:
                self.client.connect(self.broker_address, self.broker_port, keepalive=60)
                self.client.loop_start()

                connection_timeout = 10
                start_conn_time = time.time()
                while not self.connected and time.time() - start_conn_time < connection_timeout:
                    time.sleep(0.1)
                
                if not self.connected:
                    print(f"Analyser ({self.client._client_id.decode()}): Connection failed or timed out for sub_qos={sub_qos}. Skipping this suite.")
                    if self.client: self.client.loop_stop()
                    continue
            
            except Exception as e:
                print(f"Analyser ({self.client._client_id.decode()}): Error connecting client for sub_qos={sub_qos}: {e}")
                if self.client: self.client.loop_stop()
                continue

            for pub_qos_test_val in self.pub_qos_levels:
                self.current_test_pub_qos = pub_qos_test_val
                for delay_test_val in self.delays_ms:
                    self.current_test_delay = delay_test_val
                    for msg_size_test_val in self.message_sizes_bytes:
                        self.current_test_msg_size = msg_size_test_val
                        for instance_count_test_val in self.instance_counts_active:
                            self.current_test_instance_count = instance_count_test_val

                            print(f"\n--- Running Test: PubQoS={self.current_test_pub_qos}, Delay={self.current_test_delay}ms, "
                                  f"Size={self.current_test_msg_size}, Instances={self.current_test_instance_count} "
                                  f"(AnalyserSubQoS={self.current_analyser_data_sub_qos}) ---")

                            self.received_messages_current_test.clear()
                            self.sys_messages_current_test.clear()

                            print("Analyser: Sending configuration commands...")
                            cmd_success = True
                            cmd_success &= self.publish_command("request/qos", self.current_test_pub_qos)
                            cmd_success &= self.publish_command("request/delay", self.current_test_delay)
                            cmd_success &= self.publish_command("request/messagesize", self.current_test_msg_size)
                            cmd_success &= self.publish_command("request/instancecount", self.current_test_instance_count)
                            
                            if not cmd_success:
                                print("Error sending one or more configuration commands. Skipping this test.")
                                continue

                            time.sleep(1.5) 

                            print("Analyser: Sending 'go' signal...")
                            if not self.publish_command("request/go", "start_burst"):
                                print("Error sending 'go' signal. Skipping this test.")
                                continue
                                
                            wait_duration = 35 
                            print(f"Analyser: Waiting {wait_duration} seconds for test data...")
                            time.sleep(wait_duration)
                            print("Analyser: Test wait period finished.")

                            print(f"--- Test Summary (PubQoS={self.current_test_pub_qos}, Delay={self.current_test_delay}, Size={self.current_test_msg_size}, Inst={self.current_test_instance_count}, SubQoS={self.current_analyser_data_sub_qos}) ---")
                            print(f"  Received {len(self.received_messages_current_test)} relevant data messages.")
                            # --- Statistics Calculation will go here ---

            if self.client and self.client.is_connected():
                self.client.loop_stop()
                self.client.disconnect()
                print(f"Analyser ({self.client._client_id.decode()}): Disconnected after suite for sub_qos={sub_qos}.")
            time.sleep(1)

        print("\n===== All Test Suites Completed =====")

# --- Main Execution ---
if __name__ == "__main__":
    analyser = Analyser()
    analyser.run_all_tests()
