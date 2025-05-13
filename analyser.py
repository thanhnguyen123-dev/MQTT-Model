import paho.mqtt.client as mqtt
import time
import sys
# from collections import defaultdict # Will be useful for storing results later

class Analyser:
    """
    Controls MQTT publisher instances by iterating through a full suite of tests
    and (currently) prints the data they produce.
    (Step 3.2 Implementation)
    """
    def __init__(self, broker_address="localhost", broker_port=1883):
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.client_id_base = "assignment_analyser_s3_2" # Base client ID
        self.client = None # Client will be created/recreated in the loop

        # --- Test Parameters ---
        self.pub_qos_levels = [0, 1, 2]
        self.delays_ms = [0, 100]
        self.message_sizes_bytes = [0, 1000, 4000]
        self.instance_counts_active = [1, 5, 10]
        self.analyser_sub_qos_levels = [0, 1, 2] # Analyser's subscription QoS

        # --- State for current test ---
        self.current_data_topic_subscribed = None
        self.received_messages_current_test = []
        self.connected = False # Flag to track connection status

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        """Callback when connection is established."""
        if reason_code == 0:
            print(f"Analyser ({client._client_id.decode()}): Connected successfully!")
            self.connected = True
        else:
            print(f"Analyser ({client._client_id.decode()}): Failed to connect, return code {reason_code}\n")
            self.connected = False
            # If connection fails here, the loop in run_tests will handle it

    def _on_message(self, client, userdata, msg):
        """Callback when a message is received (should be data)."""
        # For now, just check if the topic seems like a data topic
        # A more robust check could be done if subscribing to multiple wildcards
        if msg.topic.startswith("counter/"):
             payload = msg.payload.decode()
             print(f"  Received data: Topic='{msg.topic}', Payload='{payload}'")
             self.received_messages_current_test.append({
                 'topic': msg.topic,
                 'payload': payload,
                 'timestamp_received': time.time(), # Record analyser-side arrival time
                 'qos': msg.qos # QoS the message was delivered with
             })
        # else:
            # print(f"  Received unexpected message on topic: {msg.topic}")


    def _on_subscribe(self, client, userdata, mid, reason_code_list, properties):
        """Callback for subscription confirmation."""
        # The reason_code_list corresponds to the list of topics in the subscribe call
        # For a single topic subscription, we check reason_code_list[0]
        if len(reason_code_list) > 0 and reason_code_list[0].is_failure:
            print(f"Analyser ({client._client_id.decode()}): Failed to subscribe to {self.current_data_topic_subscribed}, reason: {reason_code_list[0]}")
        else:
            # If subscribing to multiple topics, mid is a list of MIDs
            # For a single topic, mid is an int
            print(f"Analyser ({client._client_id.decode()}): Successfully subscribed to {self.current_data_topic_subscribed} (MID: {mid})")


    def _on_publish(self, client, userdata, mid, reason_code, properties):
        """Callback for publish confirmation (useful for QoS > 0 commands)."""
        # This callback is for when the broker acknowledges our publish (for QoS > 0)
        # print(f"  Command published (MID: {mid}, RC: {reason_code})") # Can be verbose
        pass

    def _publish_command(self, topic, payload, qos=1, retain=False):
        """Helper function to publish commands."""
        if not self.client or not self.client.is_connected():
            print("Error: Analyser client not connected. Cannot publish command.")
            return False
        # print(f"  Publishing command: Topic='{topic}', Payload='{payload}', QoS={qos}")
        
        msg_info = self.client.publish(topic, str(payload), qos=qos, retain=False)
        
        if qos > 0: # For QoS 1 and 2, wait for acknowledgment
            try:
                msg_info.wait_for_publish(timeout=5.0) # Wait for PUBACK/PUBCOMP
                if msg_info.rc != mqtt.MQTT_ERR_SUCCESS:
                    print(f"Warning: Publish command {topic}={payload} (QoS {qos}) not confirmed. RC: {msg_info.rc}")
                    return False
            except ValueError: # Can happen if disconnected
                print(f"Warning: Disconnected while waiting for publish confirmation for {topic}={payload}")
                return False
            except RuntimeError as e: # Can happen if loop isn't running or other issues
                print(f"Warning: Runtime error waiting for publish confirmation for {topic}={payload}: {e}")
                return False
        return True

    def run_all_tests(self):
        """Connects, runs all test cases, and disconnects."""
        
        for sub_qos in self.analyser_sub_qos_levels:
            print(f"\n{'='*10} Starting Test Suite for Analyser Subscription QoS = {sub_qos} {'='*10}")

            # --- Setup Client for this Subscription QoS ---
            if self.client and self.client.is_connected():
                self.client.loop_stop()
                self.client.disconnect()
                print(f"Analyser: Disconnected previous client instance.")
            
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"{self.client_id_base}_subqos{sub_qos}")
            self.client.on_connect = self._on_connect
            self.client.on_message = self._on_message
            self.client.on_subscribe = self._on_subscribe
            self.client.on_publish = self._on_publish
            self.connected = False # Reset connection flag

            try:
                print(f"Analyser ({self.client._client_id.decode()}): Attempting to connect...")
                self.client.connect(self.broker_address, self.broker_port, keepalive=60)
                self.client.loop_start()

                connection_timeout = 10
                start_conn_time = time.time()
                while not self.connected and time.time() - start_conn_time < connection_timeout:
                    time.sleep(0.1)
                
                if not self.connected:
                    print(f"Analyser ({self.client._client_id.decode()}): Connection failed or timed out for sub_qos={sub_qos}. Skipping this suite.")
                    if self.client: self.client.loop_stop() # Ensure loop is stopped
                    continue # Skip to the next sub_qos level
            
            except Exception as e:
                print(f"Analyser ({self.client._client_id.decode()}): Error connecting client for sub_qos={sub_qos}: {e}")
                if self.client: self.client.loop_stop()
                continue


            # --- Iterate through all test parameter combinations ---
            for pub_qos in self.pub_qos_levels:
                for delay in self.delays_ms:
                    for msg_size in self.message_sizes_bytes:
                        for instance_count in self.instance_counts_active:
                            print(f"\n--- Running Test: PubQoS={pub_qos}, Delay={delay}ms, Size={msg_size}, Instances={instance_count} (AnalyserSubQoS={sub_qos}) ---")

                            self.received_messages_current_test.clear()

                            # 1. Determine and Manage Data Topic Subscription
                            # We use a wildcard to get data from all active instances for this config
                            new_data_topic_wildcard = f"counter/{pub_qos}/{delay}/{msg_size}"

                            if self.current_data_topic_subscribed and self.current_data_topic_subscribed != new_data_topic_wildcard:
                                print(f"Analyser: Unsubscribing from {self.current_data_topic_subscribed}")
                                self.client.unsubscribe(self.current_data_topic_subscribed)
                                time.sleep(0.5) # Give broker time to process unsubscribe

                            if self.current_data_topic_subscribed != new_data_topic_wildcard:
                                print(f"Analyser: Subscribing to data topic: {new_data_topic_wildcard} with QoS {sub_qos}")
                                self.client.subscribe(new_data_topic_wildcard, qos=sub_qos)
                                self.current_data_topic_subscribed = new_data_topic_wildcard
                                time.sleep(1) # Allow time for subscription to be acknowledged
                            else:
                                print(f"Analyser: Already subscribed to {self.current_data_topic_subscribed}")


                            # 2. Send configuration commands to publishers
                            print("Analyser: Sending configuration commands...")
                            cmd_success = True
                            cmd_success &= self._publish_command("request/qos", pub_qos)
                            cmd_success &= self._publish_command("request/delay", delay)
                            cmd_success &= self._publish_command("request/messagesize", msg_size)
                            cmd_success &= self._publish_command("request/instancecount", instance_count)
                            
                            if not cmd_success:
                                print("Error sending one or more configuration commands. Skipping this test.")
                                continue

                            time.sleep(1.5) # Allow publishers time to process config

                            # 3. Send 'go' signal
                            print("Analyser: Sending 'go' signal...")
                            if not self._publish_command("request/go", "start_burst"): # Payload can be anything
                                print("Error sending 'go' signal. Skipping this test.")
                                continue
                                
                            # 4. Wait for the test duration + buffer
                            wait_duration = 35 # 30s burst + 5s buffer
                            print(f"Analyser: Waiting {wait_duration} seconds for test data...")
                            time.sleep(wait_duration)
                            print("Analyser: Test wait period finished.")

                            # 5. Process Results (Placeholder for now)
                            print(f"--- Test Summary (PubQoS={pub_qos}, Delay={delay}, Size={msg_size}, Inst={instance_count}, SubQoS={sub_qos}) ---")
                            print(f"  Received {len(self.received_messages_current_test)} data messages.")
                            # --- THIS IS WHERE YOU'LL ADD STATISTICS CALCULATION LATER ---
                            # Example: for msg_data in self.received_messages_current_test:
                            #             print(f"    Payload: {msg_data['payload']}")

            # End of inner loops (test parameters)
            if self.client and self.client.is_connected():
                self.client.loop_stop()
                self.client.disconnect()
                print(f"Analyser ({self.client._client_id.decode()}): Disconnected after suite for sub_qos={sub_qos}.")
            time.sleep(1) # Small pause before next sub_qos suite

        print("\n===== All Test Suites Completed =====")


# --- Main Execution ---
if __name__ == "__main__":
    analyser = Analyser()
    analyser.run_all_tests()
