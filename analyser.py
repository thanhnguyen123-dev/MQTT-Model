import paho.mqtt.client as mqtt
import time
import sys
from publisher import WAIT_DURATION
from utils import Utils

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

        # Test parameters
        # self.pub_qos_levels = [0, 1, 2]
        self.pub_qos_levels = [0]
        self.delays_ms = [100]
        self.message_sizes_bytes = [1000]
        self.instance_counts_active = [1]
        self.analyser_sub_qos_levels = [0] 

        self.received_messages_current_test = []
        self.sys_messages_current_test = {} 
        self.connected = False

        # Attributes to store the parameters of the currently running test
        # These will be updated by run_all_tests and used by on_message for filtering
        self.current_test_pub_qos = None # QoS for publisher
        self.current_test_delay = None # Delay for publisher
        self.current_test_msg_size = None # Message size for publisher
        self.current_test_instance_count = None # Number of instances for publisher
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
        payload_str = None 

        try:
            payload_str = msg.payload.decode('utf-8')
        except UnicodeDecodeError as e:
            client_id_str = client._client_id.decode() if isinstance(client._client_id, bytes) else client._client_id
            try:
                payload_str = msg.payload.decode('latin-1')
                print(f"Analyser ({client_id_str}): Successfully decoded with latin-1 as fallback.")
            except Exception as e2:
                print(f"Analyser ({client_id_str}): Also failed to decode with latin-1. Error: {e2}. Skipping message processing for this message.")
                return 
        except Exception as e: 
            client_id_str = client._client_id.decode() if isinstance(client._client_id, bytes) else client._client_id
            print(f"Analyser ({client_id_str}): An unexpected error occurred during payload decoding on topic {topic}. Error: {e}. Skipping message.")
            return


        if topic.startswith("$SYS/"):
            if topic not in self.sys_messages_current_test:
                self.sys_messages_current_test[topic] = []
            self.sys_messages_current_test[topic].append({'payload': payload_str, 'timestamp': time.time()})
            return

        if topic.startswith("counter/"):
            parsed_topic_data = self.parse_data_topic(topic)
            if not parsed_topic_data:
                return

            if payload_str is None:
                client_id_str = client._client_id.decode() if isinstance(client._client_id, bytes) else client._client_id
                print(f"Analyser ({client_id_str}): Payload is None for topic {topic}, cannot process. This might follow a decoding error.")
                return

            if (parsed_topic_data["pub_qos"] == self.current_test_pub_qos and
                parsed_topic_data["delay"] == self.current_test_delay and
                parsed_topic_data["msg_size"] == self.current_test_msg_size and
                parsed_topic_data["instance_id"] <= self.current_test_instance_count):

                try:
                    payload_content = payload_str.split(':')
                    if len(payload_content) == 3:
                        pub_message_counter = int(payload_content[0])
                        pub_message_timestamp = float(payload_content[1])
                
                
                        self.received_messages_current_test.append({
                            'topic': topic,
                            'parsed_instance_id': parsed_topic_data["instance_id"],
                            'publisher_message_counter': pub_message_counter,
                            'publisher_message_timestamp': pub_message_timestamp,
                            # 'payload': payload_str,
                            'analyser_timestamp_received': time.time(),
                            'qos_delivered': msg.qos
                        })

                except (ValueError, IndexError) as e:
                    # Handle cases where payload_str is not in the expected "counter:timestamp[:padding]" format
                    client_id_str = client._client_id.decode() if isinstance(client._client_id, bytes) else client._client_id
                    print(f"Analyser ({client_id_str}): Error parsing payload content '{payload_str}' on topic {topic}. Error: {e}. Skipping this message.")
        else:
            print(f"  Received unexpected message on topic: {topic}")


    def calculate_statistics(self):
        """
        Calculates statistics for the current test.
        """
        stats = {}
        num_received = len(self.received_messages_current_test)

        # Calculate total mean rate
        stats['total_mean_rate'] = num_received / WAIT_DURATION if num_received > 0 else 0

        # Calculate the message lost rate
        messages_by_instance = {}
        for msg_data in self.received_messages_current_test:
            instance_id = msg_data['parsed_instance_id']
            if instance_id not in messages_by_instance:
                messages_by_instance[instance_id] = []
            messages_by_instance[instance_id].append(msg_data['publisher_message_counter'])
        

        all_instances_loss_percentages = []

        for instance_id in range(1, self.current_test_instance_count + 1):
            received_counters = sorted(messages_by_instance.get(instance_id, []))
            actual_messages = len(set(received_counters))

            expected_messages = 0
            if self.current_test_delay > 0:
                expected_messages  = int(round(WAIT_DURATION * 1000 / self.current_test_delay))
            else:
                if actual_messages > 0:
                    max_counter = max(received_counters)
                    expected_messages = max_counter + 1
                else:
                    if actual_messages == 0:
                        expected_messages = 1
                    else:
                        expected_messages = actual_messages

            loss_percentage = 0
            if expected_messages > 0:
                lost_messages = expected_messages - actual_messages
                loss_percentage = (lost_messages / expected_messages) * 100

            elif actual_messages == 0:
                pass

            lost_percentage = max(0.0, min(100.0, loss_percentage))
            all_instances_loss_percentages.append(lost_percentage)
        
        average_loss_rate = 0.0
        if self.current_test_instance_count > 0 and all_instances_loss_percentages:
            average_loss_rate = sum(all_instances_loss_percentages) / self.current_test_instance_count

        stats['average_loss_rate'] = average_loss_rate
        return stats


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
            self.connected = False

            try:
                self.client.connect(self.broker_address, self.broker_port)
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
                                
                            wait_duration = WAIT_DURATION + 5
                            print(f"Analyser: Waiting {wait_duration} seconds for test data...")
                            time.sleep(wait_duration)
                            print("Analyser: Test wait period finished.")

                            current_test_stats = self.calculate_statistics()

                            print(f"--- Test Summary (PubQoS={self.current_test_pub_qos}, Delay={self.current_test_delay}, Size={self.current_test_msg_size}, Inst={self.current_test_instance_count}, SubQoS={self.current_analyser_data_sub_qos}) ---")
                            print(f"  Received {len(self.received_messages_current_test)} relevant data messages.")
                            
                            print(f"  Total Mean Rate: {current_test_stats['total_mean_rate']:.2f} messages/second")
                            print(f"  Average Loss Rate: {current_test_stats['average_loss_rate']:.2f}%")
                            # temporary print
                            # for i, data_items in enumerate(self.received_messages_current_test[:3]):
                            #     print(f" Msg {i}: {data_items}")
                            # if len(self.received_messages_current_test) > 3:
                            #     print(f" ... and {len(self.received_messages_current_test) - 3} more messages.")
                           

                           # statistics calculation

            if self.client and self.client.is_connected():
                self.client.loop_stop()
                self.client.disconnect()
                print(f"Analyser ({self.client._client_id.decode()}): Disconnected after suite for sub_qos={sub_qos}.")
            time.sleep(1)

        print("\n===== All Test Suites Completed =====")


if __name__ == "__main__":
    analyser = Analyser()
    analyser.run_all_tests()
