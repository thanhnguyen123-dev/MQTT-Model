import paho.mqtt.client as mqtt
import time
import sys
from publisher import WAIT_DURATION
from utils import Utils
import json
all_tests_results = []

class Analyser:
    """
    Controls MQTT publisher instances by iterating through a full suite of tests,
    subscribing to 'counter/#' and '$SYS/#', and filtering relevant messages.
    """
    def __init__(self, broker_address="localhost", broker_port=1883):
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.client_id_base = "analyser_main"   
        self.client = None

        # Test parameters
        self.pub_qos_levels = [0, 1, 2]
        self.delays_ms = [0, 100]
        self.message_sizes_bytes = [0, 1000, 4000]
        self.instance_counts_active = [1]
        self.analyser_sub_qos_levels = [0, 1, 2] 

        self.received_messages = []
        self.sys_messages = {} 
        self.connected = False
        
        # Attributes to store the parameters of the currently running test
        # These will be updated by run_all_tests and used by on_message for filtering
        self.pub_qos = None 
        self.delay = None 
        self.message_size = None 
        self.instance_count = None    
        self.qos = None 


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
            client.subscribe("counter/#", qos=self.qos)
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
            if topic not in self.sys_messages:
                self.sys_messages[topic] = []
            self.sys_messages[topic].append({'payload': payload_str, 'timestamp': time.time()})
            return

        if topic.startswith("counter/"):
            parsed_topic_data = self.parse_data_topic(topic)
            if not parsed_topic_data or payload_str is None:
                return

            if (parsed_topic_data["pub_qos"] == self.pub_qos and
                parsed_topic_data["delay"] == self.delay and
                parsed_topic_data["msg_size"] == self.message_size and
                parsed_topic_data["instance_id"] <= self.instance_count):

                try:
                    payload_content = payload_str.split(':')
                    if len(payload_content) == 3:
                        pub_message_counter = int(payload_content[0])
                        pub_message_timestamp = float(payload_content[1])
                
                
                        self.received_messages.append({
                            'topic': topic,
                            'parsed_instance_id': parsed_topic_data["instance_id"],
                            'publisher_message_counter': pub_message_counter,
                            'publisher_message_timestamp': pub_message_timestamp,
                            'analyser_timestamp_received': time.time(),
                            'qos_delivered': msg.qos
                        })

                except (ValueError, IndexError) as e:
                    client_id_str = client._client_id.decode() if isinstance(client._client_id, bytes) else client._client_id
                    print(f"Analyser ({client_id_str}): Error parsing payload content '{payload_str}' on topic {topic}. Error: {e}. Skipping this message.")
        else:
            print(f"  Received unexpected message on topic: {topic}")


    def calculate_statistics(self):
        """
        Calculates statistics for the current test.
        """
        num_received = len(self.received_messages)

        # Calculate total mean rate
        total_mean_rate = num_received / WAIT_DURATION if num_received > 0 else 0

        # Calculate the message lost rate
        messages_by_instance = {}
        for msg_data in self.received_messages:
            instance_id = msg_data['parsed_instance_id']
            if instance_id not in messages_by_instance:
                messages_by_instance[instance_id] = []
            messages_by_instance[instance_id].append(msg_data['publisher_message_counter'])
        

        all_instances_loss_percentages = []
        for instance_id in range(1, self.instance_count + 1):
            received_counters = sorted(messages_by_instance.get(instance_id, []))
            actual_count = len(set(received_counters))

            expected_count = 0
            if self.delay > 0:
                expected_count  = int(round(WAIT_DURATION * 1000 / self.delay))
            else:
                expected_count = max(received_counters) + 1 if actual_count > 0 else 1

            loss_percentage = 0
            if expected_count > 0:
                lost_count = expected_count - actual_count
                loss_percentage = (lost_count / expected_count) * 100

            lost_percentage = max(0.0, min(100.0, loss_percentage))
            all_instances_loss_percentages.append(lost_percentage)
    
        average_loss_rate = 0.0
        if self.instance_count > 0 and all_instances_loss_percentages:
            average_loss_rate = sum(all_instances_loss_percentages) / self.instance_count

        publisher_stats = {
            'instance_count': self.instance_count,
            'pub_qos': self.pub_qos,
            'delay': self.delay,
            'message_size': self.message_size,
            'sub_qos': self.qos,
            'total_mean_rate': total_mean_rate,
            'average_loss_rate': average_loss_rate,
            'total_messages_received': num_received,
        }

        all_tests_results.append(publisher_stats)
        return publisher_stats


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
            self.qos = sub_qos # Set QoS for 'counter/#' for this suite

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
                self.pub_qos = pub_qos_test_val
                for delay_test_val in self.delays_ms:
                    self.delay = delay_test_val
                    for msg_size_test_val in self.message_sizes_bytes:
                        self.message_size = msg_size_test_val
                        for instance_count_test_val in self.instance_counts_active:
                            self.instance_count = instance_count_test_val
                            print(f"\n--- Running Test: PubQoS={self.pub_qos}, Delay={self.delay}ms, "
                                  f"Size={self.message_size}, Instances={self.instance_count} "
                                  f"(AnalyserSubQoS={self.qos}) ---")

                            self.received_messages.clear()
                            self.sys_messages.clear()

                            print("Analyser: Sending configuration commands...")
                            cmd_success = True
                            cmd_success &= self.publish_command("request/qos", self.pub_qos)
                            cmd_success &= self.publish_command("request/delay", self.delay)
                            cmd_success &= self.publish_command("request/messagesize", self.message_size)
                            cmd_success &= self.publish_command("request/instancecount", self.instance_count)
                            
                            if not cmd_success:
                                print("Error sending one or more configuration commands. Skipping this test.")
                                continue

                            # time.sleep(1.5) 

                            print("Analyser: Sending 'go' signal...")
                            if not self.publish_command("request/go", "start_burst"):
                                print("Error sending 'go' signal. Skipping this test.")
                                continue
                                
                            wait_duration = WAIT_DURATION + 5
                            print(f"Analyser: Waiting {wait_duration} seconds for test data...")
                            time.sleep(wait_duration)
                            print("Analyser: Test wait period finished.")

                            current_test_stats = self.calculate_statistics()

                            print(f"--- Test Summary (PubQoS={self.pub_qos}, Delay={self.delay}, Size={self.message_size}, Inst={self.instance_count}, SubQoS={self.qos}) ---")
                            print(f"  Received {len(self.received_messages)} relevant data messages.")
                            
                            print(f"  Total Mean Rate: {current_test_stats['total_mean_rate']:.2f} messages/second")
                            print(f"  Average Loss Rate: {current_test_stats['average_loss_rate']:.2f} %")


            if self.client and self.client.is_connected():
                self.client.loop_stop()
                self.client.disconnect()
                print(f"Analyser ({self.client._client_id.decode()}): Disconnected after suite for sub_qos={sub_qos}.")
            # time.sleep(1)

        self.save_results_to_json()
        print("\n===== All Test Suites Completed =====")


    def save_results_to_json(self):
        """Saves all test results to a JSON file."""
        json_filename = "all_tests_results.json"
        with open(json_filename, "w") as f:
            json.dump(all_tests_results, f, indent=4)

if __name__ == "__main__":
    analyser = Analyser()
    analyser.run_all_tests()
