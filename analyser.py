import paho.mqtt.client as mqtt
import time
import sys
from publisher import Publisher
from utils import Utils
import json
from collections import Counter
from statistics import mean, stdev


class Analyser:
    """
    Controls MQTT publisher instances by iterating through a full suite of tests,
    subscribing to 'counter/#' and '$SYS/#', and filtering relevant messages.
    """
    all_tests_results = []
    WAIT_DURATION = Publisher.WAIT_DURATION

    def __init__(self, broker_address="localhost", broker_port=1883):
        self.broker_address = broker_address
        self.broker_port = broker_port  
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="Analyser")
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        self.received_messages = []
        self.sys_data = {
            'received_published_messages': [],
            'sent_published_messages': [],
            'dropped_published_messages': [],
            'memory_usage': []
        } 
        
        self.pub_qos = 0 
        self.delay = 0 
        self.message_size = 0 
        self.instance_count = 0    
        self.qos = 0 


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
        print(f"Analyser connected successfully!")
        client.subscribe("counter/#", qos=self.qos)
        client.subscribe("$SYS/#", qos=0)   


    def on_message(self, client, userdata, msg):
        """Callback when a message is received."""
        topic = msg.topic
        payload_str = None

        try:
            payload_str = msg.payload.decode('utf-8')
        except UnicodeDecodeError:
            payload_str = msg.payload.decode('utf-8', errors='ignore')

        if topic.startswith("$SYS/"):
            self.subscribe_to_sys_topics(topic, payload_str)
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


    def subscribe_to_sys_topics(self, topic: str, payload: str):
        """
        Subscribes to relevant system topics.
        """
        current_time = time.time()
        parsed_payload = None
        tuple_data = None

        try:
            parsed_payload = float(payload)
            tuple_data = (parsed_payload, current_time)
        except ValueError:
            return

        if topic == "$SYS/broker/load/publish/sent/1min":
            self.sys_data['sent_published_messages'].append(tuple_data)
        elif topic == "$SYS/broker/load/publish/received/1min":
            self.sys_data['received_published_messages'].append(tuple_data)
        elif topic == "$SYS/broker/load/publish/dropped/1min":
            self.sys_data['dropped_published_messages'].append(tuple_data)
        elif topic == "$SYS/broker/heap/current":
            self.sys_data['memory_usage'].append(tuple_data)
        else:
            return


    def calculate_statistics(self):
        """
        Calculates statistics for the current test.
        """
        num_received = len(self.received_messages)

        # Calculate total mean rate
        total_mean_rate = num_received / Analyser.WAIT_DURATION if num_received > 0 else 0

        instance_messages_map = {}
        for msg_data in self.received_messages:
            instance_id = msg_data['parsed_instance_id']
            if instance_id not in instance_messages_map:
                instance_messages_map[instance_id] = []
            instance_messages_map[instance_id].append({
                'counter': msg_data['publisher_message_counter'],
                'timestamp': msg_data['analyser_timestamp_received']
            })
        
        # Statistics for all instances
        all_instances_loss_percentages = []
        all_instances_out_of_order_percentages = []
        all_instances_duplicate_percentages = []
        all_instances_mean_gaps = []
        all_instances_stdev_gaps = []

        for instance_id in range(1, self.instance_count + 1):
            instance_messages = instance_messages_map.get(instance_id, [])
            received_counters = [msg['counter'] for msg in instance_messages]

            # Calculate the loss message percentage
            actual_count = len(set(received_counters))
            expected_count = 0
            if self.delay > 0:
                expected_count  = int(round(Analyser.WAIT_DURATION * 1000 / self.delay))
            else:
                expected_count = max(received_counters) + 1 if actual_count > 0 else 1

            loss_percentage = 0
            if expected_count > 0:
                lost_count = expected_count - actual_count
                loss_percentage = (lost_count / expected_count) * 100

            lost_percentage = max(0.0, min(100.0, loss_percentage))
            all_instances_loss_percentages.append(lost_percentage)

            # Calculate out of order message percentage
            out_of_order_count = 0
            for i in range(1, len(received_counters)):
                current_counter = received_counters[i]
                previous_counter = received_counters[i-1]
                if current_counter < previous_counter:
                    out_of_order_count += 1

            out_of_order_percentage = 0.0
            if len(received_counters) > 0:
                out_of_order_percentage = (out_of_order_count / len(received_counters)) * 100
            all_instances_out_of_order_percentages.append(out_of_order_percentage)

            # Calculate the duplicate message percentage
            duplicate_count = 0
            counter_counts = dict(Counter(received_counters))
            for counter, count in counter_counts.items():
                if count > 1:
                    duplicate_count += count - 1

            duplicate_percentage = 0.0
            if len(received_counters) > 0:
                duplicate_percentage = (duplicate_count / len(received_counters)) * 100
            all_instances_duplicate_percentages.append(duplicate_percentage)

            # Calculate the mean-intern-message-gap (timestamp difference) between consecutive messages
            gap_ms_for_instance = []
            if len(instance_messages) > 1:
                for i in range(len(instance_messages) - 1):
                    current_msg = instance_messages[i]
                    next_msg = instance_messages[i+1]

                    # check if the next message is the consecutive message
                    if next_msg['counter'] == current_msg['counter'] + 1:
                        gap_seconds = next_msg['timestamp'] - current_msg['timestamp']
                        gap_milliseconds = gap_seconds * 1000
                        gap_ms_for_instance.append(gap_milliseconds)
            
            mean_gap_for_instance = mean(gap_ms_for_instance) if gap_ms_for_instance else 0.0
            stdev_gap_for_instance = stdev(gap_ms_for_instance) if len(gap_ms_for_instance) >= 2 else 0.0
            all_instances_mean_gaps.append(mean_gap_for_instance)
            all_instances_stdev_gaps.append(stdev_gap_for_instance)

        # Calculate the average (per-publisher)
        average_loss_rate = mean(all_instances_loss_percentages) if all_instances_loss_percentages else 0.0
        average_out_of_order_rate = mean(all_instances_out_of_order_percentages) if all_instances_out_of_order_percentages else 0.0
        average_duplicate_rate = mean(all_instances_duplicate_percentages) if all_instances_duplicate_percentages else 0.0
        average_mean_gap = mean(all_instances_mean_gaps) if all_instances_mean_gaps else 0.0
        average_stdev_gap = mean(all_instances_stdev_gaps) if all_instances_stdev_gaps else 0.0

        publisher_stats = {
            'instance_count': self.instance_count,
            'pub_qos': self.pub_qos,
            'delay': self.delay,
            'message_size': self.message_size,
            'sub_qos': self.qos,
            'total_messages_received': num_received,
            'total_mean_rate': total_mean_rate,
            'average_loss_rate': average_loss_rate,
            'average_out_of_order_rate': average_out_of_order_rate,
            'average_duplicate_rate': average_duplicate_rate,
            'average_mean_gap': average_mean_gap,
            'average_stdev_gap': average_stdev_gap,
            'sys_data': self.sys_data
        }

        Analyser.all_tests_results.append(publisher_stats)
        return publisher_stats


    def publish_request(self, qos, delay, messagesize, instancecount):
        self.client.loop_start()
        self.client.publish("request/qos", qos, qos=self.qos)
        self.client.publish("request/delay", delay, qos=self.qos)
        self.client.publish("request/messagesize", messagesize, qos=self.qos)
        self.client.publish("request/instancecount", instancecount, qos=self.qos)
        self.client.publish("request/go", "start_burst", qos=self.qos)


    def run_all_tests(self):
        """Connects, runs all test cases, and disconnects."""
        qos_values = [0, 1, 2]
        delay_values = [0, 100]
        message_size_values = [0, 1000, 4000]
        instance_count_values = [1, 5, 10]

        self.client.connect(self.broker_address, self.broker_port)
        
        for delay in delay_values:
            self.delay = delay
            for msg_size in message_size_values:
                self.message_size = msg_size
                for instance_count in instance_count_values:
                    self.instance_count = instance_count
                    for analyser_qos in qos_values:
                        self.qos = analyser_qos
                        self.client.connect(self.broker_address, self.broker_port)
                        for pub_qos in qos_values:
                            self.pub_qos = pub_qos

                            self.received_messages.clear()
                            for key in self.sys_data:
                                self.sys_data[key].clear()

                            self.publish_request(pub_qos, delay, msg_size, instance_count)
                            print(f"Publishing request (delay = {delay}, messagesize = {msg_size}, instancecount = {instance_count}, pub_qos = {pub_qos}, analyser_qos = {analyser_qos})")

                            time.sleep(Analyser.WAIT_DURATION + 5)
                            self.calculate_statistics()
                        self.client.disconnect()

        
        self.save_results_to_json()
        print("\n===== All Test Suites Completed =====")


    def save_results_to_json(self):
        """Saves all test results to a JSON file."""
        json_filename = "all_tests_results.json"
        with open(json_filename, "w") as f:
            json.dump(Analyser.all_tests_results, f, indent=4)


if __name__ == "__main__":
    analyser = Analyser()
    analyser.run_all_tests()
