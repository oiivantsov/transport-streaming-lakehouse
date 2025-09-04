import logging
import paho.mqtt.client as mqtt
import json
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import time
import socket

TOPIC = "hsl_stream"
PARTITIONS = 6   # adjust based on parallelism needs
REPLICATION_FACTOR = 1  # in local/dev keep at 1
TIME_SLEEP = 0.1 # Delay (in seconds) between producing consecutive messages to Kafka

def wait_for_kafka(host, port, timeout=60):
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                print("Kafka is ready!")
                break
        except Exception:
            if time.time() - start_time > timeout:
                raise Exception("Timeout waiting for Kafka")
            print("Waiting for Kafka...")
            time.sleep(5)

wait_for_kafka('kafka', 29092)

admin_client = KafkaAdminClient(bootstrap_servers="kafka:29092", client_id="topic_creator")
existing_topics = admin_client.list_topics()

if TOPIC not in existing_topics:
    topic = NewTopic(name=TOPIC, num_partitions=PARTITIONS, replication_factor=REPLICATION_FACTOR)
    admin_client.create_topics(new_topics=[topic])
    print(f"Created topic '{TOPIC}' with {PARTITIONS} partitions")
else:
    print(f"Topic '{TOPIC}' already exists")
admin_client.close()

producer = KafkaProducer(
    bootstrap_servers='kafka:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

# Topic parts for parsing: https://digitransit.fi/en/developers/apis/5-realtime-api/vehicle-positions/high-frequency-positioning/
topic_parts = [
    "prefix", "version", "journey_type", "temporal_type", "event_type", 
    "transport_mode", "operator_id", "vehicle_number", "route_id", 
    "direction_id", "headsign", "start_time", "next_stop", 
    "geohash_level", "geohash", "sid"
]


def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    client.subscribe("/hfp/v2/journey/#")



def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload)
    except json.JSONDecodeError:
        logging.warning("Invalid JSON")
        return

    topic_parts_list = msg.topic.split("/")[1:]
    topic_parsed = dict(zip(topic_parts, topic_parts_list))

    vehicle_number = topic_parsed.get('vehicle_number')
    route_id = topic_parsed.get('route_id')
    tst = payload.get('VP', {}).get('tst')
    journey_type = topic_parsed.get('journey_type') 
    temporal_type = topic_parsed.get('temporal_type')
    transport_mode = topic_parsed.get('transport_mode')

    if (journey_type != 'journey' or temporal_type != 'ongoing' or transport_mode != 'bus'):
        logging.info(f"Skipping non-bus or non-ongoing message: journey_type={journey_type}, temporal_type={temporal_type}, transport_mode={transport_mode}")
        return

    if not vehicle_number or not route_id or not tst:
        logging.warning(f"Skipping message due to missing fields: vehicle_number={vehicle_number}, route_id={route_id}, tst={tst}")
        return

    event_body = {
        "topic": topic_parsed,
        "payload": payload
    }

    kafka_key = vehicle_number or route_id or 'unknown'

    producer.send(TOPIC, key=kafka_key, value=event_body)
    print(f"Produced: key={kafka_key} -> {event_body}")

    time.sleep(TIME_SLEEP)


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.connect("mqtt.hsl.fi", 1883, 60)
mqttc.loop_forever()
