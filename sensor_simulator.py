from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-n", "--sensorId", type=str)
parser.add_argument("-t", "--topic", type=str)
parser.add_argument("-q", "--qtyOfEvents", type=int)

args = parser.parse_args()
sensorId = args.sensorId
topic = args.topic
qtyOfEvents = args.qtyOfEvents

print(f"Sensor Id: {sensorId} started")

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i in range(qtyOfEvents):
    try:
        msgCode =f"{sensorId}-{i:03d}"
        data = {
            "msgCode": msgCode,
            "timestamp": time.time(),
            "sensorId": sensorId,
            "humidity": random.randint(15, 85),
            "temperature": random.randint(25, 45)
        }
        key=str(uuid.uuid4())
        producer.send(topic=topic, key=key, value=data)
        producer.flush()
        print(f"Message {i} from sensor {sensorId} sent to topic '{topic}' successfully (msgCode={msgCode})")
        time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")

print(f"Sensor Id: {sensorId} send {qtyOfEvents} events to topic '{topic}' successfully.")

producer.close()

