from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
from models import SensorData
import json
import uuid
import time
import random

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group'
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = f'roman_s_building_sensors'

consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

try:
    for message in consumer:
        # print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")
        inputMsg = SensorData(**message.value)

        is_alerted=False

        if inputMsg.temperature > 40:
            print(f"Alerting temperature above 40: {inputMsg.temperature} (msgCode={inputMsg.msgCode})")
            data = {
                "timestamp": inputMsg.timestamp,
                "msgCode": inputMsg.msgCode,
                "sensorId": inputMsg.sensorId,
                "temperature": inputMsg.temperature
            }
            producer.send(topic="roman_s_temperature_alerts", key=message.key, value=data)
            producer.flush()
            is_alerted=True

        if inputMsg.humidity > 80 or inputMsg.humidity < 20:
            print(f"Alerting humidity above 80 or under 20: {inputMsg.humidity} (msgCode={inputMsg.msgCode})")
            data = {
                "timestamp": inputMsg.timestamp,
                "msgCode": inputMsg.msgCode,
                "sensorId": inputMsg.sensorId,
                "humidity": inputMsg.humidity
            }
            producer.send(topic="roman_s_humidity_alerts", key=message.key, value=data)
            producer.flush()
            is_alerted=True
        if not is_alerted:
            print(f"No alert generated: {message.value} (key={inputMsg.msgCode})")

        time.sleep(1)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
    producer.close()