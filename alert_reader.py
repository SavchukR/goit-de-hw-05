from datetime import datetime
from time import strptime

from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
from models import SensorData
import json
import time

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

consumer.subscribe(['roman_s_temperature_alerts','roman_s_humidity_alerts'])

try:
    for message in consumer:
        inputMsg = SensorData(**message.value)
        if message.topic == 'roman_s_temperature_alerts':
            print(f"Alert received on temperature: {datetime.fromtimestamp( inputMsg.timestamp)}: {inputMsg.sensorId}: temperature: {inputMsg.temperature} (msgCode={inputMsg.msgCode})")
        elif message.topic == 'roman_s_humidity_alerts':
            print(f"Alert received humidity: {datetime.fromtimestamp( inputMsg.timestamp)}: {inputMsg.sensorId}: humidity: {inputMsg.humidity} (msgCode={inputMsg.msgCode})")
        else:
            raise Exception(f"Unknown topic: {message.topic}" )
        time.sleep(1)
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()