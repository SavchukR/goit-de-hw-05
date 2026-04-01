from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

admin_client.delete_topics(['roman_s_building_sensors'])
admin_client.delete_topics(['roman_s_temperature_alerts'])
admin_client.delete_topics(['roman_s_humidity_alerts'])

print("Topics: ")
topics_list = ["roman_s_building_sensors", "roman_s_temperature_alerts", "roman_s_humidity_alerts"]
[print(topic) for topic in admin_client.list_topics() if any(x in topic for x in topics_list)]

admin_client.close()

