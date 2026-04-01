from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

def new_topic(client: KafkaAdminClient, topic_name: str):

    new_topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)

    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

new_topic(admin_client, topic_name='roman_s_building_sensors')
new_topic(admin_client, topic_name='roman_s_temperature_alerts')
new_topic(admin_client, topic_name='roman_s_humidity_alerts')

print("Topics: ")
topics_list = ["roman_s_building_sensors", "roman_s_temperature_alerts", "roman_s_humidity_alerts"]
[print(topic) for topic in admin_client.list_topics() if any(x in topic for x in topics_list)]

admin_client.close()
