from confluent_kafka import Producer
import os
import time
import json

# Get environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'test-topic')

# Kafka producer configuration
conf = {'bootstrap.servers': KAFKA_BROKER}

# Create Producer instance
producer = Producer(**conf)

# Delivery report callback to check for message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Produce messages to Kafka
def produce_messages():
    while True:
        message_dict = {
            'username': 'random_username',
            'email': 'random@email'
        }
        message = json.dumps(obj=message_dict)
        print(f'Producing message: {message}')
        producer.produce(KAFKA_TOPIC, value=message, callback=delivery_report)
        producer.poll(1)  # Serve delivery reports (blocks until there are reports)
        time.sleep(5)

if __name__ == "__main__":
    produce_messages()
