from confluent_kafka import Producer
import os
import time
import json
import random
import string

# Get environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'test-topic')

conf = {'bootstrap.servers': KAFKA_BROKER}

producer = Producer(**conf)

def randomword(length):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))

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
            'username': f'{randomword(5)}',
            'email': f'{randomword(5)}@{randomword(5)}'
        }
        message = json.dumps(obj=message_dict)
        print(f'Producing message: {message}')
        producer.produce(KAFKA_TOPIC, value=message, callback=delivery_report)
        producer.poll(1)
        time.sleep(5)

if __name__ == "__main__":
    produce_messages()
