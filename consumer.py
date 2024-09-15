from confluent_kafka import Consumer, KafkaError
import os

# Get environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'test-topic')

# Kafka consumer configuration
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

# Create Consumer instance
consumer = Consumer(conf)

# Subscribe to the topic
consumer.subscribe([KAFKA_TOPIC])

# Consume messages and print them
def consume_messages():
    try:
        print(f"Consuming messages from topic: {KAFKA_TOPIC}")
        while True:
            msg = consumer.poll(1.0)  # Wait for message or timeout

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    # Some other error
                    print(f"Error: {msg.error()}")
                    break

            print(f"Received message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets
        consumer.close()

if __name__ == "__main__":
    consume_messages()
