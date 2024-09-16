import asyncio
from confluent_kafka import Consumer, KafkaError
import os
import json
from aiohttp import web

# Get environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'test-topic')

# Kafka consumer configuration
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

email_set = set()
domain_set = set()
message_set_lock = asyncio.Lock()

# Kafka consumer task: runs in the background to consume messages and store them in the queue
async def consume_messages():
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    print(f"Consuming messages from topic: {KAFKA_TOPIC}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                await asyncio.sleep(1)  # Wait a bit if no messages are available
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # End of partition, keep consuming
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Add the consumed message to the queue
            message = msg.value().decode('utf-8')
            print(f"Received message: {message}")
            try:
                message_dict = json.loads(message)
                async with message_set_lock:
                    email = message_dict['email']
                    if '@' in email:
                        email_set.add(email)
                        domain = email.split('@')[1]
                        if domain:
                            domain_set.add(domain)
            except ValueError:
                print('Decoding JSON has failed')

    finally:
        consumer.close()

# HTTP handler
async def get_count(request):
    async with message_set_lock:
        return web.json_response(
            {
                'email_count': len(email_set),
                'domain_count': len(domain_set)
            })


# Main function to start both the Kafka consumer and HTTP server
async def main():
    # Run the consumer in the background
    consumer_task = asyncio.create_task(consume_messages())

    # Set up the aiohttp web application
    app = web.Application()
    app.router.add_get('/count', get_count)

    # Start the web server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()

    # Keep the consumer task running
    await consumer_task

if __name__ == "__main__":
    asyncio.run(main())
