# Use an official Python runtime as a base image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any necessary dependencies
# You need confluent_kafka for Kafka interactions
RUN pip install --no-cache-dir confluent_kafka aiohttp

# Add any other required packages here:
# RUN pip install --no-cache-dir <other-packages>

# Run the Python application when the container starts
CMD ["python", "producer.py"]
