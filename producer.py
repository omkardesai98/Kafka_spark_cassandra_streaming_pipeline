import random
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer
from avro import schema
import avro.io
import io


# Define the Avro schema as a string
avro_schema_str = """
{
  "type": "record",
  "name": "test",
  "namespace": "spark_stream_test",
  "fields": [
    {"name": "ad_id", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "clicks", "type": "int"},
    {"name": "views", "type": "int"},
    {"name": "cost", "type": "double"}
  ]
}
"""

# Kafka parameters
schema_registry_url = "http://localhost:8081"
kafka_broker = "localhost:9092"
topic = "ads_data"


# Initialize Kafka Producer
producer = Producer({'bootstrap.servers': kafka_broker})

avro_schema = schema.parse(avro_schema_str)


def generate_data():
    ad_id = '123' + str(random.randint(1, 51))
    timestamp = (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat() + 'Z'
    clicks = random.randint(0, 100)
    views = random.randint(0, 500)
    cost = round(random.uniform(5, 100), 2)

    record = {
        "ad_id": ad_id,
        "timestamp": timestamp,
        "clicks": clicks,
        "views": views,
        "cost": cost
    }

    return record


# Produce messages to Kafka
for _ in range(100):  # Produce 100 messages
    message_writer = avro.io.DatumWriter(avro_schema)
    message_bytes_writer = io.BytesIO()
    message_encoder = avro.io.BinaryEncoder(message_bytes_writer)
    data = generate_data()
    message_writer.write(data, message_encoder)
    message_raw_bytes = message_bytes_writer.getvalue()

    # Serialize the data using AvroSerializer
    producer.produce(topic, message_raw_bytes)
    producer.flush()
    time.sleep(1)

