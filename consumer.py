import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.streaming import DataStreamReader

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaAvroConsumer") \
    .master('localhost:7077')\
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'test'
# schema_registry_url = 'http://schema-registry:8081'


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

schema_avro = open('schema.avsc',mode='r').read()


# Stream data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

kafka_df = kafka_df.select('value')
kafka_df1 = kafka_df.select(from_avro(col('value'),schema_avro))
kafka_df1.printSchema()
# Convert the Kafka value from Avro
# from za.co.absa.abris.avro.functions import from_confluent_avro



# Display the deserialized values
query = kafka_df1.writeStream \
    .outputMode("complete") \
    .option('truncate',False)\
    .format("console") \
    .start()

query.awaitTermination()
