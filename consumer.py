import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, window, current_timestamp
from pyspark.sql.avro.functions import from_avro
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster, NoHostAvailable




# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaAvroConsumer") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = 'broker:29092'
kafka_topic = 'test'

# Read Avro schema from file
with open('schema.avsc', mode='r') as file:
    schema_avro = file.read()

# Stream data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

kafka_df1 = kafka_df.select(from_avro(col('value'), schema_avro).alias("data")) \
    .select(col("data.ad_id"), col("data.timestamp"), col("data.clicks"), col("data.views"), col("data.cost")) \
    .withColumn('current_timestamp', current_timestamp())

result_df = kafka_df1.groupBy(window(col("current_timestamp"), "1 minute", "30 seconds"), col("ad_id")) \
    .agg(
        sum("clicks").alias("total_clicks"),
        sum("views").alias("total_views"),
        avg("cost").alias("avg_cost_per_view")
    ) \
    .select("window.start", "window.end", "ad_id", "total_clicks", "total_views", "avg_cost_per_view")

def process_batch(batch_df, batch_id):
    max_retries = 3
    retry_count = 0
    connected = False
    while not connected and retry_count < max_retries:
        try:
            # Initialize Cassandra connection
            cluster = Cluster(['cassandra'], port=9042)
            session = cluster.connect('spark')
            connected = True
        except NoHostAvailable:
            retry_count += 1
            print(f"Retry {retry_count}/{max_retries} - Unable to connect to Cassandra. Retrying...")
            if retry_count == max_retries:
                print("Failed to connect to Cassandra after several retries.")
                return

    try:
        # Process each row in the batch
        for row in batch_df.collect():
            ad_id = row['ad_id']
            total_clicks = row['total_clicks']
            total_views = row['total_views']
            avg_cost_per_view = row['avg_cost_per_view']

            # Query Cassandra
            query = SimpleStatement(f"SELECT total_clicks, total_views, avg_cost_per_view FROM adsData WHERE ad_id = '{ad_id}'")
            result = session.execute(query).one()

            if result:
                new_total_clicks = result.total_clicks + total_clicks
                new_total_views = result.total_views + total_views
                new_avg_cost_per_view = (result.avg_cost_per_view * result.total_views + avg_cost_per_view * total_views) / new_total_views

                update_query = SimpleStatement(f"UPDATE adsData SET total_clicks = {new_total_clicks}, total_views = {new_total_views}, avg_cost_per_view = {new_avg_cost_per_view} WHERE ad_id = '{ad_id}'")
                session.execute(update_query)
            else:
                insert_query = SimpleStatement(f"INSERT INTO adsData (ad_id, total_clicks, total_views, avg_cost_per_view) VALUES ('{ad_id}', {total_clicks}, {total_views}, {avg_cost_per_view})")
                session.execute(insert_query)

        # Clean up
        session.shutdown()
        cluster.shutdown()
    except Exception as e:
        print(f"Error processing batch: {e}")

query = result_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .start()

query.awaitTermination()

