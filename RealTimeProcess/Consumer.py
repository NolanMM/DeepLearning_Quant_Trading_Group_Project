from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from dotenv import load_dotenv
import os


class KafkaSparkConsumerStreaming:
    def __init__(self):
        load_dotenv("../.env",override=True)

        # Load environment variables
        self.kafka_v = os.getenv("KAFKA_VERSION")
        self.kafka_server = os.getenv("KAFKA_SERVER")
        self.kafka_topic = os.getenv("KAFKA_TOPIC")

        self.column_1_name = os.getenv("COLUMN_1")
        self.column_2_name = os.getenv("COLUMN_2")
        self.column_3_name = os.getenv("COLUMN_3")

        self.checkpoint_location = os.getenv("CHECKPOINT_LOCATION")

        self.postgres_v = os.getenv("POSTGRES_VERSION")
        self.postgres_url = os.getenv("POSTGRES_URL")
        self.postgres_user = os.getenv("POSTGRES_USER")
        self.postgres_pass = os.getenv("POSTGRES_PASSWORD")
        self.postgres_table = os.getenv("POSTGRES_TABLE")
        self.format_file = os.getenv("FORMAT_FILE")
        self._mode = os.getenv("MODE")

        self.config_ = f"{self.kafka_v},{self.postgres_v}"

        # Initialize Spark session with Kafka dependencies and PostgresSQL driver
        self.spark = SparkSession.builder \
            .appName("KafkaSpark_Streaming") \
            .config("spark.jars.packages", self.config_) \
            .getOrCreate()

        # Define schema for incoming data
        self.schema = StructType([
            StructField(self.column_1_name, StringType(), True),
            StructField(self.column_2_name, StringType(), True),
            StructField(self.column_3_name, StringType(), True)
        ])

        self.query = None

    def _write_streaming(self, df_, epoch_id) -> None:
        df_.write \
            .mode(self._mode) \
            .format(self.format_file) \
            .option("url", f"{self.postgres_url}") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", self.postgres_table) \
            .option("user", self.postgres_user) \
            .option("password", self.postgres_pass) \
            .save()

    def start_streaming(self):
        # Read data from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_server) \
            .option("subscribe", self.kafka_topic) \
            .load()

        # Parse the value field
        value_df = df.selectExpr("CAST(value AS STRING)")

        # Convert JSON string to DataFrame
        json_df = value_df.select(from_json(col("value"), self.schema).alias("data")).select("data.*")

        # Convert event_time to timestamp
        json_df = json_df.withColumn(self.column_3_name, to_timestamp(col(self.column_3_name)))

        self.query = json_df.writeStream \
            .foreachBatch(self._write_streaming) \
            .start()

        self.query.awaitTermination()

    def stop_streaming(self):
        if self.query is not None:
            self.query.stop()
            print("Streaming query stopped successfully.")
        else:
            print("No streaming query to stop.")


# Create an instance of the class and start/stop streaming
if __name__ == "__main__":
    kafka_spark_streaming = KafkaSparkConsumerStreaming()
    try:
        kafka_spark_streaming.start_streaming()
    except KeyboardInterrupt:
        kafka_spark_streaming.stop_streaming()
