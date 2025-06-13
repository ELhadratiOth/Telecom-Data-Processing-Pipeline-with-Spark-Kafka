import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, from_json, to_json, struct, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

def load_config(yaml_file="config.yaml"):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

input_schema = StructType([
    StructField("record_id", StringType(), nullable=False),
    StructField("record_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("caller_id", StringType(), True),
    StructField("callee_id", StringType(), True),
    StructField("sender_id", StringType(), True),
    StructField("receiver_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("duration_sec", IntegerType(), True),
    StructField("data_volume_mb", FloatType(), True),
    StructField("session_duration_sec", IntegerType(), True),
    StructField("cell_id", StringType(), True),
    StructField("technology", StringType(), True)
])

output_schema = StructType([
    StructField("record_id", StringType(), False),
    StructField("record_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),  # Changed from StringType to TimestampType
    StructField("msisdn", StringType(), True),
    StructField("secondary_msisdn", StringType(), True),
    StructField("duration_sec", FloatType(), True),
    StructField("data_volume_mb", FloatType(), True),
    StructField("cell_id", StringType(), True),
    StructField("technology", StringType(), True),
    StructField("status", StringType(), False)
])

def main():
    config = load_config()

    spark = SparkSession.builder \
        .appName(config['mediation']['spark']['app_name']) \
        .master(config['mediation']['spark']['master']) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.kafka:kafka-clients:3.8.0") \
        .config("spark.driver.extraJavaOptions", "-Dorg.apache.kafka.common.utils.LogContext=ERROR") \
        .config("spark.driver.host", "10.0.2.15") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config['mediation']['input_kafka']['bootstrap_servers']) \
        .option("subscribe", config['mediation']['input_kafka']['topic']) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    json_df = kafka_df.select(
        from_json(col("value").cast("string"), input_schema).alias("data")
    ).select("data.*")

    json_df = json_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

    normalized_df = json_df.select(
        col("record_id"),
        col("record_type"),
        col("timestamp"),  
        when(col("caller_id").isNotNull(), col("caller_id"))
        .when(col("sender_id").isNotNull(), col("sender_id"))
        .when(col("user_id").isNotNull(), col("user_id"))
        .otherwise(lit(None)).alias("msisdn"),
        when(col("callee_id").isNotNull(), col("callee_id"))
        .when(col("receiver_id").isNotNull(), col("receiver_id"))
        .otherwise(lit(None)).alias("secondary_msisdn"),
        when(col("record_type") == "data", col("session_duration_sec"))
        .when(col("record_type") == "voice", col("duration_sec"))
        .otherwise(lit(None)).cast("float").alias("duration_sec"),
        col("data_volume_mb"),
        col("cell_id"),
        col("technology"),
        lit("valid").alias("status")
    )

    validated_df = normalized_df.withColumn(
        "status",
        when(
            col("msisdn").isNull() |
            col("msisdn").startswith("999") |
            (col("duration_sec").isNotNull() & (col("duration_sec") < 0)) |
            (col("data_volume_mb").isNotNull() & (col("data_volume_mb") < 0)) |
            ~col("record_type").isin("voice", "sms", "data"),
            "error"
        ).otherwise(col("status"))
    )

    validated_df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="30 seconds") \
        .start()

    valid_df = validated_df.filter(col("status") == "valid")
    error_df = validated_df.filter(col("status") == "error")

    deduped_valid_df = valid_df.dropDuplicates(["record_id"])

    valid_kafka_df = deduped_valid_df \
        .withColumn("timestamp", col("timestamp").cast("string")) \
        .select(to_json(struct("*")).alias("value"))
        
    valid_kafka_query = valid_kafka_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config['mediation']['output_kafka']['bootstrap_servers']) \
        .option("topic", config['mediation']['output_kafka']['valid_topic']) \
        .option("checkpointLocation", "output/checkpoints/valid_kafka") \
        .outputMode("append") \
        .start()
    print(f"Started Kafka query for valid records: {valid_kafka_query.name}")

    error_kafka_df = error_df \
        .withColumn("timestamp", col("timestamp").cast("string")) \
        .select(to_json(struct("*")).alias("value"))
        
    error_kafka_query = error_kafka_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config['mediation']['output_kafka']['bootstrap_servers']) \
        .option("topic", config['mediation']['output_kafka']['dead_letter_topic']) \
        .option("checkpointLocation", "output/checkpoints/dead_letter_kafka") \
        .outputMode("append") \
        .start()
    print(f"Started Kafka query for errored records: {error_kafka_query.name}")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()