import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def load_config(yaml_file="config.yaml"):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

input_schema = StructType([
    StructField("record_id", StringType(), False),
    StructField("record_type", StringType(), True),
    StructField("timestamp", StringType(), True),
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
        .appName("NormalizedRecordsStore") \
        .master(config['rating']['spark']['master']) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.kafka:kafka-clients:3.8.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.driver.extraJavaOptions", "-Dorg.apache.kafka.common.utils.LogContext=ERROR") \
        .config("spark.driver.host", "10.0.2.15") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config['rating']['input_kafka']['bootstrap_servers']) \
        .option("subscribe", config['rating']['input_kafka']['topic']) \
        .option("startingOffsets", "earliest") \
        .option("kafka.metadata.max.age.ms", "600000") \
        .load()

    json_df = kafka_df.select(
        from_json(col("value").cast("string"), input_schema).alias("data")
    ).select("data.*")

    json_df = json_df.withColumn("timestamp", 
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

    def write_to_postgres(batch_df, batch_id):
        if batch_df.count() == 0:
            return
            
        print(f"Writing batch {batch_id} with {batch_df.count()} records to normalized_records table")
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", config['rating']['input_postgres']['url']) \
                .option("dbtable", "normalized_records") \
                .option("user", config['rating']['input_postgres']['user']) \
                .option("password", config['rating']['input_postgres']['password']) \
                .option("driver", config['rating']['input_postgres']['driver']) \
                .option("batchsize", "1000") \
                .option("isolationLevel", "NONE") \
                .mode("append") \
                .save()
            
            print(f"Successfully wrote batch {batch_id}")
            
        except Exception as e:
            print(f"Error writing batch {batch_id}: {e}")
            

    query = json_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "output/checkpoints/normalized_store") \
        .trigger(processingTime="10 seconds") \
        .start()
    query.awaitTermination()

if __name__ == "__main__":
    main()