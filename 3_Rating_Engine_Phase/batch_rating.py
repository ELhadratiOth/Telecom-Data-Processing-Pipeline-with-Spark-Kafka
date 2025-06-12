import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, round, current_timestamp, datediff, to_json, struct, date_format
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType

def load_config(yaml_file="config.yaml"):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

input_schema = StructType([
    StructField("record_id", StringType(), False),
    StructField("record_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("msisdn", StringType(), True),
    StructField("secondary_msisdn", StringType(), True),
    StructField("duration_sec", FloatType(), True),
    StructField("data_volume_mb", FloatType(), True),
    StructField("cell_id", StringType(), True),
    StructField("technology", StringType(), True),
    StructField("status", StringType(), False)
])

customer_schema = StructType([
    StructField("msisdn", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("subscription_type", StringType(), True),
    StructField("rate_plan_id", IntegerType(), True),
    StructField("activation_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("region", StringType(), True),
    StructField("student", StringType(), True)
])

rate_plan_schema = StructType([
    StructField("rate_plan_id", IntegerType(), False),
    StructField("plan_name", StringType(), True),
    StructField("service_type", StringType(), True),
    StructField("unit_price", FloatType(), True),
    StructField("free_units", IntegerType(), True),
    StructField("tiered_threshold_mb", FloatType(), True),
    StructField("tiered_price_mb", FloatType(), True)
])

def main():
    config = load_config()

    spark = SparkSession.builder \
        .appName(config['rating']['spark']['app_name']) \
        .master(config['rating']['spark']['master']) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.kafka:kafka-clients:3.8.0,org.postgresql:postgresql:42.6.0") \
        .config("spark.driver.extraJavaOptions", "-Dorg.apache.kafka.common.utils.LogContext=ERROR") \
        .config("spark.driver.host", "10.0.2.15") \
        .getOrCreate()

    normalized_df = spark.read \
        .format("jdbc") \
        .option("url", config['rating']['input_postgres']['url']) \
        .option("dbtable", config['rating']['input_postgres']['table']) \
        .option("user", config['rating']['input_postgres']['user']) \
        .option("password", config['rating']['input_postgres']['password']) \
        .option("driver", config['rating']['input_postgres']['driver']) \
        .load()

    normalized_df = normalized_df.dropDuplicates(["record_id"])
    print(f"Input records : {normalized_df.count()}")

    try:
        existing_rated_df = spark.read \
            .format("jdbc") \
            .option("url", config['rating']['output_rated_postgres']['url']) \
            .option("dbtable", config['rating']['output_rated_postgres']['table']) \
            .option("user", config['rating']['output_rated_postgres']['user']) \
            .option("password", config['rating']['output_rated_postgres']['password']) \
            .option("driver", config['rating']['output_rated_postgres']['driver']) \
            .load()
        
        existing_record_ids = existing_rated_df.select("record_id")
        
        normalized_df = normalized_df.join(
            existing_record_ids,
            "record_id",
            "left_anti"
        )
        print(f"New records to process : {normalized_df.count()}")

    except Exception as e:
        print(f"Warning: Could not check existing records (table might be empty): {e}")

    if normalized_df.count() == 0:
        print("No new records to process.")
        spark.stop()
        return

    if "timestamp" in normalized_df.columns:
        normalized_df = normalized_df.withColumn("timestamp", 
            when(col("timestamp").isNull(), current_timestamp())
            .otherwise(col("timestamp").cast("timestamp"))
        )

    normalized_df = normalized_df.withColumn(
        "billing_month", 
        date_format(col("timestamp"), "yyyy-MM-01")
    )

    customers_df = spark.read \
        .format("jdbc") \
        .option("url", config['rating']['input_postgres']['url']) \
        .option("dbtable", "customers") \
        .option("user", config['rating']['input_postgres']['user']) \
        .option("password", config['rating']['input_postgres']['password']) \
        .option("driver", config['rating']['input_postgres']['driver']) \
        .load()

    rate_plans_df = spark.read \
        .format("jdbc") \
        .option("url", config['rating']['input_postgres']['url']) \
        .option("dbtable", "rate_plans") \
        .option("user", config['rating']['input_postgres']['user']) \
        .option("password", config['rating']['input_postgres']['password']) \
        .option("driver", config['rating']['input_postgres']['driver']) \
        .load()

    rated_df = normalized_df.join(
        customers_df,
        normalized_df.msisdn == customers_df.msisdn,
        "left_outer"
    ).select(
        normalized_df["*"],
        customers_df.rate_plan_id.alias("customer_rate_plan_id"),
        customers_df.status.alias("customer_status"),
        customers_df.region,
        customers_df.student,
        customers_df.activation_date
    )

    rated_df = rated_df.withColumn(
        "rating_status",
        when(col("customer_status").isNull(), "unmatched")
        .when(col("customer_status") != "active", "rejected") 
        .when(col("customer_rate_plan_id").isNull(), "unmatched")
        .otherwise("rated")
    )

    rated_df = rated_df.withColumn(
        "required_rate_plan_type",
        when(
            (col("record_type") == "voice") & (col("cell_id").isin("PARIS", "TOULOUSE", "NEW YORK")),
            "premium"  
        ).when(
            (col("record_type") == "sms") & (col("cell_id").isin("PARIS", "TOULOUSE", "NEW YORK")),
            "premium"  
        ).otherwise("any")  
    )
    
    rated_df = rated_df.join(
        rate_plans_df,
        (rated_df.customer_rate_plan_id == rate_plans_df.rate_plan_id) & 
        (rated_df.record_type == rate_plans_df.service_type),
        "left_outer"
    )

    rated_df = rated_df.withColumn(
        "rating_status",
        when(
            (col("rating_status") == "rated") & 
            (col("required_rate_plan_type") == "premium") & 
            (~col("plan_name").contains("Premium")), 
            "rejected" 
        ).when(
            (col("rating_status") == "rated") & col("unit_price").isNull(), 
            "unmatched"
        ).otherwise(col("rating_status"))
    )

    rated_df = rated_df.withColumn(
        "cost",
        when(
            (col("rating_status") == "rated") & (col("record_type") == "voice"),
            when(col("duration_sec").isNull() | (col("duration_sec") <= 0), 0.0)
            .otherwise(col("duration_sec") * col("unit_price"))
        )
        .when(
            (col("rating_status") == "rated") & (col("record_type") == "data"),
            0.0
        )
        .when(
            (col("rating_status") == "rated") & (col("record_type") == "sms"),
            0.0
        )
        .otherwise(0.0)
    )

    rated_df = rated_df.withColumn(
        "cost",
        when(
            (col("record_type") == "voice") & (col("cost") > 0) &
            (col("timestamp").substr(12, 2).cast("int") >= 18) & 
            (col("timestamp").substr(12, 2).cast("int") <= 23),
            col("cost") * 1.02  # evening surcharge: +2% for voice
        ).when(
            (col("record_type") == "voice") & (col("cost") > 0) &
            (col("timestamp").substr(12, 2).cast("int") >= 8) & 
            (col("timestamp").substr(12, 2).cast("int") <= 12),
            col("cost") * 0.98  # morning discount: -2% for voice
        ).otherwise(col("cost"))
    )

    rated_df = rated_df.withColumn(
        "cost",
        when(
            (col("record_type") == "voice") & (col("cost") > 0) &
            (date_format(col("timestamp"), "EEEE") == "Friday"),
            col("cost") * 0.97  # friday discount: 3% for voice
        ).otherwise(col("cost"))
    )

    rated_df = rated_df.withColumn(
        "cost",
        when(
            (col("cost") > 0) &
            col("cell_id").isin("PARIS", "TOULOUSE", "NEW YORK"),
            col("cost") * 1.5  # roaming surcharge: 50% for all services
        ).otherwise(col("cost"))
    )


    final_df = rated_df.select(
        col("record_id"),
        col("record_type"),
        col("timestamp"),
        col("msisdn"),
        col("secondary_msisdn"),
        col("duration_sec"),
        col("data_volume_mb"),
        col("cell_id"),
        col("technology"),
        col("cost"), 
        col("customer_rate_plan_id").alias("rate_plan_id"),
        col("rating_status")
    )

    final_df = final_df.dropDuplicates(["record_id"])

    try:
        print(f"Writing {final_df.count()} rated records to database...")
        final_df.write \
            .format("jdbc") \
            .option("url", config['rating']['output_rated_postgres']['url']) \
            .option("dbtable", config['rating']['output_rated_postgres']['table']) \
            .option("user", config['rating']['output_rated_postgres']['user']) \
            .option("password", config['rating']['output_rated_postgres']['password']) \
            .option("driver", config['rating']['output_rated_postgres']['driver']) \
            .mode("append") \
            .save()
        
        print("Rating process completed successfully!")
        
    except Exception as e:
        print(f"Error writing to database: {e}")
    
    spark.stop()

if __name__ == "__main__":
    main()