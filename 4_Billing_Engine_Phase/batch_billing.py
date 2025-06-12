import yaml
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, count, round, lit, to_timestamp, current_timestamp, date_format, datediff, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType

def load_config(yaml_file="config.yaml"):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

rated_records_schema = StructType([
    StructField("record_id", StringType(), True),  
    StructField("record_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("msisdn", StringType(), True),
    StructField("secondary_msisdn", StringType(), True),
    StructField("duration_sec", FloatType(), True),  
    StructField("data_volume_mb", FloatType(), True), 
    StructField("cell_id", StringType(), True),
    StructField("technology", StringType(), True),
    StructField("cost", FloatType(), True),  
    StructField("rate_plan_id", IntegerType(), True),
    StructField("rating_status", StringType(), True) 
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

def main():
    config = load_config()

    spark = SparkSession.builder \
        .appName(config['billing']['spark']['app_name']) \
        .master(config['billing']['spark']['master']) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.driver.host", "10.0.2.15") \
        .getOrCreate()

    current_date = datetime.now()
    billing_start = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    billing_end = current_date + timedelta(days=1) 
    billing_start_str = billing_start.strftime("%Y-%m-%d %H:%M:%S")
    billing_end_str = billing_end.strftime("%Y-%m-%d %H:%M:%S")
    print(f"Current month billing cycle: {billing_start_str} to {billing_end_str}")

    rated_df = spark.read \
        .format("jdbc") \
        .option("url", config['billing']['input_postgres']['url']) \
        .option("dbtable", config['billing']['input_postgres']['table']) \
        .option("user", config['billing']['input_postgres']['user']) \
        .option("password", config['billing']['input_postgres']['password']) \
        .option("driver", config['billing']['input_postgres']['driver']) \
        .load()


    customers_df = spark.read \
        .format("jdbc") \
        .option("url", config['billing']['customers_postgres']['url']) \
        .option("dbtable", config['billing']['customers_postgres']['table']) \
        .option("user", config['billing']['customers_postgres']['user']) \
        .option("password", config['billing']['customers_postgres']['password']) \
        .option("driver", config['billing']['customers_postgres']['driver']) \
        .load()

    rate_plans_df = spark.read \
        .format("jdbc") \
        .option("url", config['billing']['input_postgres']['url']) \
        .option("dbtable", "rate_plans") \
        .option("user", config['billing']['input_postgres']['user']) \
        .option("password", config['billing']['input_postgres']['password']) \
        .option("driver", config['billing']['input_postgres']['driver']) \
        .load()


    filtered_df = rated_df.filter(
        (col("rating_status") == "rated") &
        (col("timestamp") >= to_timestamp(lit(billing_start_str))) &
        (col("timestamp") <= to_timestamp(lit(billing_end_str)))
    )
    
    aggregated_df = filtered_df.groupBy("msisdn").agg(
        sum(when(col("record_type") == "voice", col("cost")).otherwise(0.0)).alias("voice_cost_from_rating"),
        sum(when(col("record_type") == "sms", col("cost")).otherwise(0.0)).alias("sms_cost_from_rating"),
        sum(when(col("record_type") == "data", col("cost")).otherwise(0.0)).alias("data_cost_from_rating"),
        sum(when(col("record_type") == "voice", 
            when(
                (col("duration_sec") % 60) > 0,
                ((col("duration_sec") / 60).cast("int") + 1)  
            ).otherwise(
                col("duration_sec") / 60  
            )
        ).otherwise(0.0)).alias("voice_usage_minutes"),
        count(when(col("record_type") == "sms", 1)).alias("sms_usage_count"),
        sum(when(col("record_type") == "data",
            when(
                (col("data_volume_mb") % 1) > 0,
                ((col("data_volume_mb")).cast("int") + 1)  
            ).otherwise(
                col("data_volume_mb").cast("int") 
            )
        ).otherwise(0.0)).alias("data_usage_mb")
    )



    billing_df = aggregated_df.join(
        customers_df.select("msisdn", "customer_name", "rate_plan_id", "student", "region", "activation_date"),
        "msisdn",
        "left_outer"
    )

    billing_df = billing_df.withColumn(
        "loyalty_months",
        datediff(current_timestamp(), col("activation_date")) / 30
    )

    voice_rate_plans = rate_plans_df.filter(col("service_type") == "voice").select(
        col("rate_plan_id").alias("voice_rate_plan_id"),
        col("unit_price").alias("voice_unit_price"),
        col("free_units").alias("voice_free_minutes")
    )
    
    billing_df = billing_df.join(
        voice_rate_plans,
        billing_df.rate_plan_id == voice_rate_plans.voice_rate_plan_id,
        "left"
    )
    
    billing_df = billing_df.withColumn(
        "voice_billable_minutes",
        when(
            col("voice_usage_minutes") > col("voice_free_minutes"),
            col("voice_usage_minutes") - col("voice_free_minutes")
        ).otherwise(0.0)
    )
    
    billing_df = billing_df.withColumn(
        "voice_cost",
        when(
            (col("voice_billable_minutes") > 0) & (col("voice_usage_minutes") > 0),
            col("voice_cost_from_rating") * (col("voice_billable_minutes") / col("voice_usage_minutes"))
        ).otherwise(0.0)
    )

    sms_rate_plans = rate_plans_df.filter(col("service_type") == "sms").select(
        col("rate_plan_id").alias("sms_rate_plan_id"),
        col("unit_price").alias("sms_unit_price"),
        col("free_units").alias("sms_free_units")
    )
    
    billing_df = billing_df.join(
        sms_rate_plans,
        billing_df.rate_plan_id == sms_rate_plans.sms_rate_plan_id,
        "left"
    )
    
    billing_df = billing_df.withColumn(
        "sms_cost",
        when(
            col("sms_usage_count") > col("sms_free_units"),
            (col("sms_usage_count") - col("sms_free_units")) * col("sms_unit_price")
        ).otherwise(0.0)
    )

    data_rate_plans = rate_plans_df.filter(col("service_type") == "data").select(
        col("rate_plan_id").alias("data_rate_plan_id"),
        col("unit_price").alias("data_unit_price"),
        col("tiered_threshold_mb").alias("data_threshold_mb"),
        col("tiered_price_mb").alias("data_tiered_price")
    )
    
    billing_df = billing_df.join(
        data_rate_plans,
        billing_df.rate_plan_id == data_rate_plans.data_rate_plan_id,
        "left"
    )
    
    billing_df = billing_df.withColumn(
        "data_cost",
        when(
            col("data_usage_mb") <= col("data_threshold_mb"),
            col("data_cost_from_rating")
        ).otherwise(
            (col("data_threshold_mb") * col("data_unit_price")) +
            ((col("data_usage_mb") - col("data_threshold_mb")) * col("data_tiered_price"))
        )
    )

    billing_df = billing_df.withColumn("voice_cost", when(col("voice_cost").isNull(), 0.0).otherwise(col("voice_cost"))) \
        .withColumn("sms_cost", when(col("sms_cost").isNull(), 0.0).otherwise(col("sms_cost"))) \
        .withColumn("data_cost", when(col("data_cost").isNull(), 0.0).otherwise(col("data_cost")))

    billing_df = billing_df.withColumn(
        "subtotal_before_discounts",
        col("voice_cost") + col("sms_cost") + col("data_cost")
    )

    voice_threshold = config['billing']['usage_thresholds']['voice']['threshold_minutes']
    voice_discount = config['billing']['usage_thresholds']['voice']['discount_rate']
    data_threshold = config['billing']['usage_thresholds']['data']['threshold_mb']
    data_discount = config['billing']['usage_thresholds']['data']['discount_rate']
    total_threshold = config['billing']['usage_thresholds']['total_spending']['threshold_amount']
    total_discount = config['billing']['usage_thresholds']['total_spending']['discount_rate']

    billing_df = billing_df.withColumn(
        "voice_cost",
        when(
            col("voice_usage_minutes") >= voice_threshold,
            col("voice_cost") * (1.0 - voice_discount)
        ).otherwise(col("voice_cost"))
    )

    billing_df = billing_df.withColumn(
        "data_cost",
        when(
            col("data_usage_mb") >= data_threshold,
            col("data_cost") * (1.0 - data_discount)
        ).otherwise(col("data_cost"))
    )

    billing_df = billing_df.withColumn(
        "total_spending_discount",
        when(
            col("subtotal_before_discounts") >= total_threshold,
            col("subtotal_before_discounts") * total_discount
        ).otherwise(0.0)
    )

    student_discount_rate = config['billing']['discounts']['student_discount_rate']
    billing_df = billing_df.withColumn(
        "student_discount",
        when(
            (col("student") == "true") | (col("student") == True),
            (col("voice_cost") + col("sms_cost") + col("data_cost")) * student_discount_rate
        ).otherwise(0.0)
    )

    billing_df = billing_df.withColumn(
        "urban_discount",
        when(
            col("region") == "urban",
            (col("voice_cost") + col("sms_cost") + col("data_cost")) * 0.05
        ).otherwise(0.0)
    )

    loyalty_discount_per_year = config['billing']['discounts']['loyalty_discount_per_year']
    max_loyalty_discount = config['billing']['discounts']['max_loyalty_discount']
    
    billing_df = billing_df.withColumn(
        "loyalty_discount_rate",
        when(
            col("loyalty_months") >= 12,
            when(
                (col("loyalty_months") / 12 * loyalty_discount_per_year) > max_loyalty_discount,
                max_loyalty_discount
            ).otherwise(col("loyalty_months") / 12 * loyalty_discount_per_year)
        ).otherwise(0.0)
    )
    
    billing_df = billing_df.withColumn(
        "loyalty_discount",
        (col("voice_cost") + col("sms_cost") + col("data_cost")) * col("loyalty_discount_rate")
    )

    ramadan_month = config['billing']['seasonal_discounts']['ramadan_month']
    ramadan_discount = config['billing']['seasonal_discounts']['ramadan_discount']
    
    billing_df = billing_df.withColumn(
        "ramadan_discount",
        when(
            date_format(current_timestamp(), "M").cast("int") == ramadan_month,
            (col("voice_cost") + col("sms_cost") + col("data_cost")) * ramadan_discount
        ).otherwise(0.0)
    )

    billing_df = billing_df.withColumn(
        "total_cost_after_discounts",
        col("voice_cost") + col("sms_cost") + col("data_cost") - 
        col("total_spending_discount") - col("student_discount") - col("urban_discount") -
        col("loyalty_discount") - col("ramadan_discount")
    )

    regulatory_fee = config['billing']['taxes']['regulatory_fee']
    billing_df = billing_df.withColumn(
        "regulatory_fee",
        when(
            (col("student") == "true") | (col("student") == True),
            0.0
        ).otherwise(lit(regulatory_fee))
    )

    billing_df = billing_df.withColumn(
        "subtotal_with_fees",
        col("total_cost_after_discounts") + col("regulatory_fee")
    )

    vat_rate = config['billing']['taxes']['vat_rate']
    billing_df = billing_df.withColumn(
        "vat_amount",
        col("subtotal_with_fees") * vat_rate
    )

    billing_df = billing_df.withColumn(
        "total_cost",
        col("subtotal_with_fees") + col("vat_amount")
    )

    billing_df = billing_df.withColumn("voice_cost", round(col("voice_cost"), 4)) \
        .withColumn("sms_cost", round(col("sms_cost"), 4)) \
        .withColumn("data_cost", round(col("data_cost"), 4)) \
        .withColumn("total_cost", round(col("total_cost"), 4)) \
        .withColumn("voice_usage_minutes", round(col("voice_usage_minutes"), 4)) \
        .withColumn("data_usage_mb", round(col("data_usage_mb"), 4)) \
        .withColumn("regulatory_fee", round(col("regulatory_fee"), 4)) \
        .withColumn("vat_amount", round(col("vat_amount"), 4))

    print(f"Customers to bill: {billing_df.count()}")

    invoice_df = billing_df.withColumn("invoice_id", expr("uuid()")) \
        .withColumn("billing_period_start", to_timestamp(lit(billing_start_str))) \
        .withColumn("billing_period_end", to_timestamp(lit(billing_end_str))) \
        .withColumn("invoice_date", current_timestamp()) \
        .withColumn("status", lit("pending"))

    final_df = invoice_df.select(
        col("invoice_id"),
        col("msisdn"),
        col("customer_name"),
        col("billing_period_start"),
        col("billing_period_end"),
        when(col("voice_cost").isNull(), 0.0).otherwise(col("voice_cost")).alias("voice_cost"),
        when(col("sms_cost").isNull(), 0.0).otherwise(col("sms_cost")).alias("sms_cost"),
        when(col("data_cost").isNull(), 0.0).otherwise(col("data_cost")).alias("data_cost"),
        when(col("voice_usage_minutes").isNull(), 0.0).otherwise(col("voice_usage_minutes")).alias("voice_usage"),
        when(col("sms_usage_count").isNull(), 0).otherwise(col("sms_usage_count")).alias("sms_usage"),
        when(col("data_usage_mb").isNull(), 0.0).otherwise(col("data_usage_mb")).alias("data_usage"),
        when(col("total_cost").isNull(), 0.0).otherwise(col("total_cost")).alias("total_cost"),
        col("invoice_date"),
        col("status"),
        col("region"),
        col("student")
    )


    final_df.write \
        .format("jdbc") \
        .option("url", config['billing']['output_postgres']['url']) \
        .option("dbtable", config['billing']['output_postgres']['table']) \
        .option("user", config['billing']['output_postgres']['user']) \
        .option("password", config['billing']['output_postgres']['password']) \
        .option("driver", config['billing']['output_postgres']['driver']) \
        .mode("append") \
        .save()

    print(f"Generated {final_df.count()} invoices for billing period {billing_start_str} to {billing_end_str}")
    print("Billing process completed successfully with full calculations")

    spark.stop()

if __name__ == "__main__":
    main()