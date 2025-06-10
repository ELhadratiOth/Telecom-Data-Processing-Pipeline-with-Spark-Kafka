import yaml
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, round, when, lit, sum, count, avg
import shutil
import glob
from dotenv import load_dotenv
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

def load_config(yaml_file="config.yaml"):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def check_s3_credentials():
    return (os.getenv('AWS_ACCESS_KEY_ID') and 
            os.getenv('AWS_SECRET_ACCESS_KEY') and 
            os.getenv('AWS_DEFAULT_REGION'))

def create_spark_session_with_s3(config):
    spark_builder = SparkSession.builder \
        .appName(config['reporting']['spark']['app_name']) \
        .master(config['reporting']['spark']['master']) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.driver.host", "10.0.2.15")
    
    if check_s3_credentials():
        spark_builder = spark_builder \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.getenv('AWS_DEFAULT_REGION')}.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.path.style.access", "false") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
    else:
        print(" S3 credentials not found - local mode only")
    
    return spark_builder.getOrCreate()

def save_to_local_and_s3(df, report_name, config, spark_session):
    
    local_temp_path = f"reports/temp_{report_name}"
    local_final_path = f"reports/{report_name}"
    
    os.makedirs(local_final_path, exist_ok=True)
    
    df.coalesce(1).write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(local_temp_path)
    
    temp_files = glob.glob(f"{local_temp_path}/part-*.csv")
    if temp_files:
        shutil.move(temp_files[0], f"{local_final_path}/{report_name}.csv")
        shutil.rmtree(local_temp_path)
    
    
    if check_s3_credentials():
        try:
            s3_bucket = config['reporting']['s3']['bucket_name']
            s3_path = f"s3a://{s3_bucket}/{report_name}.csv"
            
            df.coalesce(1).write \
                .format("csv") \
                .option("header", "true") \
                .mode("overwrite") \
                .save(s3_path)
                        
        except Exception as e:
            print("Report saved locally only")
    else:
        print(f"S3 credentials not available - {report_name} saved locally only")
    


def generate_customer_report(spark, config):
    customers_df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", "customers") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    return customers_df.select(
        col("msisdn"),
        col("customer_name"),
        col("subscription_type"),
        col("rate_plan_id"),
        col("activation_date"),
        col("status"),
        col("region"),
        when(col("student") == True, "Yes").otherwise("No").alias("is_student")
    )

def generate_invoice_report(spark, config):
    invoices_df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", "invoices") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    return invoices_df.select(
        col("invoice_id"),
        col("msisdn"),
        col("customer_name"),
        round(col("voice_cost"), 2).alias("voice_cost"),
        round(col("sms_cost"), 2).alias("sms_cost"),
        round(col("data_cost"), 2).alias("data_cost"),
        round(col("total_cost"), 2).alias("total_cost"),
        col("voice_usage"),
        col("sms_usage"),
        col("data_usage"),
        col("status"),
        col("invoice_date")
    )

def generate_revenue_report(spark, config):
    invoices_df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", "invoices") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    revenue_df = invoices_df.groupBy("region").agg(
        sum("total_cost").alias("total_revenue"),
        count("*").alias("customer_count"),
        avg("total_cost").alias("avg_revenue_per_customer")
    )
    
    return revenue_df

def generate_revenue_by_service_report(spark, config):
    invoices_df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", "invoices") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    revenue_by_service_df = invoices_df.agg(
        sum("voice_cost").alias("voice_revenue"),
        sum("sms_cost").alias("sms_revenue"),
        sum("data_cost").alias("data_revenue"),
        count("*").alias("total_invoices"),
        avg("voice_cost").alias("avg_voice_cost_per_customer"),
        avg("sms_cost").alias("avg_sms_cost_per_customer"),
        avg("data_cost").alias("avg_data_cost_per_customer")
    )
    
    aggregated_values = revenue_by_service_df.collect()[0]
    
    service_data = [
        ("voice", float(aggregated_values['voice_revenue'] or 0), 
         int(aggregated_values['total_invoices']), 
         float(aggregated_values['avg_voice_cost_per_customer'] or 0)),
        ("sms", float(aggregated_values['sms_revenue'] or 0), 
         int(aggregated_values['total_invoices']), 
         float(aggregated_values['avg_sms_cost_per_customer'] or 0)),
        ("data", float(aggregated_values['data_revenue'] or 0), 
         int(aggregated_values['total_invoices']), 
         float(aggregated_values['avg_data_cost_per_customer'] or 0))
    ]
    
    
    schema = StructType([
        StructField("service_type", StringType(), False),
        StructField("total_revenue", FloatType(), False),
        StructField("total_invoices", IntegerType(), False),
        StructField("avg_revenue_per_customer", FloatType(), False)
    ])
    
    final_service_df = spark.createDataFrame(service_data, schema)
    
    return final_service_df

def generate_rated_records_report(spark, config):
    rated_records_df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", "rated_records") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    return rated_records_df.filter(col("rating_status") == "rated").select(
        col("record_id"),
        col("record_type"),
        date_format(col("timestamp"), "HH:mm:ss").alias("timestamp"), 
        col("msisdn"),
        round(col("cost"), 4).alias("cost"),
        col("rating_status")
    )

def generate_rate_plans_report(spark, config):
    rate_plans_df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", "rate_plans") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    return rate_plans_df.select(
        col("rate_plan_id"),
        col("plan_name"),
        col("service_type"),
        round(col("unit_price"), 4).alias("unit_price"),
        col("free_units"),
        col("tiered_threshold_mb"),
        col("tiered_price_mb")
    )

def generate_dead_records_report(spark, config):
    rated_records_df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", "rated_records") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    dead_rated_records_df = rated_records_df.filter(
        col("rating_status").isin("rejected", "unmatched", "error")
    ).select(
        col("record_id"),
        col("record_type"), 
        date_format(col("timestamp"), "HH:mm:ss").alias("timestamp"), 
        col("msisdn"),
        col("rating_status").alias("error_type"),
        lit("rating_engine").alias("error_source")
    )
    
    normalized_records_df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", "normalized_records") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    dead_normalized_records_df = normalized_records_df.filter(
        col("status") == "error"
    ).select(
        col("record_id"),
        col("record_type"),
        date_format(col("timestamp"), "HH:mm:ss").alias("timestamp"), 
        col("msisdn"),
        lit("validation_error").alias("error_type"),
        lit("mediation_engine").alias("error_source")
    )
    
    all_dead_records_df = dead_rated_records_df.union(dead_normalized_records_df)
    
    return all_dead_records_df

def main():
    load_dotenv(override=True)
    config = load_config()
    
    spark = create_spark_session_with_s3(config)
    
    
    import os
    import shutil
    import glob
    

    customers_report_df = generate_customer_report(spark, config)
    save_to_local_and_s3(customers_report_df, "customers", config, spark)
    
    invoices_report_df = generate_invoice_report(spark, config)
    save_to_local_and_s3(invoices_report_df, "invoices", config, spark)
    
    revenue_report_df = generate_revenue_report(spark, config)
    
    os.makedirs("reports/revenue", exist_ok=True)
    
    revenue_report_df.coalesce(1).write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save("reports/temp_revenue_by_region")
    
    temp_files = glob.glob("reports/temp_revenue_by_region/part-*.csv")
    if temp_files:
        shutil.move(temp_files[0], "reports/revenue/revenue_by_region.csv")
        shutil.rmtree("reports/temp_revenue_by_region")
    
    
    revenue_by_service_df = generate_revenue_by_service_report(spark, config)
    
    revenue_by_service_df.coalesce(1).write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save("reports/temp_revenue_by_service")
    
    temp_files = glob.glob("reports/temp_revenue_by_service/part-*.csv")
    if temp_files:
        shutil.move(temp_files[0], "reports/revenue/revenue_by_service.csv")
        shutil.rmtree("reports/temp_revenue_by_service")
    
    
    rated_records_report_df = generate_rated_records_report(spark, config)
    save_to_local_and_s3(rated_records_report_df, "rated_records", config, spark)
    
    rate_plans_report_df = generate_rate_plans_report(spark, config)
    save_to_local_and_s3(rate_plans_report_df, "rate_plans", config, spark)
    
    dead_records_report_df = generate_dead_records_report(spark, config)
    save_to_local_and_s3(dead_records_report_df, "dead_records", config, spark)
    
   
    spark.stop()

if __name__ == "__main__":
    main()
