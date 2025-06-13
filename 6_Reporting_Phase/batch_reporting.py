import yaml
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

def load_config(yaml_file="config.yaml"):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def check_s3_credentials():
    return (os.getenv('AWS_ACCESS_KEY_ID') and 
            os.getenv('AWS_SECRET_ACCESS_KEY') and 
            os.getenv('AWS_DEFAULT_REGION'))

def create_spark_session(config):
    """Create Spark session with S3 and PostgreSQL configuration"""
    spark = SparkSession.builder \
        .appName(config['reporting']['spark']['app_name']) \
        .master(config['reporting']['spark']['master']) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Configure AWS credentials for S3 access
    if check_s3_credentials():
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", f"s3.{os.getenv('AWS_DEFAULT_REGION')}.amazonaws.com")
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "false")
    
    return spark

def save_dataframe_to_csv_and_s3(df, report_name, config):

    local_path = f"reports/{report_name}"
    os.makedirs(local_path, exist_ok=True)
    local_csv_path = f"{local_path}/{report_name}.csv"
    
    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{local_path}/temp")
    
    import glob
    part_files = glob.glob(f"{local_path}/temp/part-*.csv")
    if part_files:
        import shutil
        shutil.move(part_files[0], local_csv_path)
        shutil.rmtree(f"{local_path}/temp")
    
    print(f"Saved locally: {local_csv_path}")
    
    if check_s3_credentials():
        try:
            bucket_name = config['reporting']['s3']['bucket_name']
            s3_path = f"s3a://{bucket_name}/{report_name}"
            
            df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(s3_path)
            
            print(f"Uploaded to S3: {s3_path}")
            
        except Exception as e:
            print(f"S3 upload failed for {report_name}: {e}")
    else:
        print(f"S3 credentials not available - {report_name} saved locally only")

def generate_customer_report(spark, config):
    query = """
    SELECT 
        msisdn,
        customer_name,
        subscription_type,
        rate_plan_id,
        activation_date,
        status,
        region,
        CASE WHEN student = true THEN 'Yes' ELSE 'No' END as is_student
    FROM customers
    ORDER BY customer_name
    """
    
    df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", f"({query}) as customers_report") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    return df

def generate_invoice_report(spark, config):
    query = """
    SELECT 
        invoice_id,
        msisdn,
        customer_name,
        ROUND(voice_cost::numeric, 2) as voice_cost,
        ROUND(sms_cost::numeric, 2) as sms_cost,
        ROUND(data_cost::numeric, 2) as data_cost,
        ROUND(total_cost::numeric, 2) as total_cost,
        voice_usage,
        sms_usage,
        data_usage,
        status,
        invoice_date
    FROM invoices
    ORDER BY invoice_date DESC
    """
    
    df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", f"({query}) as invoices_report") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    return df

def generate_revenue_by_region_report(spark, config):
    query = """
    SELECT 
        region,
        ROUND(SUM(total_cost)::numeric, 2) as total_revenue,
        COUNT(*) as customer_count,
        ROUND(AVG(total_cost)::numeric, 2) as avg_revenue_per_customer
    FROM invoices
    GROUP BY region
    ORDER BY total_revenue DESC
    """
    
    df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", f"({query}) as revenue_by_region_report") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    return df

def generate_revenue_by_service_report(spark, config):
    query = """
    SELECT 
        'voice' as service_type,
        ROUND(SUM(voice_cost)::numeric, 2) as total_revenue,
        COUNT(*) as total_invoices,
        ROUND(AVG(voice_cost)::numeric, 2) as avg_revenue_per_customer
    FROM invoices
    UNION ALL
    SELECT 
        'sms' as service_type,
        ROUND(SUM(sms_cost)::numeric, 2) as total_revenue,
        COUNT(*) as total_invoices,
        ROUND(AVG(sms_cost)::numeric, 2) as avg_revenue_per_customer
    FROM invoices
    UNION ALL
    SELECT 
        'data' as service_type,
        ROUND(SUM(data_cost)::numeric, 2) as total_revenue,
        COUNT(*) as total_invoices,
        ROUND(AVG(data_cost)::numeric, 2) as avg_revenue_per_customer
    FROM invoices
    ORDER BY total_revenue DESC
    """
    
    df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", f"({query}) as revenue_by_service_report") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    return df

def generate_rated_records_report(spark, config):
    query = """
    SELECT 
        record_id,
        record_type,
        timestamp,
        msisdn,
        ROUND(cost::numeric, 2) as cost,
        rating_status
    FROM rated_records
    WHERE rating_status = 'rated'
    ORDER BY timestamp DESC
    LIMIT 1000
    """
    
    df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", f"({query}) as rated_records_report") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    return df

def generate_rate_plans_report(spark, config):
    query = """
    SELECT 
        rate_plan_id,
        plan_name,
        service_type,
        ROUND(unit_price::numeric, 4) as unit_price,
        free_units,
        tiered_threshold_mb,
        tiered_price_mb
    FROM rate_plans
    ORDER BY service_type, plan_name
    """
    
    df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", f"({query}) as rate_plans_report") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    return df

def generate_dead_records_report(spark, config):
    query = """
    SELECT 
        record_id,
        rating_status as error_reason,
        'rating_engine' as error_source,
        timestamp as error_timestamp,
        CONCAT('record_type:', record_type, ', msisdn:', msisdn, ', duration:', duration_sec, ', data_volume:', data_volume_mb) as original_data
    FROM rated_records
    WHERE rating_status IN ('unmatched', 'rejected', 'error')
    ORDER BY timestamp DESC
    LIMIT 1000
    """
    
    df = spark.read \
        .format("jdbc") \
        .option("url", config['reporting']['database']['url']) \
        .option("dbtable", f"({query}) as dead_records_report") \
        .option("user", config['reporting']['database']['user']) \
        .option("password", config['reporting']['database']['password']) \
        .option("driver", config['reporting']['database']['driver']) \
        .load()
    
    return df

def main():
    load_dotenv(override=True)
    config = load_config()
    
    spark = create_spark_session(config)
    
    try:
        customer_df = generate_customer_report(spark, config)
        save_dataframe_to_csv_and_s3(customer_df, "customers", config)
        
        invoice_df = generate_invoice_report(spark, config)
        save_dataframe_to_csv_and_s3(invoice_df, "invoices", config)
        
        revenue_region_df = generate_revenue_by_region_report(spark, config)
        save_dataframe_to_csv_and_s3(revenue_region_df, "revenue_by_region", config)
        
        revenue_service_df = generate_revenue_by_service_report(spark, config)
        save_dataframe_to_csv_and_s3(revenue_service_df, "revenue_by_service", config)
        
        rated_df = generate_rated_records_report(spark, config)
        save_dataframe_to_csv_and_s3(rated_df, "rated_records", config)
        
        plans_df = generate_rate_plans_report(spark, config)
        save_dataframe_to_csv_and_s3(plans_df, "rate_plans", config)
        
        dead_df = generate_dead_records_report(spark, config)
        save_dataframe_to_csv_and_s3(dead_df, "dead_records", config)
        
    except Exception as e:
        print(f"Error during reporting: {e}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            spark.stop()
        except:
            pass

if __name__ == "__main__":
    main()
