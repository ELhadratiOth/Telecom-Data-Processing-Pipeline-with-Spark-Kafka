import yaml
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from jinja2 import Template
import weasyprint
from dotenv import load_dotenv

def load_config(yaml_file="config.yaml"):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def main():
    load_dotenv(override=True)      
    config = load_config()
    
    spark = SparkSession.builder \
        .appName(config['invoice_export']['spark']['app_name']) \
        .master(config['invoice_export']['spark']['master']) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.driver.host", "10.0.2.15") \
        .getOrCreate()

    target_customer = config['invoice_export']['target_customer']['customer_name']
    print(f"Generating invoice for customer: {target_customer}")

    invoice_df = spark.read \
        .format("jdbc") \
        .option("url", config['invoice_export']['database']['url']) \
        .option("dbtable", config['invoice_export']['database']['invoices_table']) \
        .option("user", config['invoice_export']['database']['user']) \
        .option("password", config['invoice_export']['database']['password']) \
        .option("driver", config['invoice_export']['database']['driver']) \
        .load()

    customers_df = spark.read \
        .format("jdbc") \
        .option("url", config['invoice_export']['database']['url']) \
        .option("dbtable", "customers") \
        .option("user", config['invoice_export']['database']['user']) \
        .option("password", config['invoice_export']['database']['password']) \
        .option("driver", config['invoice_export']['database']['driver']) \
        .load()

    rate_plans_df = spark.read \
        .format("jdbc") \
        .option("url", config['invoice_export']['database']['url']) \
        .option("dbtable", "rate_plans") \
        .option("user", config['invoice_export']['database']['user']) \
        .option("password", config['invoice_export']['database']['password']) \
        .option("driver", config['invoice_export']['database']['driver']) \
        .load()

    customer_invoice = invoice_df.filter(col("customer_name") == target_customer).orderBy(col("invoice_date").desc()).limit(1)
    
    if customer_invoice.count() == 0:
        print(f"No invoice found for customer: {target_customer}")
        spark.stop()
        return

    invoice_data = customer_invoice.collect()[0].asDict()
    
    customer_info = customers_df.filter(col("customer_name") == target_customer).collect()
    if len(customer_info) == 0:
        print(f"Customer {target_customer} not found in customers table")
        spark.stop()
        return
    
    customer_rate_plan_id = customer_info[0]['rate_plan_id']
    
    voice_plan = rate_plans_df.filter(
        (col("rate_plan_id") == customer_rate_plan_id) & 
        (col("service_type") == "voice")
    ).collect()
    
    sms_plan = rate_plans_df.filter(
        (col("rate_plan_id") == customer_rate_plan_id) & 
        (col("service_type") == "sms")
    ).collect()
    
    data_plan = rate_plans_df.filter(
        (col("rate_plan_id") == customer_rate_plan_id) & 
        (col("service_type") == "data")
    ).collect()

    def safe_float(value, default=0.0):
        return float(value) if value is not None else default
    
    def safe_int(value, default=0):
        return int(value) if value is not None else default
    
    voice_cost = safe_float(invoice_data.get('voice_cost'))
    sms_cost = safe_float(invoice_data.get('sms_cost'))
    data_cost = safe_float(invoice_data.get('data_cost'))
    total_cost = safe_float(invoice_data.get('total_cost'))
    voice_usage = safe_float(invoice_data.get('voice_usage'))
    sms_usage = safe_int(invoice_data.get('sms_usage'))
    data_usage = safe_float(invoice_data.get('data_usage'))
    
    voice_free_minutes = voice_plan[0]['free_units'] if voice_plan else 0
    sms_free_units = sms_plan[0]['free_units'] if sms_plan else 0
    data_threshold_mb = data_plan[0]['tiered_threshold_mb'] if data_plan else 0
    voice_unit_price = voice_plan[0]['unit_price'] if voice_plan else 0.0
    sms_unit_price = sms_plan[0]['unit_price'] if sms_plan else 0.0
    data_unit_price = data_plan[0]['unit_price'] if data_plan else 0.0
    data_tiered_price = data_plan[0]['tiered_price_mb'] if data_plan else 0.0
    plan_name = voice_plan[0]['plan_name'] if voice_plan else "Unknown Plan"
    
    subtotal = voice_cost + sms_cost + data_cost
    regulatory_fee = config['invoice_export']['billing']['regulatory_fee']
    
    subtotal_with_fees = subtotal + regulatory_fee
    vat_amount = (total_cost - subtotal_with_fees) if total_cost > subtotal_with_fees else 0.0
    
    voice_billable_minutes = max(0, voice_usage - voice_free_minutes)
    sms_billable_count = max(0, sms_usage - sms_free_units)
    
    current_month = datetime.now().month
    ramadan_month = config['invoice_export']['billing']['ramadan_month'] if 'billing' in config['invoice_export'] else 3
    
    ramadan_discount_amount = 0.0
    if current_month == ramadan_month:
        ramadan_discount_amount = subtotal * 0.10
    
    template_data = {
        **invoice_data,
        'voice_cost': voice_cost,
        'sms_cost': sms_cost,
        'data_cost': data_cost,
        'total_cost': total_cost,
        'voice_usage': voice_usage,  
        'sms_usage': sms_usage,
        'data_usage': data_usage,
        
        'subtotal': subtotal,
        'vat_amount': vat_amount,
        'regulatory_fee': regulatory_fee,
        
        'plan_name': plan_name,
        'voice_free_minutes': voice_free_minutes,
        'sms_free_units': sms_free_units,
        'data_threshold_mb': data_threshold_mb,
        'voice_unit_price': voice_unit_price,
        'sms_unit_price': sms_unit_price,
        'data_unit_price': data_unit_price,
        'data_tiered_price': data_tiered_price,
        
        'voice_billable_minutes': voice_billable_minutes,
        'sms_billable_count': sms_billable_count,
        
        'total_discounts': max(0, ramadan_discount_amount),
        'usage_discounts': 0.0,
        'ramadan_discount': ramadan_discount_amount,
        'is_ramadan_month': current_month == ramadan_month,
        'loyalty_discount': 0.0,
        'student_discount': 0.0,
        'urban_discount': 0.0,
    }

    pdf_path = generate_pdf_invoice(template_data, config)
    
    if config['invoice_export']['email']['enabled']:
        send_invoice_email(pdf_path, template_data, config)
    
    print(f"Invoice generated successfully for {target_customer}")
    spark.stop()

def generate_pdf_invoice(data, config):
    
    os.makedirs(config['invoice_export']['output']['pdf_path'], exist_ok=True)
    os.makedirs(config['invoice_export']['output']['temp_html_path'], exist_ok=True)
    
    with open('templates/invoice_template.html', 'r') as file:
        template_content = file.read()
    
    template = Template(template_content)
    html_content = template.render(**data)
    
    temp_html_path = os.path.join(config['invoice_export']['output']['temp_html_path'], f"invoice_{data['invoice_id']}.html")
    with open(temp_html_path, 'w') as file:
        file.write(html_content)
    
    pdf_filename = f"invoice_{data['customer_name'].replace(' ', '_')}_{data['invoice_id']}.pdf"
    pdf_path = os.path.join(config['invoice_export']['output']['pdf_path'], pdf_filename)
    
    weasyprint.HTML(filename=temp_html_path).write_pdf(pdf_path)
    
    os.remove(temp_html_path)
    
    print(f"PDF generated: {pdf_path}")
    return pdf_path

def send_invoice_email(pdf_path, invoice_data, config):
    
    sender_email = os.getenv('SENDER_EMAIL')
    sender_password = os.getenv('SMTP_PASSWORD')
    smtp_server = os.getenv('SMTP_SERVER')
    smtp_port = int(os.getenv('SMTP_PORT'))
    
    recipient_email = config['invoice_export']['email']['recipient']
    subject = config['invoice_export']['email']['subject_template'].format(
        invoice_date=invoice_data['invoice_date'].strftime('%Y-%m-%d')
    )
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    
    body = f"""
    Dear {invoice_data['customer_name']},

    {config['invoice_export']['email']['body_template']}

    Invoice Summary:
    - Invoice ID: {invoice_data['invoice_id']}
    - Billing Period: {invoice_data['billing_period_start'].strftime('%Y-%m-%d')} to {invoice_data['billing_period_end'].strftime('%Y-%m-%d')}
    - Total Amount: {invoice_data['total_cost']:.2f} MAD
    - Status: {invoice_data['status']}

    Best regards,
    Telecom Morocco Customer Service
    """
    
    msg.attach(MIMEText(body, 'plain'))
    
    with open(pdf_path, "rb") as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
    
    encoders.encode_base64(part)
    part.add_header(
        'Content-Disposition',
        f'attachment; filename= {os.path.basename(pdf_path)}'
    )
    msg.attach(part)
    
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(sender_email, sender_password)
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        print(f"Email sent successfully to {recipient_email}")
    except Exception as e:
        print(f"Error sending email: {e}")

if __name__ == "__main__":
    main()
