import json
import random
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from kafka import KafkaProducer
import uuid
import yaml
import string
import os
import psycopg2

def load_config(yaml_file="config.yaml"):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

customer_phone_numbers = []

def load_customer_phone_numbers(config):
    global customer_phone_numbers, customer_rate_plans, customers_by_service
    
    try:
        db_config = config.get('database', {})
        conn = psycopg2.connect(
            host=db_config.get('host', 'localhost'),
            database=db_config.get('database', 'telecom_db'),
            user=db_config.get('user', 'othman'),
            password=db_config.get('password', 'othman')
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT msisdn, rate_plan_id 
            FROM customers 
            WHERE status = 'active'
        """)
        results = cursor.fetchall()
        
        customer_phone_numbers = [row[0] for row in results]
        customer_rate_plans = {row[0]: row[1] for row in results}
        
        for msisdn, rate_plan_id in results:
            if rate_plan_id in [1, 4]:  
                customers_by_service["voice"].append((msisdn, rate_plan_id))
            if rate_plan_id in [2, 5]:  
                customers_by_service["sms"].append((msisdn, rate_plan_id))
            if rate_plan_id in [3, 6]: 
                customers_by_service["data"].append((msisdn, rate_plan_id))
        
        cursor.close()
        conn.close()
        
        print(f"Loaded {len(customer_phone_numbers)} customers with rate plan mappings from database")
        print(f"Voice customers: {len(customers_by_service['voice'])}")
        print(f"SMS customers: {len(customers_by_service['sms'])}")
        print(f"Data customers: {len(customers_by_service['data'])}")
        
    except Exception as e:
        print(f"Error loading customer data: {e}")
        customer_phone_numbers = []
        customer_rate_plans = {}
        customers_by_service = {"voice": [], "sms": [], "data": []}

customers_by_service = {
    "voice": [],
    "sms": [],
    "data": []
}

def select_random_customer_for_service(service_type, config):
    global customers_by_service
    
    customer_prob = config['data_generation']['customer_generation_probability']
    if random.random() > customer_prob:
        return None, None
    
    eligible_customers = customers_by_service.get(service_type, [])
    
    if not eligible_customers:
        print(f"No customers found for service type: {service_type}")
        return None, None
    
    random_customer = random.choice(eligible_customers)
    msisdn, rate_plan_id = random_customer
    
    return msisdn, rate_plan_id

def kafka_producer(config):
    return KafkaProducer(
        bootstrap_servers=config['output']['kafka']['bootstrap_servers'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

def generate_phone_number(error_mode=False, international=False, config=None):
    global customer_phone_numbers
    
    if error_mode:
        valid_customer_prob = config['data_generation']['valid_customer_in_error_mode']
        if random.random() < valid_customer_prob and customer_phone_numbers:
            return random.choice(customer_phone_numbers)
        digits = ''.join(random.choices(string.digits, k=9))
        return f"+999{digits}"
    
    if international:
        country_code = random.choice(["+33", "+1"])
        if country_code == "+33":
            digits = ''.join(random.choices(string.digits, k=9))  
        else:  # +1 
            digits = ''.join(random.choices(string.digits, k=10))  
        return f"{country_code}{digits}"
    
    if customer_phone_numbers:
        return random.choice(customer_phone_numbers)
    else:
        digits = ''.join(random.choices(string.digits, k=9))
        return f"+212{digits}"

def generate_record(record_type, timestamp, error_mode=False, config=None, preferred_customer=None):
    record = {"record_id": str(uuid.uuid4())}
    tech = random.choice(config['data_generation']['technologies'])
    
    is_roaming = random.random() < config['data_generation']['roaming_ratio']
    if is_roaming:
        cell_id = random.choice(config['data_generation']['international_cell_ids'])
    else:
        cell_id = random.choice(config['data_generation']['cell_ids'])
    
    if record_type == "voice":
        is_international_call = random.random() < config['data_generation']['international_call_ratio']
        
        if error_mode:
            fake_phone_prob = config['data_generation']['error_probabilities']['fake_phone_number']
            if random.random() < fake_phone_prob:
                caller_id = generate_phone_number(error_mode=True, config=config)
                rate_plan_id = random.choice([1, 4])  # Random voice plan
            else:
                caller_id, rate_plan_id = select_random_customer_for_service("voice", config)
                if caller_id is None:
                    caller_id = generate_phone_number(config=config)
                    rate_plan_id = random.choice([1, 4])
        else:
            caller_id, rate_plan_id = select_random_customer_for_service("voice", config)
            if caller_id is None:
                caller_id = generate_phone_number(config=config)
                rate_plan_id = random.choice([1, 4])
        
        error_callee_prob = config['data_generation']['error_probabilities']['error_callee_receiver']
        callee_id = generate_phone_number(
            error_mode and random.random() < error_callee_prob,
            international=is_international_call,
            config=config
        )
        
        duration = random.randint(200, 2000)
        if error_mode:
            negative_duration_prob = config['data_generation']['error_probabilities']['negative_duration']
            if random.random() < negative_duration_prob:
                duration = -abs(duration)
        
        record.update({
            "record_type": "voice",
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "caller_id": caller_id,
            "callee_id": callee_id,
            "duration_sec": duration,
            "cell_id": cell_id,
            "technology": tech,
            "is_international": is_international_call,
            "rate_plan_id": rate_plan_id
        })
    elif record_type == "sms":
        is_international_sms = random.random() < config['data_generation']['international_sms_ratio']
        
        if error_mode:
            fake_phone_prob = config['data_generation']['error_probabilities']['fake_phone_number']
            if random.random() < fake_phone_prob:
                sender_id = generate_phone_number(error_mode=True, config=config)
                rate_plan_id = random.choice([2, 5])  # Random SMS plan
            else:
                sender_id, rate_plan_id = select_random_customer_for_service("sms", config)
                if sender_id is None:
                    sender_id = generate_phone_number(config=config)
                    rate_plan_id = random.choice([2, 5])
        else:
            sender_id, rate_plan_id = select_random_customer_for_service("sms", config)
            if sender_id is None:
                sender_id = generate_phone_number(config=config)
                rate_plan_id = random.choice([2, 5])
        
        error_receiver_prob = config['data_generation']['error_probabilities']['error_callee_receiver']
        receiver_id = generate_phone_number(
            error_mode and random.random() < error_receiver_prob,
            international=is_international_sms,
            config=config
        )
        
        record.update({
            "record_type": "sms",
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "cell_id": cell_id,
            "technology": tech,
            "is_international": is_international_sms,
            "rate_plan_id": rate_plan_id
        })
    elif record_type == "data":
        data_volume = random.uniform(100, 500) if tech == "2G" else random.uniform(500, 10000)
        
        if error_mode:
            negative_data_prob = config['data_generation']['error_probabilities']['negative_data_volume']
            if random.random() < negative_data_prob:
                data_volume = -abs(data_volume)
        
        if error_mode:
            fake_phone_prob = config['data_generation']['error_probabilities']['fake_phone_number']
            if random.random() < fake_phone_prob:
                user_id = generate_phone_number(error_mode=True, config=config)
                rate_plan_id = random.choice([3, 6])  # Random data plan
            else:
                user_id, rate_plan_id = select_random_customer_for_service("data", config)
                if user_id is None:
                    user_id = generate_phone_number(config=config)
                    rate_plan_id = random.choice([3, 6])
        else:
            user_id, rate_plan_id = select_random_customer_for_service("data", config)
            if user_id is None:
                user_id = generate_phone_number(config=config)
                rate_plan_id = random.choice([3, 6])
        
        record.update({
            "record_type": "data",
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": user_id,
            "data_volume_mb": round(data_volume, 2),
            "session_duration_sec": random.randint(30, 3600),
            "cell_id": cell_id,
            "technology": tech,
            "is_international": False,  
            "rate_plan_id": rate_plan_id
        })

    if error_mode:
        error_type = random.choice(["missing_field", "unrecognized_service", "wrong_rate_plan", "none"])
        
        missing_field_prob = config['data_generation']['error_probabilities']['missing_field']
        if error_type == "missing_field" and random.random() < missing_field_prob:
            if record_type == "voice":
                if random.choice([True, False]):
                    del record["callee_id"] 
                else:
                    del record["duration_sec"]  
            elif record_type == "sms":
                del record["receiver_id"] 
            elif record_type == "data":
                if random.choice([True, False]):
                    del record["data_volume_mb"]  
                else:
                    del record["session_duration_sec"] 
                    
        unrecognized_service_prob = config['data_generation']['error_probabilities']['unrecognized_service']
        if error_type == "unrecognized_service" and random.random() < unrecognized_service_prob:
            record["record_type"] = random.choice(["unknown", "invalid", "test"])
            
        wrong_rate_plan_prob = config['data_generation']['error_probabilities']['wrong_rate_plan']
        if error_type == "wrong_rate_plan" and random.random() < wrong_rate_plan_prob:
            all_plans = [1, 2, 3, 4, 5, 6, 99, 0, -1]  
            record["rate_plan_id"] = random.choice(all_plans)

    return record

def generate_batch(num_records, config):
    records = []
    
    current_timestamp = datetime.now()
    
    print(f"Generating {num_records} records with current timestamp: {current_timestamp}")
    
    for _ in range(num_records):
        timestamp_variation = timedelta(seconds=random.randint(0, 59))
        timestamp = current_timestamp + timestamp_variation
        
        record_type = random.choices(
            list(config['data_generation']['service_distribution'].keys()),
            weights=list(config['data_generation']['service_distribution'].values()),
            k=1
        )[0]
        
        error_mode = random.random() < config['data_generation']['error_ratio']
        record = generate_record(record_type, timestamp, error_mode, config)
        records.append(record)
    return records

def save_to_file(records, config):
    output_dir = config['output']['batch_output_path']
    os.makedirs(output_dir, exist_ok=True)
    
    if config['output']['format'] == "csv":
        file_path = os.path.join(output_dir, "telecom_records.csv")
        df = pd.DataFrame(records)
        mode = 'a' if os.path.exists(file_path) else 'w'
        df.to_csv(file_path, mode=mode, index=False, header=not os.path.exists(file_path))
    elif config['output']['format'] == "jsonl":
        file_path = os.path.join(output_dir, "telecom_records.jsonl")
        with open(file_path, "a") as f:
            for record in records:
                json.dump(record, f)
                f.write("\n")

def stream_to_kafka(records, producer, config):
    for record in records:
        try:
            producer.send(
                topic=config['output']['kafka']['topic'],
                key=record["record_id"],
                value=record
            )
            producer.flush()
            print(f"Sent record ID {record['record_id']} (type: {record['record_type']}) to Kafka topic {config['output']['kafka']['topic']}")
            
        except Exception as e:
            print(f"Failed to send record {record['record_id']}: {e}")
        time.sleep(config['output']['generation']['sleep'])  
    print(f"Streamed {len(records)} records to Kafka topic {config['output']['kafka']['topic']}")

def main():
    config = load_config()
    
    load_customer_phone_numbers(config)
    
    current_time = datetime.now()
    records = generate_batch(config['data_generation']['records_per_hour'], current_time, config)
    
    if config['output']['format'] in ["csv", "jsonl"]:
        save_to_file(records, config)
        print(f"Appended {len(records)} records to {config['output']['format']} file")
    elif config['output']['format'] == "kafka":
        producer = kafka_producer(config)
        stream_to_kafka(records, producer, config)
        producer.flush()

if __name__ == "__main__":
    main()