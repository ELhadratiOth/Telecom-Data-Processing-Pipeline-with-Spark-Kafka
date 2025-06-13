import yaml
import random
import psycopg2
from datetime import datetime, timedelta
import string
import uuid

def load_config(yaml_file="config.yaml"):
    with open(yaml_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def generate_moroccan_phone():
    # Generate 9 digits after +212 
    digits = ''.join(random.choices(string.digits, k=9))
    return f"+212{digits}"

def generate_customer_name():
    first_names = [
        'Othman', 'Mohammed', 'Fatima', 'Aicha', 'Hassan', 'Youssef', 'Khadija', 'Omar', 
        'Amina', 'Said', 'Laila', 'Rachid', 'Zineb', 'Khalid', 'Nadia', 'Abdellatif',
        'Samira', 'Mustapha', 'Leila', 'Karim', 'Hayat', 'Abderrahim', 'Souad', 'Brahim',
        'Malika', 'Driss', 'Rajae', 'Aziz', 'Houda', 'Othman', 'Sanaa', 'Yassine'
    ]
    
    last_names = [
        'El Hadrati', 'Benali', 'Cherkaoui', 'El Fassi', 'Hassani', 'Idrissi', 'Jamal',
        'Kabbaj', 'Lamrani', 'Mansouri', 'Naciri', 'Ouali', 'Qadiri', 'Rhazi', 'Squalli',
        'Tazi', 'Usmani', 'Wahabi', 'Yacoubi', 'Ziani', 'Amrani', 'Berrada', 'Chraibi',
        'Douiri', 'El Alami', 'Filali', 'Ghazi', 'Hamdi', 'Iraqi', 'Jebari', 'Kettani'
    ]
    
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    
    unique_id = str(uuid.uuid4()).replace('-', '')[:5].upper()
    
    return f"{first_name} {last_name} {unique_id}"

def generate_activation_date(config):
    end_date = datetime.now()
    years_range = config['customer_generation']['activation_date_range_years']
    start_date = end_date - timedelta(days=years_range*365)
    
    random_days = random.randint(0, (end_date - start_date).days)
    activation_date = start_date + timedelta(days=random_days)
    
    return activation_date.strftime('%Y-%m-%d')

def select_rate_plan(config):
    rate_plans = config['customer_generation']['rate_plan_distribution']
    return random.choices(list(rate_plans.keys()), weights=list(rate_plans.values()))[0]

def generate_customer_batch(batch_size, config):
    customers = []
    used_phone_numbers = set()
    
    for _ in range(batch_size):
        phone_number = generate_moroccan_phone()
        while phone_number in used_phone_numbers:
            phone_number = generate_moroccan_phone()
        used_phone_numbers.add(phone_number)
        
        status_dist = config['customer_generation']['status_distribution']
        region_dist = config['customer_generation']['region_distribution']
        student_ratio = config['customer_generation']['student_ratio']
        
        customer = {
            'msisdn': phone_number,
            'customer_name': generate_customer_name(),
            'subscription_type': 'postpaid',  
            'rate_plan_id': select_rate_plan(config),
            'activation_date': generate_activation_date(config),
            'status': random.choices(
                list(status_dist.keys()), 
                weights=list(status_dist.values())
            )[0],
            'region': random.choices(
                list(region_dist.keys()), 
                weights=list(region_dist.values())
            )[0],
            'student': random.choices([True, False], weights=[student_ratio, 1-student_ratio])[0]
        }
        
        customers.append(customer)
    
    return customers

def store_customers_batch(customers, config):
    try:
        db_config = config.get('database', {})
        conn = psycopg2.connect(
            host=db_config.get('host', 'localhost'),
            database=db_config.get('database', 'telecom_db'),
            user=db_config.get('user', 'othman'),
            password=db_config.get('password', 'othman')
        )
        conn.autocommit = False
        cursor = conn.cursor()
        
        successful_inserts = 0
        failed_inserts = 0
        
        
        for customer in customers:
            try:
                cursor.execute("""
                    INSERT INTO customers (msisdn, customer_name, subscription_type, rate_plan_id, 
                                         activation_date, status, region, student)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (msisdn) DO NOTHING
                """, (
                    customer['msisdn'],
                    customer['customer_name'],
                    customer['subscription_type'],
                    customer['rate_plan_id'],
                    customer['activation_date'],
                    customer['status'],
                    customer['region'],
                    customer['student']
                ))
                
                if cursor.rowcount > 0:
                    successful_inserts += 1
                
            except Exception as e:
                print(f"Error inserting customer {customer['msisdn']}: {e}")
                conn.rollback()
                failed_inserts += 1
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        
        return successful_inserts
        
    except Exception as e:
        print(f"Database connection error: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        return 0

def generate_customer_statistics(customers):
    total = len(customers)
    
    rate_plan_count = {}
    for customer in customers:
        plan_id = customer['rate_plan_id']
        rate_plan_count[plan_id] = rate_plan_count.get(plan_id, 0) + 1
    
    urban_count = sum(1 for c in customers if c['region'] == 'urban')
    rural_count = total - urban_count
    
    student_count = sum(1 for c in customers if c['student'])
    
    active_count = sum(1 for c in customers if c['status'] == 'active')
    suspended_count = total - active_count

    for plan_id, count in sorted(rate_plan_count.items()):
        percentage = (count/total)*100
        print(f"  Plan {plan_id}: {count} customers ({percentage:.1f}%)")
    
    print(f"\nRegion Distribution:")
    print(f"  Urban: {urban_count} ({(urban_count/total)*100:.1f}%)")
    print(f"  Rural: {rural_count} ({(rural_count/total)*100:.1f}%)")
    
    print(f"\nStudent Status:")
    print(f"  Students: {student_count} ({(student_count/total)*100:.1f}%)")
    print(f"  Non-students: {total-student_count} ({((total-student_count)/total)*100:.1f}%)")
    
    print(f"\nAccount Status:")
    print(f"  Active: {active_count} ({(active_count/total)*100:.1f}%)")
    print(f"  Suspended: {suspended_count} ({(suspended_count/total)*100:.1f}%)")
    
    print(f"\nSubscription Type:")
    print(f"  Postpaid: {total} (100.0%)")

def main():
    config = load_config()
    
    
    num_customers = config['customer_generation']['default_count']

    print(f"Generating {num_customers}")
    
    batch_size = config['customer_generation']['batch_size']
    total_inserted = 0
    all_customers = []
    
    for i in range(0, num_customers, batch_size):
        current_batch_size = min(batch_size, num_customers - i)
        
        customer_batch = generate_customer_batch(current_batch_size, config)
        all_customers.extend(customer_batch)
        
        inserted = store_customers_batch(customer_batch, config)
        total_inserted += inserted
    
    generate_customer_statistics(all_customers)
    
    print(f"Total customers inserted into database: {total_inserted}")

if __name__ == "__main__":
    main()
