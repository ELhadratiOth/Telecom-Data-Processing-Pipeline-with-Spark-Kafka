--- u should be  connected as admin to  run this commands 
-- use this  to  connect as admin :
-- sudo -i -u postgres
-- psql



CREATE USER othman WITH PASSWORD 'othman';
CREATE DATABASE telecom_db OWNER othman;
GRANT ALL PRIVILEGES ON DATABASE telecom_db TO othman;
\c telecom_db
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO othman;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO othman;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO othman;


-- first table  : 
CREATE TABLE normalized_records (
    record_id VARCHAR(50) PRIMARY KEY,
    record_type VARCHAR(20),
    timestamp TIMESTAMP,
    msisdn VARCHAR(15),
    secondary_msisdn VARCHAR(15),
    duration_sec FLOAT,
    data_volume_mb FLOAT,
    cell_id VARCHAR(50),
    technology VARCHAR(10),
    status VARCHAR(20),
    is_international BOOLEAN DEFAULT FALSE,
    CONSTRAINT valid_record_type CHECK (record_type IN ('voice', 'sms', 'data'))
);


-- second table :
CREATE TABLE customers (
    msisdn VARCHAR(15) PRIMARY KEY,
    customer_name VARCHAR(100),
    subscription_type VARCHAR(20) DEFAULT 'postpaid',
    rate_plan_id INT,
    activation_date DATE,
    status VARCHAR(20) DEFAULT 'active',
    region VARCHAR(50) CHECK (region IN ('rural', 'urban')),
    student BOOLEAN DEFAULT FALSE
);


--- thrid  table  

CREATE TABLE rate_plans (
    rate_plan_id INT PRIMARY KEY,
    plan_name VARCHAR(100),
    service_type VARCHAR(20),
    unit_price FLOAT,
    free_units INT DEFAULT 0,
    tiered_threshold_mb FLOAT DEFAULT NULL,
    tiered_price_mb FLOAT DEFAULT NULL
);

INSERT INTO rate_plans (rate_plan_id, plan_name, service_type, unit_price, free_units, tiered_threshold_mb, tiered_price_mb)
VALUES
    (1, 'Standard Plan', 'voice', 0.01, 2, NULL, NULL),        -- 0.01 MAD per second, 2 free minutes
    (2, 'Standard Plan', 'sms', 0.05, 5, NULL, NULL),          -- 0.05 MAD per SMS, 5 free SMS
    (3, 'Standard Plan', 'data', 0.05, 0, 10.0, 0.1),         -- 0.05 MAD/MB first 10MB, then 0.1 MAD/MB
    (4, 'Premium Plan', 'voice', 0.08, 4, NULL, NULL),         -- 0.08 MAD per second, 4 free minutes
    (5, 'Premium Plan', 'sms', 0.05, 6, NULL, NULL),           -- 0.05 MAD per SMS, 6 free SMS
    (6, 'Premium Plan', 'data', 0.04, 0, 20.0, 0.08);         -- 0.04 MAD/MB first 20MB, then 0.08 MAD/MB


--- fourth table 
CREATE TABLE rated_records (
    record_id VARCHAR(50) PRIMARY KEY,
    record_type VARCHAR(20),
    timestamp TIMESTAMP,
    msisdn VARCHAR(15),
    secondary_msisdn VARCHAR(15),
    duration_sec FLOAT,
    data_volume_mb FLOAT,
    cell_id VARCHAR(50),
    technology VARCHAR(10),
    cost FLOAT,
    rate_plan_id INT,
    rating_status VARCHAR(20),
    CONSTRAINT fk_rate_plan FOREIGN KEY (rate_plan_id) REFERENCES rate_plans(rate_plan_id),
    CONSTRAINT valid_rating_status CHECK (rating_status IN ('rated', 'unmatched', 'rejected','error'))
);

--sixth table
CREATE TABLE invoices (
    invoice_id VARCHAR(50) PRIMARY KEY,
    msisdn VARCHAR(15) NOT NULL,
    customer_name VARCHAR(100),
    billing_period_start TIMESTAMP NOT NULL,
    billing_period_end TIMESTAMP NOT NULL,
    voice_cost FLOAT DEFAULT 0.0,
    sms_cost FLOAT DEFAULT 0.0,
    data_cost FLOAT DEFAULT 0.0,
    voice_usage FLOAT DEFAULT 0.0,
    sms_usage INTEGER DEFAULT 0,
    data_usage FLOAT DEFAULT 0.0,
    total_cost FLOAT DEFAULT 0.0,
    invoice_date TIMESTAMP NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    region VARCHAR(50) CHECK (region IN ('rural', 'urban')),
    student BOOLEAN DEFAULT FALSE,
    CONSTRAINT valid_status CHECK (status IN ('pending', 'paid', 'failed'))
);

