from kafka import KafkaConsumer

def consume_messages():
    consumer = KafkaConsumer(
        'normalized_records',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        # group_id='telecom-group',
        enable_auto_commit=True
    )

    print("Starting consumer, waiting for messages...")
    for message in consumer:
        print(f"Received: {message.value.decode('utf-8')}")

if __name__ == "__main__":
    consume_messages()
