from kafka import KafkaProducer
import time 
def produce_messages():
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
    for i in range(10):
        message = f"Message {i}".encode('utf-8')
        producer.send('topic-name', message)
        print(f"Sent: {message.decode()}")
        time.sleep(5)
    producer.flush()
    producer.close()

if __name__ == "__main__":
    produce_messages()
