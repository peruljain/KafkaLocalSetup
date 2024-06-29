from kafka import KafkaConsumer
import time

# Initialize the Kafka consumer
consumer = KafkaConsumer(
    'perul-test',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',  # Start reading at the earliest message
    group_id='perul-test-consumer'  
)

# Consume messages
for message in consumer:
    print(f'Received message: {message.value.decode("utf-8")}')
    time.sleep(1)

# Close the consumer
consumer.close()
