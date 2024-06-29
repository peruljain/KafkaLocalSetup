import logging
from kafka import KafkaProducer

# Enable logging
logging.basicConfig(level=logging.ERROR)

# Initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:29092')

for i in range(1, 10):
    # Define the topic and message
    topic = 'perul-test' 
    message = 'hello perul ' + str(i)
    partition_key = str(i)

    # Send the message
    future = producer.send(topic, key=partition_key.encode('utf-8'), value=message.encode('utf-8'))

    try:
        # Block until a single message is sent (or timeout)
        record_metadata = future.get(timeout=10)
    except Exception as e:
        logging.error('Error sending message', exc_info=e)
    else:
        logging.info(f'Message sent to topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}')

# Ensure all messages are sent before closing the producer
producer.flush()

# Close the producer
producer.close()
