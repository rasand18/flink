from confluent_kafka import Consumer, KafkaException

def create_consumer():
    config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(config)

def consume_messages(consumer, topic):
    try:
        consumer.subscribe([topic])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                break
            print('Received message: {}'.format(msg.value().decode('utf-8')))

    finally:
        consumer.close()

def main():
    topic = 'financial_transactions'
    consumer = create_consumer()
    consume_messages(consumer, topic)

if __name__ == '__main__':
    main()