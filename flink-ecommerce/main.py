import json
import random
import time

from faker import Faker
from confluent_kafka import SerializingProducer
from datetime import datetime

fake = Faker()

def generate_sales_transactions():
    user = fake.simple_profile()

    return {
        "transactionId": fake.uuid4(),
        "productId": random.choice(['product1','product2','product3','product4','product5']),
        "productName": random.choice(['laptop','mobile','tv','watch','headphones']),
        "transactionDate": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f%z')
    }

def delivery_report(err,msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition()}]")

def main():
    topic = 'financial_transactions'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092'
    })


    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 12000000:

        transaction = generate_sales_transactions()

        producer.produce(topic,
                         key=transaction['transactionId'],
                         value=json.dumps(transaction),
                         on_delivery=delivery_report)

        print(transaction)

        producer.poll(0)

        time.sleep(1)

if __name__ == "__main__":
    main()