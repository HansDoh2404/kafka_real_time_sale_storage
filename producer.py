import os
import json
import time
import csv
import random

from utils import generate_sales, delivery_report
from config import producer_conf, DATA_DIR, STREAM_INTERVAL, SALES_PER_BATCH, TOPIC
from datetime import datetime, timedelta
from faker import Faker
from faker_commerce import Provider
from confluent_kafka import Producer


# Initialisation Faker
fake = Faker()
fake.add_provider(Provider)
Faker.seed(int(datetime.now().timestamp()))
random.seed(int(datetime.now().timestamp()))

producer = Producer(producer_conf)


def sales_producer():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = os.path.join(DATA_DIR, f"sales_{timestamp}.csv")
    os.makedirs(DATA_DIR, exist_ok=True)

    fieldnames = [
        "transaction_id", "customer_id", "product_id", "quantity",
        "unit_price", "total_amount", "transaction_timestamp",
        "payment_method", "store_id", "status", "file_name"
    ]

    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(SALES_PER_BATCH):

            key, sale = generate_sales(timestamp)
            producer.produce(
                topic=TOPIC,
                key=key,
                value=json.dumps(sale),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            writer.writerow(sale)
        producer.flush()

    print(f"[GEN] {SALES_PER_BATCH} ventes écrites dans : {file_path}")


while True: 
    sales_producer()
    time.sleep(STREAM_INTERVAL)
