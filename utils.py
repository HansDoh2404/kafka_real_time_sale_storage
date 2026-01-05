import random
import uuid
from config import NUM_CUSTOMERS, NUM_PRODUCTS, NUM_STORES
from datetime import datetime, timedelta

base_time = datetime.now()

def delivery_report(err, msg):
    if err is not None:
        print(f"[DELIVERY ERROR] {err}")

def generate_sales(timestamp) :

    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(10.0, 500.0), 2)
    sale = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.randint(1, NUM_CUSTOMERS),
        "product_id": random.randint(1, NUM_PRODUCTS),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": round(quantity * unit_price, 2),
        "transaction_timestamp": (
            base_time - timedelta(minutes=random.randint(0, 1440))
        ).isoformat(),
        "payment_method": random.choice([
            "credit_card", "debit_card", "cash", "digital_wallet"
        ]),
        "store_id": random.randint(1, NUM_STORES),
        "status": random.choice(["completed", "pending", "cancelled"]),
        "file_name": f"sales_{timestamp}"
    }

    # ~20% de données sales
    if random.random() < 0.20:
        issue = random.choice([
            "null_customer", "invalid_product", "negative_amount",
            "future_date", "invalid_status", "zero_quantity",
            "mismatched_total", "invalid_payment", "null_timestamp"
        ])

        if issue == "null_customer":
            sale["customer_id"] = None
        elif issue == "invalid_product":
            sale["product_id"] = random.randint(9000, 9999)
        elif issue == "negative_amount":
            sale["total_amount"] = -abs(sale["total_amount"])
        elif issue == "future_date":
            sale["transaction_timestamp"] = (
                base_time + timedelta(days=random.randint(1, 30))
            ).isoformat()
        elif issue == "invalid_status":
            sale["status"] = random.choice(["UNKNOWN", "", None])
        elif issue == "zero_quantity":
            sale["quantity"] = 0
        elif issue == "mismatched_total":
            sale["total_amount"] = round(random.uniform(1.0, 1000.0), 2)
        elif issue == "invalid_payment":
            sale["payment_method"] = random.choice(["INVALID", "", None])
        elif issue == "null_timestamp":
            sale["transaction_timestamp"] = None

    key = str(sale["customer_id"])

    return key, sale