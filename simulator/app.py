import os
import time
import random
import json
from kafka import KafkaProducer
from faker import Faker

bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "payments_raw")

fake = Faker()

HOME_COUNTRY = "DE"
OTHER_COUNTRIES = ["FR", "ES", "IT", "US", "TR"]

CARD_POOL = [fake.credit_card_number() for _ in range(5000)]
MERCHANT_POOL = [fake.uuid4() for _ in range(2000)]

def sample_amount():
    r = random.random()
    if r < 0.70:
        return round(random.uniform(5, 50), 2)
    elif r < 0.95:
        return round(random.uniform(50, 200), 2)
    else:
        return round(random.uniform(200, 1000), 2)

def sample_country():
    r = random.random()
    if r < 0.60:
        return HOME_COUNTRY
    else:
        return random.choice(OTHER_COUNTRIES)

def sample_auth_method():
    r = random.random()
    if r < 0.50:
        return "PIN"
    elif r < 0.80:
        return "3DS"
    elif r < 0.95:
        return "BIOMETRIC"
    else:
        return "NONE"

def generate_transaction():
    return {
        "transaction_id": fake.uuid4(),
        "card_id": random.choice(CARD_POOL),
        "amount": sample_amount(),
        "currency": "EUR",
        "merchant_id": random.choice(MERCHANT_POOL),
        "country": sample_country(),
        "device_id": fake.uuid4(),
        "auth_method": sample_auth_method(),
        "event_time": fake.iso8601(),
    }

producer = KafkaProducer(
    bootstrap_servers=bootstrap,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

if __name__ == "__main__":
    print(f"Sending messages to {bootstrap}, topic={topic}")
    while True:
        tx = generate_transaction()
        producer.send(topic, tx)
        #  ≈10 TPS
        time.sleep(0.1)
