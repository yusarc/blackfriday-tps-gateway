import os
import time
import random
import json
from kafka import KafkaProducer
from faker import Faker


bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic = os.getenv("KAFKA_TOPIC", "payments_raw")
BLACKFRIDAY_MODE = os.getenv("BLACKFRIDAY_MODE", "false").lower() == "true"

fake = Faker()

HOME_COUNTRY = "DE"
OTHER_COUNTRIES = ["FR", "ES", "IT", "US", "TR"]

CARD_POOL = [fake.credit_card_number() for _ in range(5000)]
MERCHANT_POOL = [fake.uuid4() for _ in range(2000)]


# --------- Normal gün dağılımları ---------
def sample_amount_normal():
    r = random.random()
    if r < 0.70:
        return round(random.uniform(5, 50), 2)
    elif r < 0.95:
        return round(random.uniform(50, 200), 2)
    else:
        return round(random.uniform(200, 1000), 2)


def sample_country_normal():
    r = random.random()
    if r < 0.60:
        return HOME_COUNTRY
    else:
        return random.choice(OTHER_COUNTRIES)


def sample_auth_method_normal():
    r = random.random()
    if r < 0.50:
        return "PIN"
    elif r < 0.80:
        return "3DS"
    elif r < 0.95:
        return "BIOMETRIC"
    else:
        return "NONE"


# --------- Black Friday dağılımları (peak fazı) ---------
def sample_amount_blackfriday():
    r = random.random()
    if r < 0.40:
        return round(random.uniform(5, 50), 2)
    elif r < 0.80:
        return round(random.uniform(50, 200), 2)
    else:
        return round(random.uniform(200, 1500), 2)  # daha yüksek tail


def sample_country_blackfriday():
    r = random.random()
    if r < 0.40:
        return HOME_COUNTRY
    else:
        return random.choice(OTHER_COUNTRIES)  # daha çok cross-border


def sample_auth_method_blackfriday():
    r = random.random()
    if r < 0.35:
        return "PIN"
    elif r < 0.65:
        return "3DS"
    elif r < 0.80:
        return "BIOMETRIC"
    else:
        return "NONE"  # normal güne göre daha yüksek NONE oranı


def generate_transaction(amount_sampler, country_sampler, auth_sampler):
    return {
        "transaction_id": fake.uuid4(),
        "card_id": random.choice(CARD_POOL),
        "amount": amount_sampler(),
        "currency": "EUR",
        "merchant_id": random.choice(MERCHANT_POOL),
        "country": country_sampler(),
        "device_id": fake.uuid4(),
        "auth_method": auth_sampler(),
        "event_time": fake.iso8601(),
    }


def create_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"Connected to Kafka at {bootstrap}")
            return p
        except Exception as e:
            print(f"Kafka not ready, retrying in 3s: {e}")
            time.sleep(3)


producer = create_producer()


def normal_day_loop():
    print(f"[NORMAL] Sending messages to {bootstrap}, topic={topic}")
    while True:
        tx = generate_transaction(
            sample_amount_normal,
            sample_country_normal,
            sample_auth_method_normal,
        )
        producer.send(topic, tx)
        time.sleep(0.1)  # ~10 TPS


def blackfriday_loop():
    """
    3 faz:
    - warmup: 30s, ~10 TPS
    - peak:   60s, ~50 TPS
    - cooldown: 30s, ~10 TPS
    """
    print(f"[BLACKFRIDAY] Sending messages to {bootstrap}, topic={topic}")

    # Warmup
    print("[BLACKFRIDAY] Warmup phase")
    warmup_end = time.time() + 30
    while time.time() < warmup_end:
        tx = generate_transaction(
            sample_amount_normal,
            sample_country_normal,
            sample_auth_method_normal,
        )
        producer.send(topic, tx)
        time.sleep(0.1)  # ~10 TPS

    # Peak
    print("[BLACKFRIDAY] Peak phase")
    peak_end = time.time() + 60
    while time.time() < peak_end:
        tx = generate_transaction(
            sample_amount_blackfriday,
            sample_country_blackfriday,
            sample_auth_method_blackfriday,
        )
        producer.send(topic, tx)
        time.sleep(0.02)  # ~50 TPS (makinen ne kadar kaldırırsa)

    # Cooldown
    print("[BLACKFRIDAY] Cooldown phase")
    cooldown_end = time.time() + 30
    while time.time() < cooldown_end:
        tx = generate_transaction(
            sample_amount_normal,
            sample_country_normal,
            sample_auth_method_normal,
        )
        producer.send(topic, tx)
        time.sleep(0.1)

    print("[BLACKFRIDAY] Scenario finished, exiting.")
    # İstersen burada tekrar normal_loop'a geçebilirsin; şimdilik duruyoruz.


if __name__ == "__main__":
    if BLACKFRIDAY_MODE:
        blackfriday_loop()
    else:
        normal_day_loop()
