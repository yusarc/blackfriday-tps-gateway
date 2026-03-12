import os
import json
import time
from kafka import KafkaConsumer
import psycopg2


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payments_raw")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "bf_payments")
PG_USER = os.getenv("POSTGRES_USER", "bf_user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "bf_pass")


def get_pg_connection():
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD,
            )
            conn.autocommit = True
            print(f"Connected to Postgres at {PG_HOST}:{PG_PORT}, db={PG_DB}")
            return conn
        except Exception as e:
            print(f"Postgres connection failed, retrying in 3s: {e}")
            time.sleep(3)


def ensure_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            transaction_id TEXT PRIMARY KEY,
            card_id TEXT,
            amount NUMERIC,
            currency TEXT,
            merchant_id TEXT,
            country TEXT,
            device_id TEXT,
            auth_method TEXT,
            event_time TIMESTAMPTZ
        );
        """
    )


def create_consumer():
    while True:
        try:
            c = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="bf-postgres-consumer",
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS} as consumer")
            return c
        except Exception as e:
            print(f"Kafka not ready for consumer, retrying in 3s: {e}")
            time.sleep(3)


def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic={KAFKA_TOPIC}")
    consumer = create_consumer()

    conn = get_pg_connection()
    cur = conn.cursor()
    ensure_table(cur)

    for msg in consumer:
        tx = msg.value
        try:
            cur.execute(
                """
                INSERT INTO payments (
                    transaction_id, card_id, amount, currency,
                    merchant_id, country, device_id, auth_method, event_time
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING;
                """,
                (
                    tx.get("transaction_id"),
                    tx.get("card_id"),
                    tx.get("amount"),
                    tx.get("currency"),
                    tx.get("merchant_id"),
                    tx.get("country"),
                    tx.get("device_id"),
                    tx.get("auth_method"),
                    tx.get("event_time"),
                ),
            )
        except Exception as e:
            print(f"Failed to insert transaction {tx.get('transaction_id')}: {e}")


if __name__ == "__main__":
    main()
