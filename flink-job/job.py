from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types

import json
import psycopg2
from datetime import datetime


KAFKA_TOPIC = "payments_raw"
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_GROUP_ID = "flink-tps-consumer"

PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = "bf_payments"
PG_USER = "bf_user"
PG_PASSWORD = "bf_pass"


def amount_to_bucket(amount: float) -> str:
    if amount < 50:
        return "0-50"
    elif amount < 100:
        return "50-100"
    else:
        return "100+"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)

    props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "latest",
    }

    consumer = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=props,
    )

    stream = env.add_source(consumer)

    # ---------- JSON -> (country, auth_method, bucket, 1) ----------
    def to_dim_tuple(s: str):
        try:
            e = json.loads(s)
            country = e.get("country", "UNKNOWN")
            amount = float(e.get("amount", 0.0))
            auth_method = e.get("auth_method", "UNKNOWN")
            bucket = amount_to_bucket(amount)
        except Exception:
            country = "PARSE_ERROR"
            auth_method = "UNKNOWN"
            bucket = "0-50"
        return (country, auth_method, bucket, 1)

    events = stream.map(
        to_dim_tuple,
        output_type=Types.TUPLE([
            Types.STRING(),  # country
            Types.STRING(),  # auth_method
            Types.STRING(),  # amount_bucket
            Types.INT()      # count
        ])
    )

    # ---------- Key: (country, auth_method, bucket) + 1s window + sum ----------
    keyed = events.key_by(lambda e: (e[0], e[1], e[2]))

    windowed = (
        keyed
        .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
        .reduce(lambda a, b: (a[0], a[1], a[2], a[3] + b[3]))
    )

    # ---------- Postgres'e yazan map ----------
    def write_to_postgres(rec):
        country, auth_method, bucket, tps = rec

        # ts: Flink'in window end timestamp'ını burada bilmiyoruz; basitçe NOW() kullanıyoruz
        ts = datetime.utcnow()

        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO tps_metrics (ts, country, auth_method, amount_bucket, tps)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (ts, country, auth_method, bucket, int(tps)),
        )
        conn.commit()
        cur.close()
        conn.close()

        # Log için string döndürelim
        return f"INSERTED ts={ts} country={country} auth={auth_method} bucket={bucket} tps={tps}"

    result = windowed.map(
        write_to_postgres,
        output_type=Types.STRING()
    )

    result.print()

    env.execute("TPS metrics to Postgres")


if __name__ == "__main__":
    main()
