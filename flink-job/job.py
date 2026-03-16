from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types

import json


KAFKA_TOPIC = "payments_raw"
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_GROUP_ID = "flink-tps-consumer"


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

    # ---------- Ortak: JSON parse edip temel alanları çıkar ----------
    # (country, amount, auth_method) tuple'ı
    def to_tuple(s: str):
        try:
            e = json.loads(s)
            country = e.get("country", "UNKNOWN")
            amount = float(e.get("amount", 0.0))
            auth_method = e.get("auth_method", "UNKNOWN")
        except Exception:
            country = "PARSE_ERROR"
            amount = 0.0
            auth_method = "UNKNOWN"
        return (country, amount, auth_method)

    events = stream.map(
        to_tuple,
        output_type=Types.TUPLE([
            Types.STRING(),  # country
            Types.FLOAT(),   # amount
            Types.STRING()   # auth_method
        ])
    )

    # ---------- 1) Country-based TPS ----------
    # (country, 1)
    country_counts = events.map(
        lambda e: (e[0], 1),
        output_type=Types.TUPLE([Types.STRING(), Types.INT()])
    )

    country_tps = (
        country_counts
        .key_by(lambda x: x[0])  # country
        .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    )

    # ---------- 2) Global TPS ----------
    global_ones = country_counts.map(
        lambda x: x[1],
        output_type=Types.INT()
    )

    global_tps = (
        global_ones
        .window_all(TumblingProcessingTimeWindows.of(Time.seconds(1)))
        .reduce(lambda a, b: a + b)
    )

    # ---------- 3) High-value TPS (amount > 100) ----------
    high_value_flags = events.map(
        lambda e: 1 if e[1] > 100.0 else 0,
        output_type=Types.INT()
    )

    high_value_tps = (
        high_value_flags
        .window_all(TumblingProcessingTimeWindows.of(Time.seconds(1)))
        .reduce(lambda a, b: a + b)
    )

    # ---------- 4) 3DS auth TPS ----------
    auth_3ds_flags = events.map(
        lambda e: 1 if e[2] == "3DS" else 0,
        output_type=Types.INT()
    )

    auth_3ds_tps = (
        auth_3ds_flags
        .window_all(TumblingProcessingTimeWindows.of(Time.seconds(1)))
        .reduce(lambda a, b: a + b)
    )

    # ---------- Print outputs with prefixes ----------
    country_tps.map(
        lambda x: f"COUNTRY_TPS country={x[0]} tps={x[1]}",
        output_type=Types.STRING()
    ).print()

    global_tps.map(
        lambda v: f"GLOBAL_TPS tps={v}",
        output_type=Types.STRING()
    ).print()

    high_value_tps.map(
        lambda v: f"HIGH_VALUE_TPS amount>100 tps={v}",
        output_type=Types.STRING()
    ).print()

    auth_3ds_tps.map(
        lambda v: f"AUTH_3DS_TPS tps={v}",
        output_type=Types.STRING()
    ).print()

    env.execute("Country, High-Value, 3DS TPS")


if __name__ == "__main__":
    main()


