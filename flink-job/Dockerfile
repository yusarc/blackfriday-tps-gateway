FROM flink:1.18.0-scala_2.12

RUN apt-get update && \
    apt-get install -y python3 python3-pip curl && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install apache-flink==1.18.0 psycopg2-binary && \
    curl -o /opt/flink/lib/flink-connector-kafka-3.2.0-1.18.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.18/flink-connector-kafka-3.2.0-1.18.jar && \
    curl -o /opt/flink/lib/kafka-clients-3.7.0.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


