# platform

Local Docker stack for Kafka and PyFlink. **CI does not use Docker.**

## Images

| Service | Image |
|---------|--------|
| Kafka (KRaft) | `apache/kafka:4.3.1` |
| Flink + PyFlink | `platform/Dockerfile` (`apache/flink:2.2.1-java17` + `apache-flink==2.2.1`) |

## Commands

From the repository root:

```bash
make -f platform/Makefile up
make -f platform/Makefile down
make -f platform/Makefile test
make -f platform/Makefile download-kafka-connector
```

For Kafka-backed late-data jobs, download the Flink Kafka connector `5.0.0-2.2` jar (or set `STREAM_PATTERNS_KAFKA_CONNECTOR_JAR` to your copy), then use `wire_late_data_from_kafka` / `wire_late_data_to_kafka`.

After `up`: Kafka `localhost:9092`, Flink UI http://localhost:8081.
