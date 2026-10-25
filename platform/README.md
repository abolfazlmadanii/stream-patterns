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
```

After `up`: Kafka `localhost:9092`, Flink UI http://localhost:8081.
