# platform

Local Docker Compose stack for developing stream-patterns against Kafka and Flink. **CI does not use Docker**; tests run on Flink `MiniCluster` via Maven.

## Images

| Service | Image |
|---------|--------|
| Kafka (KRaft) | `apache/kafka:4.3.1` |
| Flink jobmanager + 2 taskmanagers | `apache/flink:2.2.1-java17` |

Apicurio, Postgres, and Debezium Connect are planned for a later CDC/schema pattern week.

## Commands

From the repository root (requires Docker):

```bash
make up    # start stack and wait for healthchecks
make down  # stop and remove containers, networks, and volumes
make run   # smoke job (see root README; MiniCluster fallback)
```

After `make up`:

- Kafka bootstrap (from the host): `localhost:9092`
- Flink Web UI: http://localhost:8081
