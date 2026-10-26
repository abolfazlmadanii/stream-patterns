"""Optional Kafka source/sink wiring for the late-data pattern."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from streampatterns.patterns_common.codec import decode_event

if TYPE_CHECKING:
    from pyflink.datastream import DataStream, StreamExecutionEnvironment

KAFKA_CONNECTOR_VERSION = "5.0.0-2.2"
KAFKA_CONNECTOR_JAR_NAME = f"flink-connector-kafka-{KAFKA_CONNECTOR_VERSION}.jar"
KAFKA_CONNECTOR_JAR_ENV = "STREAM_PATTERNS_KAFKA_CONNECTOR_JAR"
DEFAULT_KAFKA_CONNECTOR_JAR = Path("lib") / KAFKA_CONNECTOR_JAR_NAME


@dataclass(frozen=True, slots=True)
class LateDataKafkaConfig:
    """Kafka topics and broker settings for the late-data pattern."""

    bootstrap_servers: str
    group_id: str
    input_topic: str
    on_time_topic: str
    late_topic: str


def resolve_kafka_connector_jar() -> Path:
    """Return the connector jar path from the environment or the default under ``lib/``."""
    override = os.environ.get(KAFKA_CONNECTOR_JAR_ENV)
    if override:
        return Path(override)
    return DEFAULT_KAFKA_CONNECTOR_JAR


def apply_kafka_connector_jar(
    env: StreamExecutionEnvironment,
    jar_path: Path | None = None,
) -> str:
    """Register the Flink Kafka connector jar on the execution environment."""
    path = (jar_path or resolve_kafka_connector_jar()).expanduser()
    if not path.is_file():
        raise FileNotFoundError(
            f"Kafka connector jar not found at {path}. "
            f"Set {KAFKA_CONNECTOR_JAR_ENV} or run "
            "`make -f platform/Makefile download-kafka-connector`."
        )
    jar_uri = path.resolve().as_uri()
    env.add_jars(jar_uri)
    return jar_uri


def kafka_event_source(config: LateDataKafkaConfig):
    """Build a Kafka source of struct-encoded stream events."""
    from pyflink.common.serialization import ByteArraySchema
    from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

    return (
        KafkaSource.builder()
        .set_bootstrap_servers(config.bootstrap_servers)
        .set_group_id(config.group_id)
        .set_topics(config.input_topic)
        .set_value_only_deserializer(ByteArraySchema())
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .build()
    )


def decode_kafka_payload(payload: bytes) -> object:
    return decode_event(payload)


def attach_late_data_kafka_sinks(
    on_time_stream: DataStream,
    late_stream: DataStream,
    config: LateDataKafkaConfig,
) -> None:
    """Write on-time and late event ids to their Kafka topics."""
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.base import DeliveryGuarantee
    from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaSink

    def _string_sink(topic: str) -> KafkaSink:
        serializer = (
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        return (
            KafkaSink.builder()
            .set_bootstrap_servers(config.bootstrap_servers)
            .set_record_serializer(serializer)
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build()
        )

    on_time_stream.sink_to(_string_sink(config.on_time_topic))
    late_stream.sink_to(_string_sink(config.late_topic))
