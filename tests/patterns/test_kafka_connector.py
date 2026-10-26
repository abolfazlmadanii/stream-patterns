from pathlib import Path
from unittest.mock import MagicMock

import pytest

from streampatterns.patterns.late_data.kafka_connector import (
    KAFKA_CONNECTOR_JAR_ENV,
    KAFKA_CONNECTOR_VERSION,
    apply_kafka_connector_jar,
    resolve_kafka_connector_jar,
)


def test_resolve_kafka_connector_jar_uses_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(KAFKA_CONNECTOR_JAR_ENV, "/tmp/custom-connector.jar")
    assert resolve_kafka_connector_jar() == Path("/tmp/custom-connector.jar")


def test_apply_kafka_connector_jar_registers_file_uri(tmp_path: Path) -> None:
    jar = tmp_path / f"flink-connector-kafka-{KAFKA_CONNECTOR_VERSION}.jar"
    jar.write_bytes(b"PK")
    env = MagicMock()
    uri = apply_kafka_connector_jar(env, jar)
    assert uri == jar.resolve().as_uri()
    env.add_jars.assert_called_once_with(uri)


def test_apply_kafka_connector_jar_missing_file(tmp_path: Path) -> None:
    env = MagicMock()
    with pytest.raises(FileNotFoundError):
        apply_kafka_connector_jar(env, tmp_path / "missing.jar")
