COMPOSE_FILE := docker-compose.yml
COMPOSE := docker compose -f $(COMPOSE_FILE)
KAFKA_CONNECTOR_VERSION := 5.0.0-2.2
LIB_DIR := ../lib
KAFKA_CONNECTOR_JAR := $(LIB_DIR)/flink-connector-kafka-$(KAFKA_CONNECTOR_VERSION).jar
MAVEN_CENTRAL := https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/$(KAFKA_CONNECTOR_VERSION)

.PHONY: up down test download-kafka-connector

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down -v --remove-orphans

test:
	cd .. && uv run pytest

download-kafka-connector:
	mkdir -p $(LIB_DIR)
	curl -fL -o $(KAFKA_CONNECTOR_JAR) $(MAVEN_CENTRAL)/flink-connector-kafka-$(KAFKA_CONNECTOR_VERSION).jar
