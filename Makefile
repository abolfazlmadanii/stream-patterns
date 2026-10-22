COMPOSE_FILE := platform/docker-compose.yml
COMPOSE := docker compose -f $(COMPOSE_FILE)

.PHONY: up down run

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down -v --remove-orphans

# Smoke job: MiniCluster via Maven (no Docker). Use after `make up` when exercising the local stack manually.
run:
	mvn -B -pl testkit -am test -Dtest=TestkitSmokeJobTest
