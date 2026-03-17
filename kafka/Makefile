# Simple make targets for local Kafka + Go Sarama examples

DOCKER_COMPOSE_FILE=../cluster-docker-compose-kafka.yml

.PHONY: up down produce consume

up:
	docker compose -f $(DOCKER_COMPOSE_FILE) up -d

down:
	docker compose -f $(DOCKER_COMPOSE_FILE) down

produce:
	cd $(shell pwd) && go run ./producer

consume:
	cd $(shell pwd) && go run ./consumergroup

# Optional: run both for quick local test (consumer in background)
run:
	make up
	# เปิด terminal ใหม่ หรือรัน produce ใน terminal แยก
	# ใน terminal 1: make consume
	# ใน terminal 2: make produce
