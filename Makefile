include .env
export

CI_REGISTRY_IMAGE ?= airflow/default
TAG ?= latest
CONTAINER_IMAGE := $(CI_REGISTRY_IMAGE):$(TAG)

.PHONY: env airflow-image airflow airflow-config

env:
	cp deploy/.env.example deploy/.env

airflow-image: env
	docker build --file deploy/images/Dockerfile -t $(CONTAINER_IMAGE) .
	sed -i 's|^AIRFLOW_IMAGE_NAME=.*|AIRFLOW_IMAGE_NAME="$(CONTAINER_IMAGE)"|' deploy/.env

airflow: airflow-image
	docker compose -f deploy/docker-compose.yaml up --build -d
