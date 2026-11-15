# Requires: make (Git Bash) + Docker Desktop
# On Windows without make: run the commands directly from Git Bash.

PYTHON := .venv/Scripts/python.exe

.PHONY: up down topics logs gen gen-load init stream reset batch trigger airflow-build airflow-init dbt-run dbt-test dbt-docs

## Create required data lake and Airflow directories (run once)
init:
	mkdir -p data/lake/bronze/transactions data/lake/bronze/fraud_alerts
	mkdir -p data/lake/silver/transactions data/lake/quarantine/transactions
	mkdir -p data/checkpoints
	mkdir -p airflow/logs
	@echo "Data directories created."

## Start all services (Kafka + Spark). Run 'make init' first if data/ does not exist.
up:
	docker compose up -d
	@echo "Kafka UI: http://localhost:8090"

## Stop all containers (data volume is kept)
down:
	docker compose down

## List existing topics
topics:
	docker compose exec kafka kafka-topics.sh \
		--bootstrap-server kafka:29092 --describe

## Tail Kafka container logs
logs:
	docker compose logs -f kafka

## Run the generator at the rate configured in .env
gen:
	cd generator && ../$(PYTHON) producer.py

## Quick load test: 500 events/sec, 5000 messages, no seed lock
gen-load:
	cd generator && EVENTS_PER_SECOND=500 MAX_MESSAGES=5000 SEED=0 ../$(PYTHON) producer.py

## Submit the Spark streaming job (runs continuously — Ctrl+C to stop)
## First run downloads the Kafka connector JAR (~10 MB, cached in spark-packages volume)
stream:
	docker compose exec spark-master /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
	  --py-files /opt/spark-jobs/schemas.py \
	  /opt/spark-jobs/stream_ingest.py

## Run the bronze→silver batch job for a specific date (D=YYYY-MM-DD required)
## Example: make batch D=2026-11-09
batch:
	docker compose exec spark-master /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  --packages org.postgresql:postgresql:42.7.3 \
	  --py-files /opt/spark-jobs/schemas.py \
	  /opt/spark-jobs/bronze_to_silver.py \
	  --ingest-date $(D)

## Trigger the daily pipeline manually for a specific date (D=YYYY-MM-DD required)
## Example: make trigger D=2026-11-09
trigger:
	docker compose exec airflow-scheduler airflow dags trigger mfs_daily_pipeline \
	  --logical-date $(D)

## Run dbt models (staging views + gold tables)
dbt-run:
	MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler \
	  /home/airflow/.local/bin/dbt run --project-dir /opt/dbt --profiles-dir /opt/dbt --no-use-colors

## Run dbt schema tests
dbt-test:
	MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler \
	  /home/airflow/.local/bin/dbt test --project-dir /opt/dbt --profiles-dir /opt/dbt --no-use-colors

## Generate dbt docs (then view at dbt/target/index.html)
dbt-docs:
	MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler \
	  /home/airflow/.local/bin/dbt docs generate --project-dir /opt/dbt --profiles-dir /opt/dbt --no-use-colors

## Build the custom Airflow image for all services (run after first clone or Dockerfile changes)
airflow-build:
	docker compose build airflow-apiserver airflow-scheduler airflow-triggerer airflow-dag-processor

## Full reset: stop containers, wipe Kafka volume, checkpoints, and bronze files (destructive)
## Use after manually clearing Kafka so checkpoints and offsets stay in sync.
reset:
	@read -p "Delete Kafka data, checkpoints, and bronze files? (yes/no): " ans && [ "$$ans" = "yes" ]
	docker compose down -v
	rm -rf data/checkpoints/*
	rm -rf data/lake/bronze/transactions/*
	rm -rf data/lake/bronze/fraud_alerts/*
	rm -rf data/lake/silver/transactions/*
	rm -rf data/lake/quarantine/transactions/*
	@echo "Reset complete. Run 'make init && make up' to start fresh."
