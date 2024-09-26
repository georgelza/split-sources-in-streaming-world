
.DEFAULT_GOAL := help
include .env

define HELP

Available commands:

- build: Build all the images from the downloaded infrastructure components. After this execute "make run", followed by "make deploy" and then "make app".

- run: This will call core.
- deploy: This will create the Kafka topics, schema entries, all the Hive catalogs, Flink tables, flink inserts jobs etc.
- run_app: Run workload creator/the Golang app.
- stop_app: Stop the workload creator/the Golang app.

- stop: Stop the project.
- down: Tear down the project, clean directories.
- ps: Show all running containers.

- logs: Show logs.
- monitor_app: show the logs of the workload creator/the Golang app.

- watch: Watch logs.
- ksql: Open KSql.
- fsql: Open Flink Sql.

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help


build: 
	cd connect; make build
	cd flink; make build;
	cd sql-client; make build

run:
	docker compose -p devlab up -d broker schema-registry control-center connect \
		ksqldb-server ksqldb-cli \
		minio mc \
		flink-jobmanager flink-taskmanager flink-sql-client \
		postgres metaserver hiveserver2 \
		mysqlcdc postgrescdc mongodb
# pyiceberg duckdb kcat

deploy: createtopics createcat deploycdcflink deployflink

createtopics:
	cd creTopics; ./creTopics.sh
	
createcat:
	docker compose exec flink-sql-client sql-client.sh -f /sql/creCat.sql

deploycdcflink:
	docker compose exec flink-sql-client sql-client.sh -f /sql/creCdc.sql

deployflink:
	docker compose exec flink-sql-client sql-client.sh -f /sql/creFlink.sql

run_app:
	cd ../app; ./runs_avro.sh

stop:
	docker compose stop

start:
	docker compose start

down:	
	docker compose down
	cd data; rm -rf minio 
	cd data; rm -rf mongodb 
	cd data; rm -rf postgres
	cd data; rm -rf postgrescdc
	cd data; rm -rf mysqlcdc
	cd data; rm -rf mongodb
	cd data; rm -rf warehouse

ps:
	docker compose ps

logs:
	docker compose logs

logsf:
	docker compose logs -f

watch:
	watch docker compose ps

ksql:
	docker compose exec ksqldb-cli ksql http://ksqldb-server:8088

fsql:
	docker compose exec flink-sql-client sql-client.sh
