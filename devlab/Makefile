
.DEFAULT_GOAL := help
include .env

define HELP

Available commands:

- build: Build all the images from the downloaded infrastructure components. After this execute "make run", followed by "make deploy" and then "make app".

- run: This will startup the core infrastructure of the project first
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
#	cd connect; make build
	cd flink; make build;
	cd sql-client; make build
#	cd ../app; make build	



# Run the core, followed by deploy and then app
run: core

core:
	docker compose -p devlab up -d broker schema-registry control-center connect \
		ksqldb-server ksqldb-cli kcat \
		flink-sql-client mongodb postgres paradedb hive-metastore \
		pyiceberg duckdb mysqlcdc postgrescdc 

deploy: createcat createtopics 
#deploycdcflink

createcat:
	docker compose exec flink-sql-client sql-client.sh -f /sql/creCat.sql

createtopics:
	cd creTopics; ./creTopics.sh

deploycdcflink:
	docker compose exec flink-sql-client sql-client.sh -f /sql/creCdc.sql

deployflink:
	docker compose exec flink-sql-client sql-client.sh -f /sql/creFlink.sql

run_app:
	cd ../app; ./runs_avro.sh
#	docker compose -p devlab up -d app

rdb:
	docker compose down mysqlcdc mongodb postgres postgrescdc paradedb minio
	cd data; cd minio; rm -rf * ;rm -rf .minio.sys ;rm -rf .DS*
	cd data; cd postgres; rm -rf * ;rm -rf .DS*
	cd data; cd paradedb; rm -rf * ;rm -rf .DS*
	cd data; cd postgrescdc; rm -rf * ;rm -rf .DS*
	cd data; cd mysqlcdc; rm -rf * ;rm -rf .DS*
	cd data; cd mongodb; rm -rf * ;rm -rf .DS*
	docker compose -p devlab up mysqlcdc mongodb postgres postgrescdc paradedb minio -d

stop_app:
	docker compose -p devlab stop app

stop:
	docker compose stop

down:	
	docker compose down
	cd data; cd minio; rm -rf * ;rm -rf .minio.sys ;rm -rf .DS*
	cd data; cd postgres; rm -rf * ;rm -rf .DS*
	cd data; cd paradedb; rm -rf * ;rm -rf .DS*
	cd data; cd postgrescdc; rm -rf * ;rm -rf .DS*
	cd data; cd mysqlcdc; rm -rf * ;rm -rf .DS*
	cd data; cd mongodb; rm -rf * ;rm -rf .DS*

ps:
	docker compose ps

logs:
	docker compose logs

monitor_app:
	docker compose logs -f app |grep Record

watch:
	watch docker compose ps

ksql:
	docker compose exec ksqldb-cli ksql http://ksqldb-server:8088

fsql:
	docker compose exec flink-sql-client sql-client.sh

start_pdb:
	docker compose -p devlab up -d paradedb
stop_pdb:
	docker compose -p devlab stop paradedb
monitor_pdb:
	docker compose logs -f paradedb