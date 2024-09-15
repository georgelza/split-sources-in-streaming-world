
.DEFAULT_GOAL := help

define HELP

Available commands:

- build: Build all the images from the downloaded infrastructure components.

- core: This will startup the core infrastructure components

- run: This will startup the entire docker compose project 

- deploy: This will create all the catalogs, tables, flink inserts etc.

- run_app: This will run the Golang app, the load generator. For now it's a direct Golang run. will be replaced as a container.

- stop: Stop the project

- down: Tear down the project, clean directories

- ps: Show all running containers

- logs: Show logs

- watch: Watch logs

- ksql: Open KSql

- fsql: Open Flink Sql

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help

build:
	cd connect; make build 
	cd flink; make build;
	cd sql-client; make build
	cd hdfs; make buildbase
	cd hdfs; make build

core: 
	docker compose -p devlab up -d broker schema-registry control-center connect \
		ksqldb-server ksqldb-cli kcat \
		hive-metastore flink-sql-client \
		namenode resourcemanager historyserver nodemanager1 datanode1 datanode2 datanode3 datanode4 datanode5

run:
	docker compose -p devlab up -d

deploy: createcat createtopics deployksql deployflink1 deployflink2

createcat:
	docker compose exec flink-sql-client sql-client.sh -f /sql/creCat.sql
createtopics:
	cd ../creTopics; ./creTopics.sh
deployksql:
	docker compose exec ksqldb-cli ksql -f /sql/creStreams.sql  http://ksqldb-server:8088
deployflink1:
	docker compose exec flink-sql-client sql-client.sh -f /sql/creFlink_1.sql
deployflink2:
	docker compose exec flink-sql-client sql-client.sh -f /sql/creFlink_2.sql

# to be fixed still. App runs but for some reason even though it says it's posting payload need to figure out where to.
run_app:
	cd ../app; ./runs_avro.sh
#	docker compose -p devlab up -d app

# well this sort of requires the app to run inside the container, which is broken atm, so we running it directly above still.
stop_app:
	docker compose -p devlab stop app

stop:
	docker compose stop

down:	
	docker compose down
	cd data; cd hdfs; rm -rf * ;rm -rf .DS*
	cd data; cd minio; rm -rf * ;rm -rf .minio.sys ;rm -rf .DS*

ps:
	docker compose ps

logs:
	docker compose logs

# if run va container.
monitor_app:
	docker compose logs -f app |grep Record

watch:
	watch docker compose ps

ksql:
	docker compose exec ksqldb-cli ksql http://ksqldb-server:8088

fsql:
	docker compose exec flink-sql-client sql-client.sh