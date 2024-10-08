.DEFAULT_GOAL := help

define HELP

Available commands:

- pullall: Pull all source images.
- getall: wget all the *.gz files, to be copied into ./stage directories 
- buildall: Build all images.
- clean: Remove all *.gz files downloaded

- os: Docker Pull all source OS images.

- confluent: Pull all the Confluent Kafka source images.

- flink: Pull all the Apache Flink base image.

- db: Pull all our database images.

- S3: Pull our MinIO and MC console image.

- misc: Pull the additional images, acting as glue etc.

- iop: Pull our IOP app images (Prometheus,Pushgateway, Grafana, ...)


endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help

pullall: os confluent flink db s3 misc hadoop hive

buildall: buildos buildflinkbase builddb buildopenjdk8 buildhive

# Base images used along the way, used to build 2nd level images.
# arm64/v8 used as base for all images except the app/*.
# ubuntu:jammy & golang:1.22 (see misc:) are used when building the app/* into a docker image
os: 
	docker pull arm64v8/ubuntu:20.04
	docker pull ubuntu:jammy

confluent:
	docker pull confluentinc/cp-kafka:7.6.1
	docker pull confluentinc/cp-kafka-connect-base:7.6.1
	docker pull confluentinc/cp-schema-registry:7.6.1
	docker pull confluentinc/cp-ksqldb-server:7.6.1
	docker pull confluentinc/cp-ksqldb-cli:7.6.1
	docker pull confluentinc/cp-enterprise-control-center:7.6.1
	docker pull confluentinc/cp-kcat:7.6.1

flink:
	docker pull arm64v8/flink:1.18.1-scala_2.12-java11
#	docker pull flink:1.19.1-scala_2.12-java11
#	docker pull flink:1.20.0-scala_2.12-java11

db:
	docker pull postgres:12	
	docker pull mongodb/mongodb-atlas-local:7.0.5
	docker pull mysql:8.0
	docker pull paradedb/paradedb:latest

s3: 
	docker pull minio/minio:latest
	docker pull minio/mc:latest

misc:
	docker pull golang:1.22
# 	Used in hms-postgres
	docker pull naushadh/hive-metastore:latest

# 	Used for inspecting data in the various back end stores.
	docker pull python:3.12-bookworm
	docker pull codieplusplus/duckdb

hadoop:
	cd build-hive-openjdk8/basehadoop; mkdir stage; cd stage; wget https://downloads.apache.org/hadoop/common/hadoop-3.3.5/hadoop-3.3.5-aarch64.tar.gz

hive:
	cd build-hive-openjdk8/basehive; mkdir stage; cd  stage; wget https://downloads.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
	cd build-hive-openjdk8/basehive; cd stage; wget https://downloads.apache.org/hadoop/common/hadoop-3.3.5/hadoop-3.3.5-aarch64.tar.gz
	cd build-hive-openjdk8/basehive; cd stage; wget http://www.congiu.net/hive-json-serde/1.3.8/cdh5/json-serde-1.3.8-jar-with-dependencies.jar 
	cd build-hive-openjdk8/basehive; cd stage; wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar


# Build images, not 100% all of whats needed, but these are all common images.
buildos:
	cd ./build-ubuntu-os-20.04; make build

buildflinkbase:
	cd build-flink-base; make build

builddb:
	cd build-postgres-12.0; make build

buildopenjdk8:
	cd build-ubuntu-os-openjdk8; make build

buildhive:
	cd build-hive-openjdk8; make build