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

pullall: os confluent flink db s3 misc #iop

getall: 
	cd build-hadoop-openjdk11-hdfs; make wget;
	cd build-hive-openjdk8; make wget

buildall: buildos buildopenjdk buildhadoop buildhive builddb # buildapp

clean:
	cd build-hadoop-openjdk11-hdfs; make clean
	cd build-hive-openjdk8; make clean

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

db:
	docker pull postgres:12	
	docker pull mongodb/mongodb-atlas-local:7.0.5
	docker pull mysql:9.0
	docker pull paradedb/paradedb:latest

s3: 
	docker pull minio/minio:latest
	docker pull minio/mc:latest

iop:	
	docker pull prom/prometheus:latest
	docker pull prom/pushgateway:latest
	docker pull grafana/grafana:latest

misc:
	docker pull golang:1.22
# 	Used in hms-postgres
	docker pull naushadh/hive-metastore:latest

# 	Used for inspecting data in the various back end stores.
	docker pull python:3.12-bookworm
	docker pull codieplusplus/duckdb

buildall: buildos buildopenjdk buildhadoop buildhive builddb buildflinkbase 

# Build images, not 100% all of whats needed, but these are all common images.
buildos:
	cd ./build-ubuntu-os-20.04; make build

buildopenjdk:
	cd build-ubuntu-os-openjdk8; pwd; make build
	cd build-ubuntu-os-openjdk11; pwd; make build

buildhadoop:
	cd build-hadoop-openjdk11-hdfs; make buildbase
	cd build-hadoop-openjdk11-hdfs; make build

buildhive:
	cd build-hive-openjdk8;  make basehadoop
	cd build-hive-openjdk8;  make basehive
	cd build-hive-openjdk8;  make build

builddb:
	cd build-postgres-12.0; make build

builddb:
	cd build-flink-base; make build


# Used if desired to create fake JSON based data
# docker pull shadowtraffic/shadowtraffic:0.6.0