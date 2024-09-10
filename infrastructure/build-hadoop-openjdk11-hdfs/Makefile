.DEFAULT_GOAL := help
include ../.env

VERSION=1.0.0
HADOOP_VERSION=3.3.5-java11

define HELP

Based on Ubuntu 20.04 / OpenJDK 11 / Apache Hadoop 3.3.8

Available commands:

- wget: Download Apache Hadoop 3.3.8 tar.gz source files.

- buildbase: Build the base image project

- build: Build this project

- run: Run the project

- stop: Stop the project

- down: Stop and remove the project

- logs: Show the logs

- ps: Show the status

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help

wget: clean
	cd base; mkdir -p stage; 
	cd base; cd stage; wget https://downloads.apache.org/hadoop/common/hadoop-3.3.5/hadoop-3.3.5-aarch64.tar.gz

clean:
	cd base; cd stage; rm *

buildbase: wget
	sudo docker build --build-arg REPO_NAME=${REPO_NAME} -t ${REPO_NAME}/hadoop-base-$(HADOOP_VERSION):$(VERSION) ./base

build: buildbase
	sudo docker build --build-arg REPO_NAME=${REPO_NAME} -t ${REPO_NAME}/hadoop-namenode-$(HADOOP_VERSION):$(VERSION) ./namenode
	sudo docker build --build-arg REPO_NAME=${REPO_NAME} -t ${REPO_NAME}/hadoop-datanode-$(HADOOP_VERSION):$(VERSION) ./datanode
	sudo docker build --build-arg REPO_NAME=${REPO_NAME} -t ${REPO_NAME}/hadoop-resourcemanager-$(HADOOP_VERSION):$(VERSION) ./resourcemanager
	sudo docker build --build-arg REPO_NAME=${REPO_NAME} -t ${REPO_NAME}/hadoop-nodemanager-$(HADOOP_VERSION):$(VERSION) ./nodemanager
	sudo docker build --build-arg REPO_NAME=${REPO_NAME} -t ${REPO_NAME}/hadoop-historyserver-$(HADOOP_VERSION):$(VERSION) ./historyserver

run:
	docker compose -p devlab up -d

stop:
	docker compose stop

down:
	docker compose down

logs:
	docker compose logs

ps:
	docker compose ps