.DEFAULT_GOAL := help
include ../.env

HADOOP_VERSION=3.3.5-java11
VERSION=1.0.0

define HELP

Available commands:

- buildbase: Build the base image project

- build: Build this project

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help

buildbase:
	sudo docker build -t ${REPO_NAME}/hadoop-base-$(HADOOP_VERSION):$(VERSION) ./base

build:  
	sudo docker build -t ${REPO_NAME}/hadoop-namenode-$(HADOOP_VERSION):$(VERSION) ./namenode
	sudo docker build -t ${REPO_NAME}/hadoop-datanode-$(HADOOP_VERSION):$(VERSION) ./datanode
	sudo docker build -t ${REPO_NAME}/hadoop-resourcemanager-$(HADOOP_VERSION):$(VERSION) ./resourcemanager
	sudo docker build -t ${REPO_NAME}/hadoop-nodemanager-$(HADOOP_VERSION):$(VERSION) ./nodemanager
	sudo docker build -t ${REPO_NAME}/hadoop-historyserver-$(HADOOP_VERSION):$(VERSION) ./historyserver

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