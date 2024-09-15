.DEFAULT_GOAL := help

REPO_NAME=georgelza
IMAGE_NAME=apacheflink
VERSION=1.18.1-scala_2.12-java11-c

define HELP

Available commands:

- build: Build this project

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help

build:
	sudo docker build --build-arg REPO_NAME=${REPO_NAME} -t ${REPO_NAME}/${IMAGE_NAME}:${VERSION} .

