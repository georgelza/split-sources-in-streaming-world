.DEFAULT_GOAL := help

REPO_NAME=georgelza
IMAGE_NAME=apacheflink_sqlpod
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
	sudo docker build -t ${REPO_NAME}/${IMAGE_NAME}:${VERSION} .

