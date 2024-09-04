
.DEFAULT_GOAL := help
include ../.env

IMAGE_NAME=apacheflink
VERSION=1.18.1-scala_2.12-java11-d

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

