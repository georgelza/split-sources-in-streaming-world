
.DEFAULT_GOAL := help
include ../.env

IMAGE_NAME=apacheflink-base
VERSION=1.18.1-scala_2.12-java11

define HELP

Available commands:

- build: Build this project

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help

build:
	docker build -t ${REPO_NAME}/${IMAGE_NAME}:${VERSION} .

