.DEFAULT_GOAL := help
include ../.env

IMAGE_NAME=ubuntu20.04-openjdk11
VERSION=1.0.0

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
