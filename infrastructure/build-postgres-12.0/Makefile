.DEFAULT_GOAL := help
include ../.env

IMAGE_NAME=postgres-12.0
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
	sudo docker build -t ${REPO_NAME}/${IMAGE_NAME}:${VERSION} .

