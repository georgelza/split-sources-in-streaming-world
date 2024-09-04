#!/bin/bash

export COMPOSE_PROJECT_NAME=devlab

docker compose exec broker kafka-topics \
 --create -topic avro_salesbaskets \
 --bootstrap-server localhost:9092 \
 --partitions 1 \
 --replication-factor 1

docker compose exec broker kafka-topics \
 --create -topic avro_salespayments \
 --bootstrap-server localhost:9092 \
 --partitions 1 \
 --replication-factor 1

# Shadowtraffic test topics - Still in Development
# docker compose exec broker kafka-topics \
#  --create -topic salesbaskets-shadow \
#  --bootstrap-server localhost:9092 \
#  --partitions 1 \
#  --replication-factor 1

#  docker compose exec broker kafka-topics \
#  --create -topic salespayments-shadow \
#  --bootstrap-server localhost:9092 \
#  --partitions 1 \
#  --replication-factor 1


# Lets list topics, excluding the default Confluent Platform topics
docker compose exec broker kafka-topics \
 --bootstrap-server localhost:9092 \
 --list | grep -v '_confluent' |grep -v '__' |grep -v '_schemas' | grep -v 'default' | grep -v 'docker-connect'

 ./reg_salesbaskets.sh

 ./reg_salespayments.sh
