#!/bin/bash

# Register Schema's on local topics
schema=$(cat ./schema_salesbaskets.avsc | sed 's/\"/\\\"/g' | tr -d "\n\r")
SCHEMA="{\"schema\": \"$schema\", \"schemaType\": \"AVRO\"}"
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "$SCHEMA" \
  http://localhost:9081/subjects/avro_salesbaskets-value/versions
#  http://mbp.local:9081/subjects/avro_salesbaskets-value/versions
