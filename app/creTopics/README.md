# Some Topic Notes

Simple shell script to create 2 topics based Avro..

avro_salesbaskets
avro_salespayments

Make sure to populate match the "COMPOSE_PROJECT_NAME" with the value used in the .env driving the docker compose project names.

## For Schema Registry

I used the 2 below *.sh and *.avsc (Avro Schema files) to create the schema entries for the Avro source topics.