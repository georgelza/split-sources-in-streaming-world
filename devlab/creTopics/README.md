# Some Topic Notes

Simple shell script to create 2 topics based Avro..

avro_salesbaskets
avro_salespayments

Make sure to populate match the "COMPOSE_PROJECT_NAME" with the value used in the .env driving the docker compose project names.

## For Schema Registry

I used the 2 below *.sh and *.avsc (Avro Schema files) to create the schema entries for the Avro source topics.

Not for the Version 2.0 of this blog we added created_at column to the avro_salespayments table. This comes from our source Mysql and Postgresql tables. 

NOTE: At the moment I don't execute the reg_salespayments.sh script, other words we dont' create a schema, we're allowing the Kafka and flink Combination to auto create this.