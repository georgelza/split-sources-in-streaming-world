### My little data generator...

Well guess have to admit she's not that little anymore.

As it stands she can push 2 payloads (salesbasket and salespayments) to Confluent Kakfa topics or the salesbasket to Kafka Topic and the salespayments to either MySqlDB or/and PostgreSQL.  avro serialized when pushed to Kafka Topic.

- Output records to a MongoDB.

- The rate of records can be controlled.

- The Amount of records can be controlled.

- The records produced can be stored to flat json files also, per run.

- Records can be randomely simulated form mutliple stores or per specific/specfied store.

- Source seed data can be defined.

- The number of potential items per basket can be controlled.

- The quantity of a item picked can be controlled.

- The number of possible terminals per store can be controlled.

