
# Data Flows

1. creCat create the 2 cataloges, c_hive and c_paimon

2. creCdc.sql create the source CDC links pointing to MySqlDB and/or PostgreSQLDB

## Primary data flow:

- creFlink.sql

1. source (INPUT) the Kafka topic: avro_salesbaskets into Flink into c_hive.db01.t_k_avro_salesbaskets
2. source (INPUT) the inserted records from either MySqlDB or PostgreSQL (DB:sales, Table:salespayments) via CDC into Flink c_hive.db01.t_f_msqlcdc_salespayments or c_hive.db01.t_f_pgcdc_salespayments
3. push either of them into flink table: c_hive.db01.t_f_salespayments, itself configured to export to Kafka Topic: avro_salespayments
4. join c_hive.db01.t_k_avro_salesbaskets and c_hive.db01.t_f_salespayments to create c_hive.db01.t_f_avro_salescompleted
5. push c_hive.db01.t_f_avro_salescompleted into Kafka topic: avro_salescompleted
6. sink (OUTPUT) c_hive.db01.t_f_avro_salescompleted into Apache Paimon table: c_paimon.dev.t_salespayments
7. Unnest c_hive.db01.t_f_avro_salescompleted basketitems array to c_hive.db01.t_f_unnested_sales
8. sink (OUTPUT) c_hive.db01.t_unnested_sales into Apache Paimon table: c_paimon.dev.t_unnested_sales
9. push c_hive.db01.t_unnested_sales into Kafka topic: avro_unnested_sales
