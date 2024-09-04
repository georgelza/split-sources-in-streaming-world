
## The Todo

### Primary data flow:

1. source (INPUT) the Kafka topic: avro_salesbaskets into Flink into t_k_avro_salesbaskets
2. source (INPUT) the inserted records from either MySqlDB or PostgreSQL (DB:sales, Table:salespayments) via CDC into Flink 
3. into t_f_msqlcdc_salespayments or t_f_pgcdc_salespayments
3. push either of them into flink table: t_f_salespayments, itself configured to export to Kafka Topic: avro_salespayments
4. join t_k_avro_salesbaskets and t_f_salespayments to create t_f_avro_salescompleted
5. push t_f_avro_salescompleted into Kafka topic: avro_salescompleted
6. sink (OUTPUT) t_f_avro_salescompleted into Apache Paimon table: parquet_salescompleted
7. Unnest t_f_avro_salescompleted basketitems array to t_f_unnested_sales
8. sink (OUTPUT) t_f_unnested_sales into Apache Paimon table: parquet_unnested_sales
9. push t_f_unnested_sales into Kafka topic: avro_unnested_sales

### Primary data flow:

1. Do some more aggregations,
2. Push them to Kakfa Topics
3. Push then to Apache Paimon tables