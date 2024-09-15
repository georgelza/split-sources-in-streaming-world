
-- Source topics is Avro serialized.
-- Flink UI : http://localhost:9081/#/overview

-- The below injest the avro_salescompleted data from the kSql created stream as a output of a join from the 2 source kSql tables, the join results are inserted into avro_salescompleted_x.
-- salesbaskets_x and salespayments is build as virtual tables from the original topics (salesbaskets and salespayments)
-- join key is invoiceNumber.

-- After this we do a simple aggregate on sales per store per terminal per 5min and per hour (these values are at the root of the avro_salesbaskets table).

-- First Create a Catalog using our defined hms and backing S3.

-- -- AS TO_TIMESTAMP(FROM_UNIXTIME(CAST(SALETIMESTAMP_EPOC AS BIGINT) / 1000)),

-- The below builds a flink table avro_salescompleted, backed/sourced from the Kafka topic/kSql created table using output from out stream 
-- processing/query created in crekSqlFlows/creStreams.sql

-- INTERESTING, things written to the c_hive catalog is only recorded as existing in the hive catalog, but not persisted to Minio/S3... The persistence in this case
-- comes from salescompleted writing out to Kafka. 

CREATE OR REPLACE TABLE c_hive.db01.t_k_avro_salescompleted (
    INVNUMBER STRING,
    SALEDATETIME_LTZ STRING,
    SALETIMESTAMP_EPOC STRING,
    TERMINALPOINT STRING,
    NETT DOUBLE,
    VAT DOUBLE,
    TOTAL DOUBLE,
    STORE row<ID STRING, NAME STRING>,
    CLERK row<ID STRING, NAME STRING, SURNAME STRING>,
    BASKETITEMS array<row<ID STRING, NAME STRING, BRAND STRING, CATEGORY STRING, PRICE DOUBLE, QUANTITY INT>>,
    FINTRANSACTIONID STRING,
    PAYDATETIME_LTZ STRING,
    PAYTIMESTAMP_EPOC STRING,
    PAID DOUBLE,
    SALESTIMESTAMP_WM as TO_TIMESTAMP(FROM_UNIXTIME(CAST(`SALETIMESTAMP_EPOC` AS BIGINT) / 1000)),
    WATERMARK FOR SALESTIMESTAMP_WM AS SALESTIMESTAMP_WM
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'avro_salescompleted',
    'properties.bootstrap.servers'  = 'broker:29092',
    'scan.startup.mode'             = 'earliest-offset',
    'properties.group.id'           = 'testGroup',
    'value.format'                  = 'avro-confluent',
    'value.avro-confluent.schema-registry.url' = 'http://schema-registry:9081',
    'value.fields-include'          = 'ALL'
);

-- NEW OUTPUT Tables/Aggregations.
--
-- https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/window-agg/
-- We going to output the group by into this table, backed by topic which we will sink to MongoDB via connector

CREATE TABLE c_hive.db01.t_f_avro_sales_per_store_per_terminal_per_5min (
    store_id STRING,
    terminalpoint STRING,
    window_start  TIMESTAMP(3),
    window_end TIMESTAMP(3),
    salesperterminal BIGINT,
    totalperterminal DOUBLE,
    PRIMARY KEY (store_id, terminalpoint, window_start, window_end) NOT ENFORCED
) WITH (
    'connector'                     = 'upsert-kafka',
    'topic'                         = 'avro_sales_per_store_per_terminal_per_5min',
    'properties.bootstrap.servers'  = 'broker:29092',
    'key.format'                    = 'avro-confluent',
    'key.avro-confluent.url'        = 'http://schema-registry:9081',
    'value.format'                  = 'avro-confluent',
    'value.avro-confluent.url'      = 'http://schema-registry:9081',
    'value.fields-include'          = 'ALL'
);

CREATE TABLE c_hive.db01.t_f_avro_sales_per_store_per_terminal_per_hour (
    store_id STRING,
    terminalpoint STRING,
    window_start  TIMESTAMP(3),
    window_end TIMESTAMP(3),
    salesperterminal BIGINT,
    totalperterminal DOUBLE,
    PRIMARY KEY (store_id, terminalpoint, window_start, window_end) NOT ENFORCED
) WITH (
    'connector'                     = 'upsert-kafka',
    'topic'                         = 'avro_sales_per_store_per_terminal_per_hour',
    'properties.bootstrap.servers'  = 'broker:29092',
    'key.format'                    = 'avro-confluent',
    'key.avro-confluent.url'        = 'http://schema-registry:9081',
    'value.format'                  = 'avro-confluent',
    'value.avro-confluent.url'      = 'http://schema-registry:9081',
    'value.fields-include'          = 'ALL'
);


SET 'pipeline.name' = 'Sales per store per terminal per 5min - Output to Kafka kTable/topic';

Insert into c_hive.db01.t_f_avro_sales_per_store_per_terminal_per_5min
SELECT 
    `STORE`.`ID` as STORE_ID,
    TERMINALPOINT,
    window_start,
    window_end,
    COUNT(*) as salesperterminal,
    SUM(TOTAL) as totalperterminal
  FROM TABLE(
    TUMBLE(TABLE c_hive.db01.t_k_avro_salescompleted, DESCRIPTOR(SALESTIMESTAMP_WM), INTERVAL '5' MINUTES))
  GROUP BY `STORE`.`ID`, TERMINALPOINT, window_start, window_end; 


SET 'pipeline.name' = 'Sales per store per terminal per hour - Output to Kafka kTable/topic';

Insert into c_hive.db01.t_f_avro_sales_per_store_per_terminal_per_hour
SELECT 
    `STORE`.`ID` as STORE_ID,
    TERMINALPOINT,
    window_start,
    window_end,
    COUNT(*) as salesperterminal,
    SUM(TOTAL) as totalperterminal
  FROM TABLE(
    TUMBLE(TABLE c_hive.db01.t_k_avro_salescompleted, DESCRIPTOR(SALESTIMESTAMP_WM), INTERVAL '1' HOUR))
  GROUP BY `STORE`.`ID`, TERMINALPOINT, window_start, window_end;