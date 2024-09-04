-- Usage for Table/SQL API

-- NOTE: 
-- Case sentivity... need to match the case as per types/fs.go structs avro sections.

-- The Todo
--
-- source (INPUT) the Kafka topic: avro_salesbaskets into Flink into t_k_avro_salesbaskets
-- source (INPUT) the inserted records from either MySqlDB or PostgreSQL (DB:sales, Table:salespayments) via CDC into Flink 
-- into t_f_msqlcdc_salespayments or t_f_pgcdc_salespayments
-- push either of them into flink table: t_f_salespayments, itself configured to export to Kafka Topic: avro_salespayments
-- join t_k_avro_salesbaskets and t_f_salespayments to create t_f_avro_salescompleted
-- push t_f_avro_salescompleted into Kafka topic: avro_salescompleted
-- sink (OUTPUT) t_f_avro_salescompleted into Apache Paimon table: parquet_salescompleted
-- Unnest t_f_avro_salescompleted basketitems array to t_f_unnested_sales
-- sink (OUTPUT) t_f_unnested_sales into Apache Paimon table: parquet_unnested_sales
-- push t_f_unnested_sales into Kafka topic: avro_unnested_sales


-- Set checkpoint to happen every minute
SET 'execution.checkpointing.interval' = '5sec';

-- Set this so that the operators are separate in the Flink WebUI.
SET 'pipeline.operator-chaining.enabled' = 'false';

-- display mode
-- SET 'sql-client.execution.result-mode' = 'table';

SET 'execution.runtime-mode' = 'streaming';
-- SET 'execution.runtime-mode' = 'batch';

USE c_hive.db01;

-- Source topic is Avro serialized.
CREATE OR REPLACE TABLE c_hive.db01.t_k_avro_salesbaskets (
    `invoiceNumber`       STRING,
    `saleDateTime_Ltz`    STRING,
    `saleTimestamp_Epoc`  STRING,
    `terminalPoint`       STRING,
    `nett`                DOUBLE,
    `vat`                 DOUBLE,
    `total`               DOUBLE,
    `store`               row<`id` STRING, `name` STRING>,
    `clerk`               row<`id` STRING, `name` STRING, `surname` STRING>,
    `basketItems`         array<row<`id` STRING, `name` STRING, `brand` STRING, `category` STRING, `price` DOUBLE, `quantity` INT, `subtotal` DOUBLE>>,
    `saleTimestamp_WM`    as TO_TIMESTAMP(FROM_UNIXTIME(CAST(`saleTimestamp_Epoc` AS BIGINT) / 1000)),
    WATERMARK FOR `saleTimestamp_WM` AS `saleTimestamp_WM`
) WITH (
    'connector'                                 = 'kafka',
    'topic'                                     = 'avro_salesbaskets',
    'properties.bootstrap.servers'              = 'broker:29092',
    'properties.group.id'                       = 'testGroup',
    'scan.startup.mode'                         = 'earliest-offset',
    'value.format'                              = 'avro-confluent',
    'value.avro-confluent.schema-registry.url'  = 'http://schema-registry:9081',
    'value.fields-include'                      = 'ALL'
);

-- This will be a output table, allowing us to push the values sourced from mysql/cdc source data back to Kafka
CREATE OR REPLACE TABLE c_hive.db01.t_f_avro_salespayments (
    `invoiceNumber`     STRING,
    `payDateTime_Ltz`   STRING,
    `payTimestamp_Epoc` STRING,
    `paid`              DOUBLE,
    `finTransactionId`  STRING,
    `created_at`        TIMESTAMP(3),
    `payTimestamp_WM` AS TO_TIMESTAMP(FROM_UNIXTIME(CAST(`payTimestamp_Epoc` AS BIGINT) / 1000)),
    WATERMARK FOR `payTimestamp_WM` AS `payTimestamp_WM`,
    PRIMARY KEY (`invoiceNumber`) NOT ENFORCED
) WITH (
    'connector'                                = 'upsert-kafka'
    ,'topic'                                   = 'avro_salespayments'
    ,'properties.bootstrap.servers'            = 'broker:29092'
    ,'value.fields-include'                    = 'ALL'
    ,'value.format'                            = 'avro-confluent'
    ,'value.avro-confluent.schema-registry.url'= 'http://schema-registry:9081'
    ,'key.format'                              = 'avro-confluent'
    ,'key.avro-confluent.schema-registry.url'  = 'http://schema-registry:9081'
);

-- Source's created in creCdc.sql

-- MySqlDB
-- Insert into above as select from cdc sourced : t_f_msqlcdc_salespayments
INSERT INTO c_hive.db01.t_f_avro_salespayments (
    `invoiceNumber`,
    `payDateTime_Ltz`,
    `payTimestamp_Epoc`,
    `paid`,
    `finTransactionId`,
    `created_at`
  ) SELECT
    invoiceNumber,
    payDateTime_Ltz,  
    payTimestamp_Epoc,
    paid,
    finTransactionId,
    created_at
  FROM 
    c_hive.db01.t_f_msqlcdc_salespayments;

-- OR

-- PostgreSql
-- Insert into above as select from cdc sourced : t_f_pgcdc_salespayments
INSERT INTO c_hive.db01.t_f_avro_salespayments (
    `invoiceNumber`,
    `payDateTime_Ltz`,
    `payTimestamp_Epoc`,
    `paid`,
    `finTransactionId`,
    `created_at`
  ) SELECT
    invoicenumber,
    paydatetime_ltz,  
    paytimestamp_epoc,
    paid,
    fintransactionid,
    created_at
  FROM 
    c_hive.db01.t_f_pgcdc_salespayments;


-- original with connector: kafka
-- [ERROR] Could not execute SQL statement. Reason:
-- org.apache.flink.table.api.TableException: Table sink 'c_hive.db01.t_f_avro_salespayments' doesn't support consuming update and delete changes which is produced by node TableSourceScan(table=[[c_hive, db01, t_f_msqlcdc_salespayments]], fields=[invoiceNumber, payDateTime_Ltz, payTimestamp_Epoc, paid, finTransactionId, created_at])


-- [ERROR] Could not execute SQL statement. Reason:
-- org.apache.calcite.sql.validate.SqlValidatorException: Cannot apply 'TO_TIMESTAMP' to arguments of type 'TO_TIMESTAMP(<VARCHAR(2147483647)>, <INTEGER>)'. Supported form(s): 'TO_TIMESTAMP(<CHARACTER>)'
-- 'TO_TIMESTAMP(<CHARACTER>, <CHARACTER>)'


-- [ERROR] Could not execute SQL statement. Reason:
-- org.apache.flink.sql.parser.error.SqlValidateException: Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED controls if the constraint checks are performed on the incoming/outgoing data. Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode


-- [ERROR] Could not execute SQL statement. Reason:
-- org.apache.flink.table.api.ValidationException: 'upsert-kafka' tables require to define a PRIMARY KEY constraint. The PRIMARY KEY specifies which columns should be read from or write to the Kafka message key. The PRIMARY KEY also defines records in the 'upsert-kafka' table should update or delete on which keys.


-- [ERROR] Could not execute SQL statement. Reason:
-- org.apache.calcite.runtime.CalciteException: Non-query expression encountered in illegal context


-- [ERROR] Could not execute SQL statement. Reason:
-- org.apache.flink.table.api.ValidationException: Unsupported options found for 'upsert-kafka'.
-- Unsupported options:
-- scan.startup.mode
-- Supported options:

--> remove --    'scan.startup.mode'                      = 'earliest-offset',

-- [ERROR] Could not execute SQL statement. Reason:
-- org.apache.flink.table.api.ValidationException: One or more required options are missing.
-- Missing required options are:
-- url

-- [ERROR] Could not execute SQL statement. Reason:
-- org.apache.flink.table.api.ValidationException: Could not find required sink format 'key.format'.

-- errors then as per schema incompatibility, I added the created_at column that was not in the original avro topic schema, simply removed
-- the creation of the schema on the target topic, had flink/kafka self create it.


-- Our t_f_avro_salescompleted (OUTPUT) table which will push values to the CP Kafka topic.
-- https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/formats/avro-confluent/

CREATE OR REPLACE TABLE c_hive.db01.t_f_avro_salescompleted (
    `invoiceNumber`       STRING,
    `saleDateTime_Ltz`    STRING,
    `saleTimestamp_Epoc`  STRING,
    `terminalPoint`       STRING,
    `nett`                DOUBLE,
    `vat`                 DOUBLE,
    `total`               DOUBLE,
    `store`               row<`id` STRING, `name` STRING>,
    `clerk`               row<`id` STRING, `name` STRING, `surname` STRING>,
    `basketItems`         array<row<`id` STRING, `name` STRING, `brand` STRING, `category` STRING, `price` DOUBLE, `quantity` INT, `subtotal` DOUBLE>>,     
    `payDateTime_Ltz`     STRING,
    `payTimestamp_Epoc`   STRING,
    `paid`                DOUBLE,
    `finTransactionId`    STRING,
    `payTimestamp_WM`     AS TO_TIMESTAMP(FROM_UNIXTIME(CAST(`payTimestamp_Epoc` AS BIGINT) / 1000)),
    `saleTimestamp_WM`    AS TO_TIMESTAMP(FROM_UNIXTIME(CAST(`saleTimestamp_Epoc` AS BIGINT) / 1000)),
    WATERMARK FOR `saleTimestamp_WM` AS `saleTimestamp_WM`
) WITH (
    'connector'                                = 'kafka',
    'topic'                                    = 't_f_avro_salescompleted',
    'properties.bootstrap.servers'             = 'broker:29092',
    'properties.group.id'                      = 'testGroup',
    'scan.startup.mode'                        = 'earliest-offset',
    'value.format'                             = 'avro-confluent',
    'value.avro-confluent.schema-registry.url' = 'http://schema-registry:9081',
    'value.fields-include'                     = 'ALL'
);

-- unnest the salesBasket -> Output
-- Our t_f_unnested_sales (OUTPUT) table which will push values to the CP Kafka topic.

CREATE OR REPLACE TABLE c_hive.db01.t_f_unnested_sales (
    `store_id`            STRING,
    `product`             STRING,
    `brand`               STRING,
    `subtotal`            DOUBLE,
    `category`            STRING,
    `saleDateTime_Ltz`    STRING,
    `saleTimestamp_Epoc`  STRING,
    `saleTimestamp_WM`    AS TO_TIMESTAMP(FROM_UNIXTIME(CAST(`saleTimestamp_Epoc` AS BIGINT) / 1000)),
      WATERMARK FOR `saleTimestamp_WM` AS `saleTimestamp_WM`
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 't_f_unnested_sales',
    'properties.bootstrap.servers'  = 'broker:29092',
    'properties.group.id'           = 'testGroup',
    'scan.startup.mode'             = 'earliest-offset',
    'value.format'                  = 'avro-confluent',
    'value.avro-confluent.url'      = 'http://schema-registry:9081',
    'value.fields-include'          = 'ALL'
);


-- Create Iceberg target tables, stored on S3, data pulled from hive catalogged table/source, either as a HIVE computer table or from Kafka.

-- Our avro_salescompleted (OUTPUT) table which will push values to the CP Kafka topic.
-- https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/formats/avro-confluent/


-- the fields in the select is case sensitive, needs to match theprevious create tables which match the definitions in the struct/avro sections.
-- SET 'execution.runtime-mode' = 'streaming';
    
SET 'pipeline.name' = 'Sales Completed Join - Output to Kafka Topic';

Insert into c_hive.db01.t_f_avro_salescompleted
select
        b.invoiceNumber,
        b.saleDateTime_Ltz,
        b.saleTimestamp_Epoc,
        b.terminalPoint,
        b.nett,
        b.vat,
        b.total,
        b.store,
        b.clerk,    
        b.basketItems,        
        a.paydatetime_ltz,
        a.paytimestamp_epoc,
        a.paid,
        a.fintransactionid
    FROM 
        c_hive.db01.t_f_pgcdc_salespayments a,
        c_hive.db01.t_k_avro_salesbaskets b
    WHERE a.invoicenumber = b.invoiceNumber
    AND a.created_at > b.saleTimestamp_WM 
    AND b.saleTimestamp_WM > (b.saleTimestamp_WM - INTERVAL '1' HOUR);

-- [ERROR] Could not execute SQL statement. Reason:
-- org.apache.flink.table.api.TableException: Table sink 'c_hive.db01.t_f_avro_salescompleted' doesn't support consuming update and delete changes which is produced by node Join(joinType=[InnerJoin], where=[((invoicenumber = invoiceNumber) AND (created_at > saleTimestamp_WM))], select=[invoicenumber, paydatetime_ltz, paytimestamp_epoc, paid, fintransactionid, created_at, invoiceNumber, saleDateTime_Ltz, saleTimestamp_Epoc, terminalPoint, nett, vat, total, store, clerk, basketItems, saleTimestamp_WM], leftInputSpec=[JoinKeyContainsUniqueKey], rightInputSpec=[NoUniqueKey])
