-- Usage for Table/SQL API

-- NOTE: 
-- Case sentivity... need to match the case as per types/fs.go structs avro sections.

-- Set checkpoint to happen every minute
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.interval' = '10s';

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
     'connector'                                 = 'kafka'
    ,'topic'                                     = 'avro_salesbaskets'
    ,'properties.bootstrap.servers'              = 'broker:29092'
    ,'properties.group.id'                       = 'testGroup'
    ,'scan.startup.mode'                         = 'earliest-offset'
    ,'value.format'                              = 'avro-confluent'
    ,'value.avro-confluent.schema-registry.url'  = 'http://schema-registry:9081'
    ,'key.format'                                = 'raw'
    ,'key.fields'                                = 'invoiceNumber'
    ,'value.fields-include'                      = 'ALL'
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
     'connector'                               = 'upsert-kafka'
    ,'topic'                                   = 'avro_salespayments'
    ,'properties.bootstrap.servers'            = 'broker:29092'
    ,'value.format'                            = 'avro-confluent'
    ,'value.avro-confluent.schema-registry.url'= 'http://schema-registry:9081'
    ,'key.format'                              = 'raw'
    ,'properties.group.id'                     = 'mysqlcdcsourced' 
    ,'value.fields-include'                    = 'ALL'
);


SET 'pipeline.name' = 'SalesPayments - (t_f_avro_salespayments) -> Kafka (avro_salespayments)';

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

-- INSERT INTO c_hive.db01.t_f_avro_salespayments (
--     `invoiceNumber`,
--     `payDateTime_Ltz`,
--     `payTimestamp_Epoc`,
--     `paid`,
--     `finTransactionId`,
--     `created_at`
--   ) SELECT
--     invoicenumber,
--     paydatetime_ltz,  
--     paytimestamp_epoc,
--     paid,
--     fintransactionid,
--     created_at
--   FROM 
--     c_hive.db01.t_f_pgcdc_salespayments;



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
    WATERMARK FOR `saleTimestamp_WM` AS `saleTimestamp_WM`,
    PRIMARY KEY (`invoiceNumber`) NOT ENFORCED
) WITH (
     'connector'                               = 'upsert-kafka'
    ,'topic'                                   = 'avro_salescompleted'
    ,'properties.bootstrap.servers'            = 'broker:29092'
    ,'value.format'                            = 'avro-confluent'
    ,'value.avro-confluent.schema-registry.url'= 'http://schema-registry:9081'
    ,'key.format'                              = 'raw'
    ,'properties.group.id'                     = 'mysqlcdcsourced' 
    ,'value.fields-include'                    = 'ALL'
);


-- SET 'execution.runtime-mode' = 'streaming';
    
SET 'pipeline.name' = 'SalesCompleted - (t_f_avro_salescompleted) -> Kafka (avro_salescompleted)';

INSERT INTO c_hive.db01.t_f_avro_salescompleted
SELECT
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
        a.payDateTime_Ltz,
        a.payTimestamp_Epoc,
        a.paid,
        a.finTransactionId
    FROM 
        c_hive.db01.t_f_avro_salespayments a,
        c_hive.db01.t_k_avro_salesbaskets b
    WHERE a.invoiceNumber = b.invoiceNumber
    AND a.created_at > b.saleTimestamp_WM 
    AND b.saleTimestamp_WM > (b.saleTimestamp_WM - INTERVAL '1' HOUR);

-- unnest the salesBasket -> Output
-- Our t_f_unnested_sales (OUTPUT) table which will push values to the CP Kafka topic.

CREATE OR REPLACE TABLE c_hive.db01.t_2_unnested_sales (
    `invoicenumber`       STRING
    ,`store_id`            STRING
    ,`category`            STRING
    ,`brand`               STRING
    ,`product`             STRING
    ,`subtotal`            DOUBLE
    ,`saleDateTime_Ltz`    STRING
    ,`saleTimestamp_Epoc`  STRING
    ,`saleTimestamp_WM`    AS TO_TIMESTAMP(FROM_UNIXTIME(CAST(`saleTimestamp_Epoc` AS BIGINT) / 1000))
    ,WATERMARK FOR `saleTimestamp_WM` AS `saleTimestamp_WM`
    ,PRIMARY KEY (`invoicenumber`, `store_id`, `category`, `brand`, `product`) NOT ENFORCED
) WITH (
     'connector'                               = 'upsert-kafka'
    ,'topic'                                   = 'avro_unnested_sales'
    ,'properties.bootstrap.servers'            = 'broker:29092'
    ,'value.format'                            = 'avro-confluent'
    ,'value.avro-confluent.schema-registry.url'= 'http://schema-registry:9081'
    ,'key.format'                              = 'avro-confluent'
    ,'key.avro-confluent.schema-registry.url'  = 'http://schema-registry:9081'
    ,'properties.group.id'                     = 'mysqlcdcsourced' 
    ,'value.fields-include'                    = 'ALL'
);

-- -- If the Primary key is multiple columns then you can't use raw format.

SET 'pipeline.name' = 'SalesUnnested Basket - (t_2_unnested_sales) -> Kafka (avro_unnested_sales)';

INSERT INTO c_hive.db01.t_2_unnested_sales
  SELECT
      `invoiceNumber`       AS `invoicenumber`,
      `store`.`id`          AS `store_id`,
      bi.`category`         AS `category`,
      bi.`brand`            AS `brand`,
      bi.`name`             AS `product`,
      bi.`subtotal`         AS `subtotal`,
      `saleDateTime_Ltz`    as saleDateTime_Ltz,
      `saleTimestamp_Epoc`  as saleTimestamp_Epoc
    FROM c_hive.db01.t_f_avro_salescompleted  -- assuming avro_salescompleted is a table function
    CROSS JOIN UNNEST(`basketItems`) AS bi;


------------------------------------------------------------------------
-- Apache Paimon outputs
------------------------------------------------------------------------

SET 'pipeline.name' = 'SalesBasket (t_salesbaskets) -> Paimon (c_paimon.t_salesbaskets)';

CREATE TABLE c_paimon.dev.t_salesbaskets WITH (
    'file.format' = 'avro'
  )
  AS SELECT 
    `invoiceNumber`,
    `saleDateTime_Ltz`,
    `saleTimestamp_Epoc`,
    `terminalPoint`,
    `nett`,
    `vat`,
    `total`,
    `store`,
    `clerk`,
    `basketItems`,
    `saleTimestamp_WM`
  FROM c_hive.db01.t_k_avro_salesbaskets;


SET 'pipeline.name' = 'SalesPayments (t_salespayments) -> Paimon (c_paimon.dev.t_salespayments)';

CREATE TABLE c_paimon.dev.t_salespayments AS
  SELECT 
    `invoiceNumber`,
    `payDateTime_Ltz`,
    `payTimestamp_Epoc`,
    `paid`,
    `finTransactionId`,
    `payTimestamp_WM`
  FROM c_hive.db01.t_f_avro_salespayments;


SET 'pipeline.name' = 'SalesCompleted (t_salescompleted) -> Paimon (c_paimon.dev.t_salescompleted)';

CREATE TABLE c_paimon.dev.t_salescompleted WITH (
    'file.format' = 'avro'
  ) AS SELECT 
    `invoiceNumber`,
    `saleDateTime_Ltz`,
    `saleTimestamp_Epoc`,
    `terminalPoint`,
    `nett`,
    `vat`,
    `total`,
    `store`,
    `clerk`,
    `basketItems`,     
    `payDateTime_Ltz`,
    `payTimestamp_Epoc`,
    `paid`,
    `finTransactionId`,
    `payTimestamp_WM`,
    `saleTimestamp_WM`
   FROM c_hive.db01.t_f_avro_salescompleted;


-- SET 'pipeline.name' = 'SalesUnnested Basket (t_unnested_sales) -> Paimon (c_paimon.dev.t_unnested_sales)';

CREATE TABLE c_paimon.dev.t_unnested_sales WITH (
    'file.format' = 'parquet'
  ) AS SELECT 
      `invoicenumber`,
      `store_id`,
      `category`,
      `product` ,
      `brand` ,
      `subtotal`,
      `saleDateTime_Ltz`,
      `saleTimestamp_Epoc`
  FROM c_hive.db01.t_2_unnested_sales;
