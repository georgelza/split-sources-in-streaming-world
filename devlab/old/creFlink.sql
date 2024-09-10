-- Usage for Table/SQL API

-- NOTE: 
-- Case sentivity... need to match the case as per types/fs.go structs avro sections.

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
     'connector'                                 = 'kafka'
    ,'topic'                                     = 'avro_salesbaskets'
    ,'properties.bootstrap.servers'              = 'broker:29092'
    ,'properties.group.id'                       = 'testGroup'
    ,'scan.startup.mode'                         = 'earliest-offset'
    ,'key.fields'                                = 'invoiceNumber'
    ,'key.format'                                = 'raw'
    ,'value.format'                              = 'avro-confluent'
    ,'value.avro-confluent.schema-registry.url'  = 'http://schema-registry:9081'
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
    ,'value.fields-include'                    = 'ALL'
    ,'value.format'                            = 'avro-confluent'
    ,'value.avro-confluent.schema-registry.url'= 'http://schema-registry:9081'
    ,'key.format'                              = 'raw'
    ,'properties.group.id'                     = 'mysqlcdcsourced' 
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
    ,'topic'                                   = 't_f_avro_salescompleted'
    ,'properties.bootstrap.servers'            = 'broker:29092'
    ,'value.fields-include'                    = 'ALL'
    ,'value.format'                            = 'avro-confluent'
    ,'value.avro-confluent.schema-registry.url'= 'http://schema-registry:9081'
    ,'key.format'                              = 'raw'
    ,'properties.group.id'                     = 'mysqlcdcsourced' 
    ,'value.fields-include'                    = 'ALL'
);


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

CREATE OR REPLACE TABLE c_hive.db01.t_f_unnested_sales (
    `store_id`            STRING,
    `product`             STRING,
    `brand`               STRING,
    `subtotal`            DOUBLE,
    `category`            STRING,
    `saleDateTime_Ltz`    STRING,
    `saleTimestamp_Epoc`  STRING,
    `saleTimestamp_WM`    AS TO_TIMESTAMP(FROM_UNIXTIME(CAST(`saleTimestamp_Epoc` AS BIGINT) / 1000)),
    WATERMARK FOR `saleTimestamp_WM` AS `saleTimestamp_WM`,
    PRIMARY KEY (`store_id`, `product`, `brand`) NOT ENFORCED
) WITH (
     'connector'                               = 'upsert-kafka'
    ,'topic'                                   = 't_f_unnested_sales'
    ,'properties.bootstrap.servers'            = 'broker:29092'
    ,'value.fields-include'                    = 'ALL'
    ,'value.format'                            = 'avro-confluent'
    ,'value.avro-confluent.schema-registry.url'= 'http://schema-registry:9081'
    ,'key.format'                              = 'avro-confluent'
    ,'key.avro-confluent.schema-registry.url'  = 'http://schema-registry:9081'
    ,'properties.group.id'                     = 'mysqlcdcsourced' 
    ,'value.fields-include'                    = 'ALL'
);


SET 'pipeline.name' = 'Sales Completed, Unnested Basket - Output to Kafka Topic';

INSERT INTO c_hive.db01.t_f_unnested_sales
  SELECT
      `store`.`id` as `store_id`,
      bi.`name` AS `product`,
      bi.`brand` AS `brand`,
      bi.`subtotal` AS `subtotal`,
      bi.`category` AS `category`,
      `saleDateTime_Ltz` as saleDateTime_Ltz,
      `saleTimestamp_Epoc` as saleTimestamp_Epoc
    FROM c_hive.db01.t_f_avro_salescompleted  -- assuming avro_salescompleted is a table function
    CROSS JOIN UNNEST(`basketItems`) AS bi;


------------------------------------------------------------------------
-- Apache Paimon outputs
------------------------------------------------------------------------

SET 'pipeline.name' = 'Sales Basket Source - Output to Paimon Table';

CREATE TABLE c_paimon.dev.t_salesbaskets AS 
  SELECT 
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


SET 'pipeline.name' = 'Sales Payments Source - Output to Paimon Table';

CREATE TABLE c_paimon.dev.t_salespayments AS
  SELECT 
    `invoiceNumber`,
    `payDateTime_Ltz`,
    `payTimestamp_Epoc`,
    `paid`,
    `finTransactionId`,
    `payTimestamp_WM`
  FROM c_hive.db01.t_f_avro_salespayments;


SET 'pipeline.name' = 'Sales Completed - Output to Paimon Table';

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


SET 'pipeline.name' = 'Unnested Sales Baskets - Output to Paimon Target';

CREATE TABLE c_paimon.dev.t_unnested_sales WITH (
    'file.format' = 'avro'
  ) AS 
  SELECT 
      `store_id`,
      `product` ,
      `brand` ,
      `subtotal`,
      `category`,
      `saleDateTime_Ltz`,
      `saleTimestamp_Epoc`
  FROM c_hive.db01.t_f_unnested_sales;


------------------------------------------------------------------------------------
-- Some More aggregations
------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE c_hive.db01.t_f_avro_sales_per_store_per_brand_per_5min_x (
  `store_id` STRING,
  `brand` STRING,
  window_start  TIMESTAMP(3),
  window_end TIMESTAMP(3),
  `salesperbrand` BIGINT,
  `totalperbrand` DOUBLE
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'avro_sales_per_store_per_brand_per_5min_x',
    'properties.bootstrap.servers'  = 'broker:29092',
    'properties.group.id'           = 'testGroup',
    'scan.startup.mode'             = 'earliest-offset',
    'value.format'                  = 'avro-confluent',
    'value.avro-confluent.url'      = 'http://schema-registry:9081',
    'value.fields-include'          = 'ALL'
);



-- Errors from here
SET 'pipeline.name' = 'Sales Per Store Per Brand per 5min - Output to Kafka Topic';

INSERT INTO c_hive.db01.t_f_avro_sales_per_store_per_brand_per_5min_x
SELECT 
    store_id,
    brand,
    window_start,
    window_end,
    COUNT(*) as `salesperbrand`,
    SUM(subtotal) as `totalperbrand`
  FROM TABLE(
    TUMBLE(TABLE c_hive.db01.t_f_unnested_sales, DESCRIPTOR(saleTimestamp_WM), INTERVAL '5' MINUTE))
  GROUP BY store_id, brand, window_start, window_end;


CREATE OR REPLACE TABLE c_hive.db01.t_f_avro_sales_per_store_per_product_per_5min_x (
  `store_id` STRING,
  `product` STRING,
  window_start  TIMESTAMP(3),
  window_end TIMESTAMP(3),
  `salesperproduct` BIGINT,
  `totalperproduct` DOUBLE
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'avro_sales_per_store_per_product_per_5min_x',
    'properties.bootstrap.servers'  = 'broker:29092',
    'properties.group.id'           = 'testGroup',
    'scan.startup.mode'             = 'earliest-offset',
    'value.format'                  = 'avro-confluent',
    'value.avro-confluent.url'      = 'http://schema-registry:9081',
    'value.fields-include'          = 'ALL'
);


SET 'pipeline.name' = 'Sales Per Store Per Product per 5min - Output to Kafka Topic';

INSERT INTO c_hive.db01.t_f_avro_sales_per_store_per_product_per_5min_x
SELECT 
    store_id,
    product,
    window_start,
    window_end,
    COUNT(*) as `salesperproduct`,
    SUM(saleValue) as `totalperproduct`
  FROM TABLE(
    TUMBLE(TABLE c_hive.db01.t_f_unnested_sales, DESCRIPTOR(saleTimestamp_WM), INTERVAL '5' MINUTE))
  GROUP BY store_id, product, window_start, window_end;



CREATE OR REPLACE TABLE c_hive.db01.t_f_avro_sales_per_store_per_category_per_5min_x (
  `store_id` STRING,
  `category` STRING,
  window_start  TIMESTAMP(3),
  window_end TIMESTAMP(3),
  `salesperproduct` BIGINT,
  `totalperproduct` DOUBLE
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'avro_sales_per_store_per_category_per_5min_x',
    'properties.bootstrap.servers'  = 'broker:29092',
    'properties.group.id'           = 'testGroup',
    'scan.startup.mode'             = 'earliest-offset',
    'value.format'                  = 'avro-confluent',
    'value.avro-confluent.url'      = 'http://schema-registry:9081',
    'value.fields-include'          = 'ALL'
);


SET 'pipeline.name' = 'Sales Per Store Per Category per 5min - Output to Kafka Topic';

INSERT INTO c_hive.db01.t_f_avro_sales_per_store_per_category_per_5min_x
SELECT 
    store_id,
    category,
    window_start,
    window_end,
    COUNT(*) as `salespercategory`,
    SUM(saleValue) as `totalpercategory`
  FROM TABLE(
    TUMBLE(TABLE c_hive.db01.t_f_unnested_sales, DESCRIPTOR(saleTimestamp_WM), INTERVAL '5' MINUTE))
  GROUP BY store_id, category, window_start, window_end;


CREATE OR REPLACE TABLE c_hive.db01.t_f_avro_sales_per_store_per_terminal_per_5min_x (
    `store_id` STRING,
    `terminalPoint` STRING,
    window_start  TIMESTAMP(3),
    window_end TIMESTAMP(3),
    `salesperterminal` BIGINT,
    `totalperterminal` DOUBLE
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'avro_sales_per_store_per_terminal_per_5min_x',
    'properties.bootstrap.servers'  = 'broker:29092',
    'properties.group.id'           = 'testGroup',
    'scan.startup.mode'             = 'earliest-offset',
    'value.format'                  = 'avro-confluent',
    'value.avro-confluent.url'      = 'http://schema-registry:9081',
    'value.fields-include'          = 'ALL'
);


SET 'pipeline.name' = 'Sales Per Store Per Terminal per 5min - Output to Kafka Topic';

INSERT INTO c_hive.db01.t_f_avro_sales_per_store_per_terminal_per_5min_x
SELECT 
    `store`.`id` as `store_id`,
    terminalPoint,
    window_start,
    window_end,
    COUNT(*) as `salesperterminal`,
    SUM(total) as `totalperterminal`
  FROM TABLE(
    TUMBLE(TABLE c_hive.db01.t_f_avro_salescompleted, DESCRIPTOR(saleTimestamp_WM), INTERVAL '5' MINUTES))
  GROUP BY `store`.`id`, terminalPoint, window_start, window_end;

CREATE OR REPLACE TABLE c_hive.db01.t_f_avro_sales_per_store_per_terminal_per_hour_x (
    `store_id` STRING,
    `terminalPoint` STRING,
    window_start  TIMESTAMP(3),
    window_end TIMESTAMP(3),
    `salesperterminal` BIGINT,
    `totalperterminal` DOUBLE
) WITH (
    'connector'                     = 'kafka',
    'topic'                         = 'avro_sales_per_store_per_terminal_per_hour_x',
    'properties.bootstrap.servers'  = 'broker:29092',
    'properties.group.id'           = 'testGroup',
    'scan.startup.mode'             = 'earliest-offset',
    'value.format'                  = 'avro-confluent',
    'value.avro-confluent.url'      = 'http://schema-registry:9081',
    'value.fields-include'          = 'ALL'
);

SET 'pipeline.name' = 'Sales Per Store Per Terminal per hour - Output to Kafka Topic';

INSERT INTO c_hive.db01.t_f_avro_sales_per_store_per_terminal_per_hour_x
SELECT 
    `store`.`id` as `store_id`,
    terminalPoint,
    window_start,
    window_end,
    COUNT(*) as `salesperterminal`,
    SUM(total) as `totalperterminal`
  FROM TABLE(
    TUMBLE(TABLE c_hive.db01.t_f_avro_salescompleted, DESCRIPTOR(saleTimestamp_WM), INTERVAL '1' HOUR))
  GROUP BY `store`.`id`, terminalPoint, window_start, window_end;