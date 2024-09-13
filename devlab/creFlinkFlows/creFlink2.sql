-- Usage for Table/SQL API


-- https://medium.com/@ipolyzos_/apache-paimon-introducing-deletion-vectors-584666ee90de
-- deletion-vectors.enabled = true -> use ORC file format.


------------------------------------------------------------------------------------
-- Some More aggregations
------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE c_hive.db01.t_f_avro_sales_per_store_per_brand_per_5min_x (
  `store_id`      STRING,
  `brand`         STRING,
  window_start    TIMESTAMP(3),
  window_end      TIMESTAMP(3),
  `salesperbrand` BIGINT,
  `totalperbrand` DOUBLE,
  PRIMARY KEY (`store_id`, `brand`, `window_start`, `window_end`) NOT ENFORCED
) WITH (
     'connector'                              = 'upsert-kafka'
    ,'topic'                                  = 'avro_sales_per_store_per_brand_per_5min_x'
    ,'properties.bootstrap.servers'           = 'broker:29092'
    ,'scan.startup.mode'                      = 'earliest-offset'
    ,'value.format'                           = 'avro-confluent'
    ,'value.avro-confluent.url'               = 'http://schema-registry:9081'
    ,'key.format'                             = 'avro-confluent'
    ,'key.avro-confluent.schema-registry.url' = 'http://schema-registry:9081'
    ,'properties.group.id'                    = 'mysqlcdcsourced' 
    ,'value.fields-include'                   = 'ALL'
);


-- Errors from here
SET 'pipeline.name' = 'Sales Per Store Per Brand per 5min - Output to Kafka Topic';

INSERT INTO c_hive.db01.t_f_avro_sales_per_store_per_brand_per_5min_x
SELECT 
    store_id,
    brand,
    window_start,
    window_end,
    COUNT(*)      as `salesperbrand`,
    SUM(subtotal) as `totalperbrand`
  FROM TABLE(
    TUMBLE(TABLE c_hive.db01.t_f_unnested_sales, DESCRIPTOR(saleTimestamp_WM), INTERVAL '5' MINUTE))
  GROUP BY store_id, brand, window_start, window_end;


CREATE OR REPLACE TABLE c_hive.db01.t_f_avro_sales_per_store_per_product_per_5min_x (
  `store_id`          STRING,
  `product`           STRING,
  window_start        TIMESTAMP(3),
  window_end          TIMESTAMP(3),
  `salesperproduct`   BIGINT,
  `totalperproduct`   DOUBLE,
  PRIMARY KEY (`store_id`, `product`, `window_start`, `window_end`) NOT ENFORCED
) WITH (
     'connector'                              = 'upsert-kafka'
    ,'topic'                                  = 'avro_sales_per_store_per_product_per_5min_x'
    ,'properties.bootstrap.servers'           = 'broker:29092'
    ,'scan.startup.mode'                      = 'earliest-offset'
    ,'value.format'                           = 'avro-confluent'
    ,'value.avro-confluent.url'               = 'http://schema-registry:9081'
    ,'key.format'                             = 'avro-confluent'
    ,'key.avro-confluent.schema-registry.url' = 'http://schema-registry:9081'
    ,'properties.group.id'                    = 'mysqlcdcsourced' 
    ,'value.fields-include'                   = 'ALL'
);

SET 'pipeline.name' = 'Sales Per Store Per Product per 5min - Output to Kafka Topic';

INSERT INTO c_hive.db01.t_f_avro_sales_per_store_per_product_per_5min_x
SELECT 
    store_id,
    product,
    window_start,
    window_end,
    COUNT(*)        as `salesperproduct`,
    SUM(saleValue)  as `totalperproduct`
  FROM TABLE(
    TUMBLE(TABLE c_hive.db01.t_f_unnested_sales, DESCRIPTOR(saleTimestamp_WM), INTERVAL '5' MINUTE))
  GROUP BY store_id, product, window_start, window_end;



CREATE OR REPLACE TABLE c_hive.db01.t_f_avro_sales_per_store_per_category_per_5min_x (
  `store_id`        STRING,
  `category`        STRING,
  window_start      TIMESTAMP(3),
  window_end        TIMESTAMP(3),
  `salesperproduct` BIGINT,
  `totalperproduct` DOUBLE,
  PRIMARY KEY (`store_id`, `category`, `window_start`, `window_end`) NOT ENFORCED
) WITH (
     'connector'                              = 'upsert-kafka'
    ,'topic'                                  = 'avro_sales_per_store_per_category_per_5min_x',
    ,'properties.bootstrap.servers'           = 'broker:29092',
    ,'scan.startup.mode'                      = 'earliest-offset',
    ,'value.format'                           = 'avro-confluent',
    ,'value.avro-confluent.url'               = 'http://schema-registry:9081'
    ,'key.format'                             = 'avro-confluent'
    ,'key.avro-confluent.schema-registry.url' = 'http://schema-registry:9081'
    ,'properties.group.id'                    = 'mysqlcdcsourced' 
    ,'value.fields-include'                   = 'ALL'
);


SET 'pipeline.name' = 'Sales Per Store Per Category per 5min - Output to Kafka Topic';

INSERT INTO c_hive.db01.t_f_avro_sales_per_store_per_category_per_5min_x
SELECT 
    store_id,
    category,
    window_start,
    window_end,
    COUNT(*)        as `salespercategory`,
    SUM(saleValue)  as `totalpercategory`
  FROM TABLE(
    TUMBLE(TABLE c_hive.db01.t_f_unnested_sales, DESCRIPTOR(saleTimestamp_WM), INTERVAL '5' MINUTE))
  GROUP BY store_id, category, window_start, window_end;


CREATE OR REPLACE TABLE c_hive.db01.t_f_avro_sales_per_store_per_terminal_per_5min_x (
    `store_id`          STRING,
    `terminalPoint`     STRING,
    window_start        TIMESTAMP(3),
    window_end          TIMESTAMP(3),
    `salesperterminal`  BIGINT,
    `totalperterminal`  DOUBLE,
    PRIMARY KEY (`store_id`, `terminalPoint`, `window_start`, `window_end`) NOT ENFORCED
) WITH (
     'connector'                              = 'upsert-kafka'
    ,'topic'                                  = 'avro_sales_per_store_per_terminal_per_5min_x'
    ,'properties.bootstrap.servers'           = 'broker:29092'
    ,'scan.startup.mode'                      = 'earliest-offset'
    ,'value.format'                           = 'avro-confluent'
    ,'value.avro-confluent.url'               = 'http://schema-registry:9081'
    ,'key.format'                             = 'avro-confluent'
    ,'key.avro-confluent.schema-registry.url' = 'http://schema-registry:9081'
    ,'properties.group.id'                    = 'mysqlcdcsourced' 
    ,'value.fields-include'                   = 'ALL'
);


SET 'pipeline.name' = 'Sales Per Store Per Terminal per 5min - Output to Kafka Topic';

INSERT INTO c_hive.db01.t_f_avro_sales_per_store_per_terminal_per_5min_x
SELECT 
    `store`.`id`    as `store_id`,
    terminalPoint,
    window_start,
    window_end,
    COUNT(*)        as `salesperterminal`,
    SUM(total)      as `totalperterminal`
  FROM TABLE(
    TUMBLE(TABLE c_hive.db01.t_f_avro_salescompleted, DESCRIPTOR(saleTimestamp_WM), INTERVAL '5' MINUTES))
  GROUP BY `store`.`id`, terminalPoint, window_start, window_end;
