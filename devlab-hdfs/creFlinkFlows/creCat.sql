
-- https://paimon.apache.org/docs/0.7/how-to/creating-catalogs/#creating-a-catalog-with-filesystem-metastore


CREATE CATALOG c_hive WITH (
  'type'          = 'hive',
  'hive-conf-dir' = '/opt/sql-client/conf'
);

USE CATALOG c_hive;

CREATE DATABASE c_hive.db01;

-- SHOW DATABASES;
-- USE c_hive.db01;
-- SHOW TABLES;

USE CATALOG default_catalog;

CREATE CATALOG c_paimon WITH (
  'type'                        = 'paimon',
  'catalog-type'                = 'hadoop',     
  'warehouse'                   = 'hdfs://namenode:9000/paimon/',
  'property-version'            = '1',
  'table-default.file.format'   = 'parquet'
);

-- With above we just create table on storage, table inherites type from catalog definition

USE CATALOG c_paimon;

CREATE DATABASE c_paimon.dev;

-- SHOW DATABASES;
-- USE c_paimon.dev;
-- SHOW TABLES;



