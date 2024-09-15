
-- https://paimon.apache.org/docs/0.7/how-to/creating-catalogs/#creating-a-catalog-with-filesystem-metastore


USE CATALOG default_catalog;

CREATE CATALOG c_hive WITH (
  'type'          = 'hive',
  'hive-conf-dir' = './conf/'
);

USE CATALOG c_hive;

CREATE DATABASE db01;

-- SHOW DATABASES;
-- USE c_hive.db01;
-- SHOW TABLES;

-- Paimon on S3
USE CATALOG default_catalog;

CREATE CATALOG c_paimon WITH (
  'type'                        = 'paimon',
  'catalog-type'                = 'hadoop',     
  'warehouse'                   = 'hdfs://namenode:9000/paimon/',
  'property-version'            = '1',
  'table-default.file.format'   = 'parquet'
);

-- -- With above we just create table on storage, table inherites type from catalog definition

USE CATALOG c_paimon;

CREATE DATABASE IF NOT EXISTS dev;
-- => dev.db

-- SHOW DATABASES;
-- USE c_paimon.dev;
-- SHOW TABLES;



-- For now I'm specifying the AWS user and password using 
-- CREATE CATALOG c_paimon WITH (
--      'type'                      = 'paimon'
--     ,'warehouse'                 = 's3://paimon/'
--     ,'catalog-type'              = 'hive'
--     ,'hive-conf-dir'             = './conf'
--     ,'table-default.file.format' = 'parquet'
--     ,'fs.s3a.endpoint'           = 'http://minio:9000'
--     ,'fs.s3a.access-key'         = 'admin'
--     ,'fs.s3a.secret-key'         = 'password'
-- );
