
-- https://www.decodable.co/blog/catalogs-in-flink-sql-hands-on
-- https://github.com/decodableco/examples/tree/main/catalogs/flink-iceberg-jdbc


-- This willbe the commands used to create the catalog sitting behind flink
--

-- INTERESTING, things written to the c_hive catalog is only recorded as existing in the hive catalog, but not persisted to Minio/S3... The persistence in this case
-- comes from salescompleted writing out to Kafka. 

CREATE CATALOG c_hive WITH (
        'type'          = 'hive',
        'hive-conf-dir' = './conf'
);

USE CATALOG c_hive;

CREATE DATABASE c_hive.db01;

USE c_hive.db01;
SHOW TABLES;

USE CATALOG default_catalog;

-- => http://minio:9000/warehouse/dev/db/*
CREATE CATALOG c_iceberg WITH (
       'type'           = 'iceberg',
       'catalog-type'   = 'hive',
       'warehouse'      = 's3a://iceberg',
       'hive-conf-dir'  = './conf'
);

USE CATALOG c_iceberg;

CREATE DATABASE c_iceberg.dev;

USE c_iceberg.dev;
SHOW TABLES;


