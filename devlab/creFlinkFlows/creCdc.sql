-- https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/
-- https://www.youtube.com/watch?v=5AThYUD4grA

-- Create 2 Flink Source tables related to 2 database tables (1 from MySQL CDC and 1 from MySQL CDC)

SET 'execution.checkpointing.interval' = '5s';

SET 'pipeline.name' = 'creCdc - Create Source Mysql: t_f_msqlcdc_salespayments table';

-- register a MySQL table 'salespayments' in Flink SQL
--
CREATE TABLE c_hive.db01.t_f_msqlcdc_salespayments (
        `invoiceNumber`         STRING,
        `payDateTime_Ltz`       STRING,
        `payTimestamp_Epoc`     STRING,
        `paid`                  DOUBLE,
        `finTransactionId`      STRING,
	`created_at`            TIMESTAMP,
        PRIMARY KEY(`invoiceNumber`) NOT ENFORCED
) WITH (
        'connector'             = 'mysql-cdc',
        'hostname'              = 'mysqlcdc',
        'port'                  = '3306',
        'username'              = 'flinkcdc',
        'password'              = 'flinkpw',
        'database-name'         = 'sales',
        'table-name'            = 'salespayments',
        'scan.startup.mode'     = 'earliest-offset',  -- https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/#startup-reading-position
        'server-id'             = '184054'
);

-- SET 'pipeline.name' = 'creCdc - Create Source PostgreSql: t_f_pgcdc_salespayments table';

-- -- register a PostgreSql table 'salespayments' in Flink SQL
-- --
-- CREATE TABLE c_hive.db01.t_f_pgcdc_salespayments (
--         invoicenumber           STRING,
--         paydatetime_ltz         STRING,
--         paytimestamp_epoc       STRING,
--         paid                    DOUBLE,
--         fintransactionid        STRING,
-- 	created_at              TIMESTAMP,
--         PRIMARY KEY(invoicenumber) NOT ENFORCED
-- ) WITH (
--         'connector'                         = 'postgres-cdc',
--         'hostname'                          = 'postgrescdc',
--         'port'                              = '5432',               -- NOTE: this is the port of the db on the container, not the external docker exported port via a port mapping.
--         'username'                          = 'flinkcdc',
--         'password'                          = 'flinkpw',
--         'database-name'                     = 'sales',
--         'schema-name'                       = 'public',
--         'table-name'                        = 'salespayments',
--         'slot.name'                         = 'flinkcdc',
--         'scan.incremental.snapshot.enabled' = 'true',   -- experimental feature: incremental snapshot (default off)
--         'scan.startup.mode'                 = 'initial', -- https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/postgres-cdc/#startup-reading-position
--         'decoding.plugin.name'              = 'pgoutput'
-- );

-- NOTE:
-- Flink CDC uses debezium engine internally.
-- You can pass debezium configuration using debezium. as a prefix: