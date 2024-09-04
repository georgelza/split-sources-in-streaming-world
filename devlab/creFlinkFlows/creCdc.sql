-- https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/


SET 'execution.checkpointing.interval' = '1s';

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
        'scan.startup.mode'     = 'latest-offset',
        'server-id'             = '184054'
);

SET 'pipeline.name' = 'creCdc - Create Source PostgreSql: t_f_pgcdc_salespayments table';

-- register a PostgreSql table 'salespayments' in Flink SQL
--
CREATE TABLE c_hive.db01.t_f_pgcdc_salespayments (
        invoicenumber           STRING,
        paydatetime_ltz         STRING,
        paytimestamp_epoc       STRING,
        paid                    DOUBLE,
        fintransactionid        STRING,
	created_at              TIMESTAMP,
        PRIMARY KEY(invoicenumber) NOT ENFORCED
) WITH (
        'connector'                         = 'postgres-cdc',
        'hostname'                          = 'postgrescdc',
        'port'                              = '5432',               -- NOTE: this is the port of the db on the container, not the external docker exported port via a port mapping.
        'username'                          = 'flinkcdc',
        'password'                          = 'flinkpw',
        'database-name'                     = 'sales',
        'schema-name'                       = 'public',
        'table-name'                        = 'salespayments',
        'slot.name'                         = 'flinkcdc',
        'scan.incremental.snapshot.enabled' = 'true',   -- experimental feature: incremental snapshot (default off)
        'scan.startup.mode'                 = 'latest-offset',
        'decoding.plugin.name'              = 'pgoutput'
);

-- NOTE:
-- Flink CDC uses debezium engine internally.
-- You can pass debezium configuration using debezium. as a prefix: