-- mysql -u root -pdbpassword -h localhost sales

SET GLOBAL time_zone = '+2:00';
SET GLOBAL binlog_expire_logs_seconds = 60000;


CREATE USER IF NOT EXISTS 'flinkcdc'@'sales' IDENTIFIED BY 'flinkpw';
GRANT SELECT, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'flinkcdc'@'%';
FLUSH PRIVILEGES;

-- Uses sales database.
USE sales;

-- Create test tables.
DROP TABLE IF EXISTS sales.salespayments;

CREATE TABLE IF NOT EXISTS sales.salespayments(
        `invoiceNumber`         varchar(40) NOT NULL, 
        `payDateTime_Ltz`       text, 
        `payTimestamp_Epoc`     text,
        `paid`                  double,
        `finTransactionId`      text, 
        created_at datetime default CURRENT_TIMESTAMP,
        CONSTRAINT PK_Salespayments PRIMARY KEY (`invoiceNumber`)
        );

-- grant SUPER, REPLICATION CLIENT to user;
-- https://dev.mysql.com/doc/refman/8.4/en/replication-howto-repuser.html