SET GLOBAL time_zone = '+2:00';

USE sales;
CREATE TABLE IF NOT EXISTS sales.salespayments(
        `invoiceNumber`         varchar(40) NOT NULL, 
        `payDateTime_Ltz`       text, 
        `payTimestamp_Epoc`     text,
        `paid`                  double,
        `finTransactionId`      text, 
        created_at datetime default CURRENT_TIMESTAMP,
        CONSTRAINT PK_Salespayments PRIMARY KEY (`invoiceNumber`)
        );

CREATE USER IF NOT EXISTS 'flinkcdc'@'sales' IDENTIFIED BY 'flinkpw';
GRANT SELECT, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'flinkcdc'@'sales';
FLUSH PRIVILEGES;