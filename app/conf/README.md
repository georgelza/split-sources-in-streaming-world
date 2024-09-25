## Configuration files.

### When the run_avro.sh script is executed, it will look for the following configuration files (1-5) in the conf directory.

1. avro_app.json
    This is our main configuration file for our Golan app.

2. avro_kafka.json
    This is our Kafka configuration

3. avro_mongo.json
    This is our MongoDb configuration

4. avro_mysql.json
    This is our MySql configuration

5. avro_postgres.json
    This is our Postgresql configuration

6. sit_seed.json
    This is our seed data file for the App.

### The following files will be used when the application is packages as a docker image.

7. pg_hba.conf
    This is our Postgresql server startup configuration file/defines who can connect from where.

8. postgresql.conf
    This is our Postgresql server startup configurations file.

9. mysql.cnf
    This is our MysqlDB server startup configuration file.

