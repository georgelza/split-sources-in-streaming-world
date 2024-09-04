
###############################################################################
#SQL CLI - inspired by https://github.com/wuchong/flink-sql-demo/tree/v1.11-EN/sql-client
###############################################################################

FROM arm64v8/flink:1.18.1-scala_2.12-java11
SHELL ["/bin/bash", "-c"]

# this is where we have flink itself installed.
WORKDIR /opt/flink


RUN echo "--> Install some useful tools" && \
    build_deps="neovim tree lnav unzip" && \
    apt-get update && \
    apt-get install -y $build_deps


RUN echo "--> used to interface with minio store." && \
    wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-aarch64.zip && \
    unzip duckdb_cli-linux-aarch64.zip -d /usr/local/bin && \
    rm duckdb_cli-linux-aarch64.zip


RUN echo "--> Creating SQL Client directories" && \
    mkdir -p /opt/sql-client/conf


RUN echo "--> Copy SQL Client shell scripts"
COPY bin/sql-client.sh /opt/sql-client/

# Install JARs
# See https://repo.maven.apache.org/maven2/org/apache/flink/


RUN echo "--> Install JARs: Flink's Kafka connector via CURL" && \
    mkdir -p ./lib/kafka && pushd $_ && \
    curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.18/flink-sql-connector-kafka-3.2.0-1.18.jar -O && \
    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.18/flink-connector-jdbc-3.2.0-1.18.jar -O && \
#   curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.0.0_2.12/1.18.1/flink-sql-connector-hive-3.0.0_2.12-1.18.1.jar -O && \
    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-json/1.18.1/flink-sql-json-1.18.1.jar -O && \
    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro/1.18.1/flink-sql-avro-1.18.1.jar -O && \
    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-parquet/1.18.1/flink-sql-parquet-1.18.1.jar -O && \
    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.18.1/flink-sql-avro-confluent-registry-1.18.1.jar -O && \
    popd


RUN echo "--> Install JARs: Flink's Hive connector (Catalogs) via CURL" && \
    mkdir -p ./lib/hive && pushd $_ && \
    curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.18.1/flink-sql-connector-hive-3.1.3_2.12-1.18.1.jar -O && \
    curl https://repo.maven.apache.org/maven2/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar -O && \
    curl https://repo.maven.apache.org/maven2/org/apache/hive/hive-metastore/3.1.3/hive-metastore-3.1.3.jar -O && \
    popd


RUN echo "--> Install JARs: Runtime Dependencies for Iceberg via CURL" && \
    mkdir -p ./lib/iceberg && pushd $_ && \
    curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.18/1.5.0/iceberg-flink-runtime-1.18-1.5.0.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.0/iceberg-aws-bundle-1.5.0.jar-O && \
    popd


RUN echo "--> Install JARs: AWS / Hadoop S3 via CURL" && \
    mkdir -p ./lib/aws && pushd $_ && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O && \
    curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.648/aws-java-sdk-bundle-1.12.648.jar -O  && \
    popd


RUN echo "--> Install JARs: Hadoop AWS classes via CURL" && \
    mkdir -p ./lib/hadoop && pushd $_ && \
    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-9.0/flink-shaded-hadoop-2-uber-2.8.3-9.0.jar && \
    popd
    

RUN echo "--> Install JARs: AWS / AWS Bundled classes via CURL" && \
    mkdir -p ./lib/aws-bundle && pushd $_ && \
    curl https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.26.9/bundle-2.26.9.jar && \
    popd


RUN echo "--> Install JARs: Hadoop via CURL" && \
    mkdir -p ./lib/hadoop && pushd $_ && \
    curl https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.1.1/commons-configuration2-2.1.1.jar -O && \
    curl https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.4/hadoop-auth-3.3.4.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar -O && \
    curl https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar -O && \
    curl https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/5.3.0/woodstox-core-5.3.0.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.4/hadoop-hdfs-client-3.3.4.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.4/hadoop-mapreduce-client-core-3.3.4.jar -O  && \
    popd

    
# RUN echo "-> Install JARs: DBs via CURL" && \
#    mkdir -p ./lib/dbs && pushd $_ && \
#    curl https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -O && \
#    curl https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.0.0/mysql-connector-j-9.0.0.jar -O && \
#    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.0.1-1.17/flink-sql-connector-elasticsearch7-3.0.1-1.18.jar -O && \
#    curl https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar -O && \
#    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-protobuf/1.18.1/flink-sql-protobuf-1.18.1.jar -O && \
#    popd

# https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/overview/
#RUN echo "-> Install JARs: Flink CDC's via CURL" && \
#    mkdir -p ./lib/cdc && pushd $_ && \
#    curl https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar  -O && \
#    curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/3.1.0/flink-sql-connector-mysql-cdc-3.1.0.jar  -O && \
#    curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-postgres-cdc/3.1.0/flink-sql-connector-postgres-cdc-3.1.0.jar -O && \
#    curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7/3.0.1-1.17/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar  -O && \
#    curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mongodb-cdc/3.1.0/flink-sql-connector-mongodb-cdc-3.1.0.jar  -O && \
#    curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-oracle-cdc/3.1.1/flink-sql-connector-oracle-cdc-3.1.1.jar  -O && \
#    curl https://repo1.maven.org/maven2/com/oracle/ojdbc/ojdbc8/19.3.0.0/ojdbc8-19.3.0.0.jar -O && \
#    curl https://repo1.maven.org/maven2/com/oracle/database/xml/xdb/19.3.0.0/xdb-19.3.0.0.jar -O && \
#    popd


RUN echo "-> Install JARs: Flink CDC's via COPY"
WORKDIR /opt/flink/lib/cdc
COPY stage/* .

USER root:root

RUN chown -R flink:flink /opt/flink
RUN chown -R flink:flink /opt/sql-client

RUN echo "--> Purge apt artifacts" && \
    apt-get purge -y --auto-remove $build_deps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
    
WORKDIR /opt/sql-client
ENV SQL_CLIENT_HOME=/opt/sql-client

ADD bin/docker-entrypoint.sh /
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]

