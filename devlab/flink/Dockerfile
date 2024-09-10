ARG REPO_NAME="georgelza"

FROM ${REPO_NAME}/apacheflink-base:1.18.1-scala_2.12-java11
SHELL ["/bin/bash", "-c"]

# this is where we have flink itself installed.
WORKDIR /opt/flink
USER root:root


RUN echo "--> Install JARs: Flink's S3 plugin" && \
    mkdir ./plugins/s3-fs-hadoop && \
    mv ./opt/flink-s3-fs-hadoop-1.18.1.jar ./plugins/s3-fs-hadoop/


RUN echo "--> Enable SQL Client to find the job manager when running it from this image" && \
    sed -i "s/jobmanager.rpc.address: localhost/jobmanager.rpc.address: flink-jobmanager/g" ./conf/flink-conf.yaml


ENV FLINK_HOME=/opt/flink

# Install JARs
# See https://repo.maven.apache.org/maven2/org/apache/flink/


RUN echo "--> Install JARs: Flink's Kafka connector" && \
    mkdir -p ./lib/kafka && pushd $_ && \
    curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.18/flink-sql-connector-kafka-3.2.0-1.18.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.0.0_2.12/1.18.1/flink-sql-connector-hive-3.0.0_2.12-1.18.1.jar -O && \
    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-json/1.18.1/flink-sql-json-1.18.1.jar -O && \
    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro/1.18.1/flink-sql-avro-1.18.1.jar -O && \
    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-parquet/1.18.1/flink-sql-parquet-1.18.1.jar -O && \
    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.18.1/flink-sql-avro-confluent-registry-1.18.1.jar -O && \
    popd


RUN echo "--> Install JARs: Flink's Hive connector (Catalogs)" && \
    mkdir -p ./lib/hive && pushd $_ && \
    curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.3_2.12/1.18.1/flink-sql-connector-hive-3.1.3_2.12-1.18.1.jar -O && \
    curl https://repo.maven.apache.org/maven2/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3.jar -O && \
    curl https://repo.maven.apache.org/maven2/org/apache/hive/hive-metastore/3.1.3/hive-metastore-3.1.3.jar -O && \
    popd


RUN echo "--> Install JARs: Flinks Hive/Hadoop" && \
    mkdir -p ./lib/hadoop && pushd $_ && \
    curl https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.1.1/commons-configuration2-2.1.1.jar -O && \
    curl https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar -O && \
    curl https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar -O && \
    curl https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/5.3.0/woodstox-core-5.3.0.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.4/hadoop-auth-3.3.4.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.4/hadoop-hdfs-client-3.3.4.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.4/hadoop-mapreduce-client-core-3.3.4.jar -O  && \
    popd


RUN echo "--> Install JARs: Flink's Paimon connector (Catalogs)" && \
    mkdir -p ./lib/paimon && pushd $_ && \
    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.18/flink-connector-jdbc-3.2.0-1.18.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/0.8.2/paimon-flink-1.18-0.8.2.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar -O && \
    curl https://repo1.maven.org/maven2/org/apache/paimon/paimon-s3/0.8.2/paimon-s3-0.8.2.jar -O && \
    curl https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.27/mysql-connector-java-8.0.27.jar  -O && \
    curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/3.1.0/flink-sql-connector-mysql-cdc-3.1.0.jar  -O && \
    curl https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-postgres-cdc/3.1.0/flink-sql-connector-postgres-cdc-3.1.0.jar -O && \
    popd


#    curl https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-action/0.8.2/paimon-flink-action-0.8.2.jar -O && \
#    curl https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-common/0.8.2/paimon-flink-common-0.8.2.jar -O && \
#    curl https://repo1.maven.org/maven2/org/apache/paimon/paimon-bundle/0.8.2/paimon-bundle-0.8.2.jar && \
#    popd


# RUN echo "--> Install JARs: AWS / Hadoop S3" && \
#     mkdir -p ./lib/aws && pushd $_ && \
#     curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O && \
#     curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.648/aws-java-sdk-bundle-1.12.648.jar -O  && \
#     popd


# RUN echo "--> Install JARs: Hadoop AWS classes" && \
#     mkdir -p ./lib/hadoop && pushd $_ && \
#     curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-9.0/flink-shaded-hadoop-2-uber-2.8.3-9.0.jar && \
#     popd
    

# RUN echo "--> Install JARs: AWS / AWS Bundled classes" && \
#     mkdir -p ./lib/aws-bundle && pushd $_ && \
#     curl https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.26.9/bundle-2.26.9.jar && \
#     popd


# RUN echo "--> Install JARs: Hadoop" && \
#     mkdir -p ./lib/hadoop && pushd $_ && \
#     curl https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.1.1/commons-configuration2-2.1.1.jar -O && \
#     curl https://repo1.maven.org/maven2/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar -O && \
#     curl https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar -O && \
#     curl https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/5.3.0/woodstox-core-5.3.0.jar -O && \
#     curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.4/hadoop-auth-3.3.4.jar -O && \
#     curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar -O && \
#     curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.4/hadoop-hdfs-client-3.3.4.jar -O && \
#     curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.4/hadoop-mapreduce-client-core-3.3.4.jar -O  && \
#     popd


#   https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/overview/
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

#   Flink Pipeline Connectors
#RUN echo "-> Install JARs: Flink CDC's via CURL" && \
#    mkdir -p ./lib/cdcpipeline && pushd $_ && \
#    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-paimon/3.2.0/flink-cdc-pipeline-connector-paimon-3.2.0.jar -O && \
#    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-mysql/3.2.0/flink-cdc-pipeline-connector-mysql-3.2.0.jar -O && \
#    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-kafka/3.2.0/flink-cdc-pipeline-connector-kafka-3.2.0.jar -O && \
#    curl https://repo.maven.apache.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-values/3.2.0/flink-cdc-pipeline-connector-values-3.2.0.jar -O && \
#    popd


RUN chown -R flink:flink /opt/flink

USER flink:flink

CMD ./bin/start-cluster.sh && sleep infinity
