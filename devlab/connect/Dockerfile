FROM confluentinc/cp-kafka-connect-base:7.6.1

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

RUN echo "--> Installing Connector Plugins"

# Source - https://docs.confluent.io/kafka-connectors/spooldir/current/overview.html
RUN confluent-hub install --no-prompt jcustenborder/kafka-connect-spooldir:2.0.43
# Sink/Source - https://github.com/mongodb/mongo-kafka/blob/master/README.md
RUN confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:1.12.0
# Sink/Source - https://docs.confluent.io/kafka-connectors/jdbc/current/index.html
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest
# Source - https://github.com/streamsend-io/file-chunk-connectors
RUN confluent-hub install --no-prompt markteehan/file-chunk-sink:latest
# Sink - https://www.confluent.io/hub/tabular/iceberg-kafka-connect
RUN confluent-hub install --no-prompt tabular/iceberg-kafka-connect:latest
# Source - https://github.com/confluentinc/kafka-connect-datagen/blob/master/README.md
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest
# Sink - https://docs.confluent.io/kafka-connectors/redis/current/index.html
RUN confluent-hub install --no-prompt jcustenborder/kafka-connect-redis:latest
# Sink - https://docs.confluent.io/kafka-connectors/s3-sink/current/overview.html
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:latest

# SINK - https://docs.confluent.io/kafka-connectors/hdfs/current/overview.html
# RUN confluent-hub install --no-prompt confluentinc/kafka-connect-hdfs:latest
# Source - https://debezium.io/documentation/reference/2.7/connectors/postgresql.html
# RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:latest
# Source - https://debezium.io/documentation/reference/2.7/connectors/mongodb.html
# RUN confluent-hub install --no-prompt debezium/debezium-connector-mongodb:latest
# Source - https://debezium.io/docs/connectors/mysql/
# RUN confluent-hub install --no-prompt debezium/debezium-connector-mysql:latest

USER root


# COPY stage/mysql-connector-j-9.0.0.jar /usr/share/java/kafka/mysql-connector-j-9.0.0.jar 
# COPY stage/postgresql-42.7.3.jar /usr/share/java/kafka/postgresql-42.7.3.jar 


RUN echo "--> Installing Database Connector Plugins" && \
    wget -O /usr/share/java/kafka/mysql-connector-j-9.0.0.jar \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.0.0/mysql-connector-j-9.0.0.jar


USER appuser

# docker build -t kafka-connect-custom:1.0 .