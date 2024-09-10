ARG REPO_NAME="georgelza"

FROM ${REPO_NAME}/ubuntu20.04-openjdk11:1.0.0

USER root

COPY stage/hadoop-3.3.5-aarch64.tar.gz /tmp/

WORKDIR /tmp/

RUN echo "--> install Apache Hadoop 3.3.5" && \
    tar -xzvf /tmp/hadoop-3.3.5-aarch64.tar.gz && \
    mv /tmp/hadoop-3.3.5 /opt/ && \
    rm /tmp/hadoop-*

RUN ln -s /opt/hadoop-3.3.5/etc/hadoop /etc/hadoop

RUN echo "--> make Apache Hadoop directories" && \
    mkdir /opt/hadoop-3.3.5/logs && \
    mkdir /hadoop-data

RUN echo "--> set environment variable"
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV HADOOP_VERSION=3.3.5
ENV HADOOP_HOME=/opt/hadoop-3.3.5
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV MULTIHOMED_NETWORK=1
ENV PATH=$HADOOP_HOME/bin/:$PATH

USER root

ADD bin/entrypoint.sh /entrypoint.sh
RUN chmod a+x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]