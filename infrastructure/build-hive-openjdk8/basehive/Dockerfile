ARG REPO_NAME="georgelza"

FROM ${REPO_NAME}/hadoop-base-3.3.5-java8:1.0.0

USER root
WORKDIR /tmp/

RUN echo "--> Copy Source Binaries"
COPY stage/apache-hive-3.1.3-bin.tar.gz /tmp
COPY stage/postgresql-42.7.3.jar /tmp
COPY stage/json-serde-1.3.8-jar-with-dependencies.jar /tmp

RUN echo "--> Make Temp dir" && \
    mkdir -p /tmp/var/bin && \
    mkdir -p /tmp/var/conf && \
    mkdir -p /tmp/root

COPY bin/* /tmp/var/bin
# To be removed and files rather imported via volume mounts.
COPY conf/* /tmp/var/conf

RUN echo "--> set environment variables"
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64

ENV HADOOP_HEAPSIZE=1024

ENV HIVE_VERSION=3.1.3
ENV HIVE_HOME=/opt/hive
ENV HIVE_CONF_DIR=$HIVE_HOME/conf
ENV HIVE_AUX_JARS_PATH=$HIVE_HOME/extlib

ENV POSTGRES_JDBC_VERSION=42.7.3
ENV PATH=$HIVE_HOME/bin:$HADOOP_HOME/bin/:$PATH
ENV USER=root

#Add ca_certificates to the image ( if trust is not allready added through base image )
COPY ca_certificates/* /usr/local/share/ca-certificates/


RUN echo "--> Install Update CA_Certs" && \
    update-ca-certificates 2>/dev/null || true && \
    echo "NOTE: CA warnings suppressed." 
        

RUN echo "--> install apache hive 3.1.3" && \
    tar -xzvf /tmp/apache-hive-3.1.3-bin.tar.gz && \
    mv /tmp/apache-hive-3.1.3-bin $HIVE_HOME && \
    rm /tmp/apache-hive-*

    
RUN echo "--> Install the Postgresql JDBC libraries" && \
   mkdir -p $HIVE_HOME/lib && \
   mv /tmp/postgresql-$POSTGRES_JDBC_VERSION.jar $HIVE_HOME/lib/postgresql-jdbc.jar


RUN echo "--> Download json serializer/deserializer" && \
    #Download json serializer/deserializer
    #https://stackoverflow.com/questions/26644351/cannot-validate-serde-org-openx-data-jsonserde-jsonserde    
    mv /tmp/json-serde-1.3.8-jar-with-dependencies.jar $HIVE_HOME/lib/json-serde-1.3.8-jar-with-dependencies.jar && \
    #curl -s -L http://www.congiu.net/hive-json-serde/1.3.8/cdh5/json-serde-1.3.8-jar-with-dependencies.jar -o $HIVE_HOME/lib/json-serde-1.3.8-jar-with-dependencies.jar && \
    mkdir -p $HIVE_HOME/extlib && \
    #Install Atlas hooks
    #&& tar xf /tmp/apache-atlas-2.0.0-hive-hook.tar.gz --strip-components 1 -C $HIVE_HOME/extlib \
    # TODO: remove me...
    for file in $(find $HIVE_HOME/extlib/ -name '*.jar' -print); do ln -s $file $HIVE_HOME/lib/; done;
	

RUN echo "--> Install/Link AWS s3 drivers" && \
	ln -s $HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar $HIVE_HOME/lib/. && \
	ln -s $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-$HADOOP_VERSION.jar $HIVE_HOME/lib/. && \
	ln -s $HADOOP_HOME/share/hadoop/tools/lib/aws-java-sdk-bundle-*.jar $HADOOP_HOME/share/hadoop/common/lib/. && \
	ln -s $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-$HADOOP_VERSION.jar $HADOOP_HOME/share/hadoop/common/lib/.


RUN echo "--> Remove libs causing error from hive - duplicated from hadoop" && \
	rm /opt/hive/lib/log4j-slf4j-impl-*.jar

# Replace with rather using remote volume mounts.
RUN echo "--> Move hadoop configuration files and scripts" && \
    mv /tmp/var/conf/core-site.xml $HADOOP_CONF_DIR && \
    mv /tmp/var/conf/hadoop-log4j2.properties $HADOOP_CONF_DIR/log4j.properties

RUN echo "--> Move hive configuration files and scripts" && \
    mv /tmp/var/conf/* $HIVE_CONF_DIR

RUN echo "--> Move hive scripts" && \
    chmod a+x /tmp/var/bin/* && \
	mv /tmp/var/bin/hivemetastore /usr/local/bin/ && \
	mv /tmp/var/bin/hiveserver /usr/local/bin/ && \
	rm -rf /tmp/var/*


RUN mkdir -p /opt/hive/logs

WORKDIR /opt/hive

ADD bin/entrypoint.sh /entrypoint.sh
RUN chmod a+x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]