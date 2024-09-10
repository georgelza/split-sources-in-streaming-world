ARG REPO_NAME="georgelza"

FROM ${REPO_NAME}/hadoop-base-3.3.5-java11:1.0.0
 
RUN echo "--> Build Hadoop HDFS Datanode"

HEALTHCHECK CMD curl -f http://localhost:9864/ || exit 1

ENV HDFS_CONF_dfs_datanode_data_dir=file:///hadoop/dfs/warehouse
RUN mkdir -p /hadoop/dfs/warehouse
VOLUME /hadoop/dfs/warehouse

ADD bin/run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 9864

CMD ["/run.sh"]
