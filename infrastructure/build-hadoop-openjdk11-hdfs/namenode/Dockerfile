ARG REPO_NAME="georgelza"

FROM ${REPO_NAME}/hadoop-base-3.3.5-java11:1.0.0

RUN echo "--> Build Hadoop HDFS Namenode"

HEALTHCHECK CMD curl -f http://localhost:9870/ || exit 1

ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/warehouse
RUN mkdir -p /hadoop/dfs/warehouse
VOLUME /hadoop/dfs/warehouse

ADD bin/run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 9870

CMD ["/run.sh"]
