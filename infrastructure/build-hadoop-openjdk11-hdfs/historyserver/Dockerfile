ARG REPO_NAME="georgelza"

FROM ${REPO_NAME}/hadoop-base-3.3.5-java11:1.0.0

RUN echo "--> Build Hadoop HDFS History server"

HEALTHCHECK CMD curl -f http://localhost:8188/ || exit 1

ENV YARN_CONF_yarn_timeline___service_leveldb___timeline___store_path=/hadoop/yarn/timeline
RUN mkdir -p /hadoop/yarn/timeline
VOLUME /hadoop/yarn/timeline

ADD bin/run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 8188

CMD ["/run.sh"]
