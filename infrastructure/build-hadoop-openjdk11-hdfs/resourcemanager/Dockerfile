ARG REPO_NAME="georgelza"

FROM ${REPO_NAME}/hadoop-base-3.3.5-java11:1.0.0

RUN echo "--> Build Hadoop HDFS Resource Manager"

HEALTHCHECK CMD curl -f http://localhost:8088/ || exit 1

ADD bin/run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 8088

CMD ["/run.sh"]
