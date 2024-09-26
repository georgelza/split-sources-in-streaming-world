ARG REPO_NAME="georgelza"

FROM ${REPO_NAME}/hive-base-3.1.3-java8:1.0.0
 
RUN echo "--> Build Apache Hive Metastore"

RUN echo "--> Expose Ports" 
EXPOSE 9083

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh", "hivemetastore"]

HEALTHCHECK CMD [ "sh", "-c", "netstat -ln | grep 9083" ]
