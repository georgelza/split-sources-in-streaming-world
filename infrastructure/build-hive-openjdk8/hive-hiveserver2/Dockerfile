ARG REPO_NAME="georgelza"

FROM ${REPO_NAME}/hive-base-3.1.3-java8:1.0.0
 
RUN echo "--> Build Apache Hive Hiveserver"

RUN echo "--> Expose Ports" 
EXPOSE 10000
EXPOSE 10001
EXPOSE 10002

RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh", "hiveserver"]

HEALTHCHECK CMD [ "sh", "-c", "netstat -ln | grep 10002" ]