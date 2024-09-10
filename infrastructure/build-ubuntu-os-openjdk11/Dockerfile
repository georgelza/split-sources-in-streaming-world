ARG REPO_NAME="georgelza"

FROM ${REPO_NAME}/ubuntu20.04:1.0.0

WORKDIR /root

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

# Install some useful tools
RUN echo "--> install  Open JDK 11 ARM64" && \
    build_deps="openjdk-11-jdk" && \
    apt-get update && \
    apt-get install -y $build_deps


RUN echo "--> Purge apt artifacts" && \
    apt-get purge -y --auto-remove $build_deps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
    
