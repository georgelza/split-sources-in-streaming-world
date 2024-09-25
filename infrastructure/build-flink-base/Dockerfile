
FROM arm64v8/flink:1.18.1-scala_2.12-java11
SHELL ["/bin/bash", "-c"]


RUN echo "--> Install some useful tools" && \
    build_deps="neovim tree lnav unzip" && \
    apt-get update && \
    apt-get install -y $build_deps


RUN echo "--> Install Duckdb client" && \
    wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-aarch64.zip \
    && unzip duckdb_cli-linux-aarch64.zip -d /usr/local/bin \
    && rm duckdb_cli-linux-aarch64.zip


RUN echo "--> Purge apt artifacts" && \
    apt-get purge -y --auto-remove $build_deps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*