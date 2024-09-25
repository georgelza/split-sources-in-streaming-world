FROM arm64v8/ubuntu:20.04

# original https://github.com/YanYunNN/hadoop-cluster-docker-m1/blob/main/Dockerfile

WORKDIR /root
	
RUN echo "--> Install OS dependencies openssh-server & misc tools" && \
    build_deps="openssh-server wget neovim curl unzip net-tools" && \
    apt-get update && \
    apt-get install -y $build_deps
    

RUN echo "--> Purge apt artifacts" && \
    apt-get purge -y --auto-remove $build_deps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*