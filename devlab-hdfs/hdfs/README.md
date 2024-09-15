# Some Apache Hadoop (HDFS) Notes

docker compose config

ALSO SEE: https://nathan-torento.medium.com/distributed-systems-fault-tolerance-tutorial-78b825f8cada


## Go into the namenode container to run bash commands from it
```
docker compose exec -it namenode bash
```

## In the namenode container, create a directory called "input"
```
mkdir input
```

## Create a text files to store 
```
echo "Hello World" >input/f1.txt
```

Then we'll create an input directory on HDFS
If you receive an error about connecting to the namenode,
simply wait a few minutes before trying again.
Docker is still probably taking a while to build
and connect all the containers.
```
hadoop fs -mkdir -p input
```

## Here, we'll put the text file into the datanodes on HDFS
```
hdfs dfs -put ./input/f1.txt input
```

## Let's ensure that the file was stored by our HDFS into 3 replicas each;
```
hdfs fsck input/f1.txt -files -blocks -locations
```

https://devops.datenkollektiv.de/apache-hadoop-setting-up-a-local-test-environment.html

```
hdfs dfsadmin -report

hdfs dfs -mkdir /tmp

hdfs dfs -ls /tmp

hdfs dfs -ls

docker run -it --rm --name hdfs-shell --network hdfs -e "CORE_CONF_fs_defaultFS=hdfs://namenode:9000" -e "CLUSTER_NAME=hdfspaimon" -t hadoop-base:3.3.5-java11 /bin/bash


<property>
  <name>fs.default.name</name>
  <value>hdfs://hadoop:8020</value>
</property>

sudo netstat -tulpn | grep :8020
```