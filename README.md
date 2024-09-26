# Split Data source in a day to day stream processing & streaming based Analytic solution.

(Version 2)

This repo and the to be Blog posts follows on from a previous series of article's posted, [An exercise in Discovery, Streaming data in the analytical world](https://medium.com/@georgelza/an-exercise-in-discovery-streaming-data-in-the-analytical-world-part-1-e7c17d61b9d2) -> with the associated git Repo: [MongoCreator-GoProducer-avro](https://github.com/georgelza/MongoCreator-GoProducer-avro), we will simply think of the previous article series as Version 1, it will eventually become clear why I have versioned the articles... ;)


So in that serious I joked, this is probably not the last of the series, this is not the end of it...

The original article had a Golang application posting 2 json documents from a fake store onto 22 Kafka topics. We did some Kafka stream processing on that. We then also dropped those 2 topics into Flink where replicated the same processing to demostrate the power of Flink, and differences when compared with Kafka. From here we pushed the data into 2 datastore solutions. First was a Apache Iceberg soluton with storage on S3. The second was into Apache Paimon with storage on Apache HDFS cluster.


On 27 Aug 2024 - I had a chat wit a friend and got a friendly request. Let's take the original article and extend it only such a small/ little bit, Lets show the capabilities of Flink CDC to injest data. Basically, we're going to split the 2 source streams.

 - The salesbasket will still go onto Kafka topic, 
 - And then secondly, the associated Salespayments document/record will be pushed into a database (MySQL). From here will then configure Flink CDC (and build in Debezium capability) to pull/source the data from the database and push it into a Flink Table (as per original article, salespayments).
 - As per the original article we will now join the 2 streams together, created salescompleted record set.
 - We will then use this salescompleted record set to created a unnested_sales table, after which we will push everything into a Paimon tables.

For reference see the blog-doc/diagram's folder for some diagrams depicting the flow of data.


1. So first, I modified the app (Golang) to push the salespayments to either Kafka salespayments topic (as current) or into a salespayments table in the sales database (Mysqldb 8.x), controlled by Mysql_enabled setting in the *_app.json file.

2. Next up, need to extend my Apache Flink stack (Flink Jobserver and Taskmanager and the Flink Sql client) and add the right Flink CDC/Jar files, allowing us to configure FlinkCDC from the source salespayments table into a Flink Table (t_f_msqlcdc_salespayments or t_f_pgcdc_salespayments). 

3. We will also configure Flink to push this 2nd source (salespayments) now onwards onto a Kafak topic avro_salespayments vs where we origially source data from, thus still aligning with the original Kafka topic/s.

4. From Flink the aggregated data as per the original article will be pushed back onto Kafka topic's also.

5. For the Analytical part we will again push the data down onto an Apache Paimon table format using the Apache Avro file format now located on AWS S3 (simulated via a MinIO container).

6. A late add, I removed the pre packaged Hime Metastore and replaced it with a split tier Hiverserver2 and Metastore, based on Hadoop 3.3.5 and Hive 3.1.3 build on top of Ubuntu 20.04 and OpenJDK8.

Ye... I think that will be a good start, and that will accomplish what we want to demostrate, a split source environment and using Apache Flink's CDC apabilities to ingest the data from the database and push it into a Flink Table's for further processing, joining with data from i.e: Kafka sources, a more pragmatic/realistic example.

From here change into the devlab directory and see the README.md file for more details.

(If anything is not clear, or something is broken, I keep on tinkering, then please email me).

George

georgelza@gmail.com

[Split Data source in a streaming solution](https://github.com/georgelza/split-sources-in-streaming-world.git)


## Bulding and Running this project

The following steps are the process required to get the project up and running. - >NOTE this was build on a Mac M1 Pro. If you running on Intel please go through the Dockerfiles and changed the dockerfile to use the intel/amd docker images, similarly check the infrastructure/Makefile in the getall and pullall steps.

1. search the project subdirectory structure and replace repo_owner=georgelza with your github username.
2. change into infrastructure.
3. make getall
4. make pullall
5. make buildall
6. change into devlab
7. make build
8. make run  -> you might need to reco this one a couple of times until the stack is stable.
9. make deploy -> if it fails here.. it's most probably going to be due to the metaserver/metastore not running, it's a quiark. just re-excute step 8 and then redo step 9
10. make run_app

## Re Debezium

Debezium is a distributed platform that turns your existing databases into event streams, so applications can see and respond almost instantly to each committed row-level change in the databases. Debezium is built on top of Kafka and provides Kafka Connect compatible connectors that monitor specific database management systems. The event streams produced exposes changes in your databases as Kafka topics. 


## Credits... due.

Without these guys and their willingness to entertain allot of questions and some times siply dumb ideas and helping me slowly onto the right path all of this would simply not have been possible.


    Ben Gamble,
        Apache Kafka, Apache Flink, streaming and stuff (as he calls it)
        A good friend, thats always great to chat to... and we seldom stick to original topic.
        https://confluentcommunity.slack.com/team/U03R0RG6CHZ


    Ian Engelbrecht,
        VeeAM
        (Manager, Technical Sales at Veeam Software English Africa)
        https://www.linkedin.com/in/ian-veeam/


## NOTES:


### Misc Reading Resources:

- Good article that covers how Paimon Table format works... [Apache Paimon: Introducing Deletion Vectors](https://medium.com/@ipolyzos_/apache-paimon-introducing-deletion-vectors-584666ee90de) 

- [Debezium Architecture](https://debezium.io/documentation/reference/stable/architecture.html)
- [Overview of Debezium](https://docs.redhat.com/en/documentation/red_hat_integration/2022.q3/html/getting_started_with_debezium/introduction-debezium)
- Interesting read re Debezium, by the main custodium for last many... ;) yrs. [Change Data Capture Breaks Encapsulation”. Does it, though](https://www.decodable.co/blog/change-data-capture-breaks-encapsulation-does-it-though)



### For My Own notes: (When git does not want to work/sync failing)
    
```git push -u origin main```

#### Results in

Enumerating objects: 142, done.
Counting objects: 100% (142/142), done.
Delta compression using up to 10 threads
Compressing objects: 100% (129/129), done.
error: RPC failed; HTTP 400 curl 22 The requested URL returned error: 400
send-pack: unexpected disconnect while reading sideband packet
Writing objects: 100% (142/142), 3.67 MiB | 4.95 MiB/s, done.
Total 142 (delta 27), reused 0 (delta 0), pack-reused 0
fatal: the remote end hung up unexpectedly
Everything up-to-date
    
#### Execute the following

```git config http.postBuffer 524288000```