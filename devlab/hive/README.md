## Hive Metaserver


I've been using hive as a metastore in the standadalone options available via hub.docker.com.
For those I extended them to use PostgreSQL as datastore.
Issue has been that they are build on Apache Hive 3.0.0 which in fact is not supported by Apache Flink so ended building my own.

This entailed installing Apache Hadoop 3.3.5 on top of my base Ubuntu 20.04 image followed by installing Apache Hive 3.1.3. as a base hive image. 

This base image is then deployed as the 2 required components, 

- Hiverserver2  
- Hive Metastore.

The stack (and online examples) brought with it the hive.env file which can be used to set the configuration variables and then push them into the containers as local environment variables. 

The entrypoint.sh script then reads these environemnt variables and write them out to the required target configuration files. -> Funky..., Possible considering expanding this to also set the log4j.properties file/variables.


This works... but I don't like it 1000%, it means I need to maintian some (PostgreSql and S3) user credentials in my .env files and also in this file... (duplication) and well had some "issues" i.e. the metastore.uris for hive-server will be overriden/ignored when set via the above file, so we rather set it in the docker-compose.yml via the SERVICE_OPTS variable option.

Also discovered even though the one container will accept the datastore username/password/uri etc, the other container still seem to need the values set in the hive.env file, had errors if I tried to pass them in via SERVICE_OPTS.

I'd be happier if values thats not "compromising" or dynamic be in the hive.env file and everything that is used by multiple containers be defined/provide via the docker-compose.yml file, sourced from our .env file.

I'd prefer to set all my variables in the .env file and then push them into the containers... to be explored further.


