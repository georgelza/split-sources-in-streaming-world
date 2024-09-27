## Running Stack

See the master README.md file for the overall project overview and how to build and run the project.

### Variables and Variables and Variables...

There is sadly no easy way to do this as this is not a small stack, of a single product.

- Start by going into ./conf directory and see the various files there.
- Followed by the .env file and then the docker-compose file.


### Flink configuration variables.

See the docker-compose.yaml file for the various variables passed into the 3 Flink containers.
Some of them originate out of the .env file, for the Hive environment some originate out of the hive.env file and some out of flink-conf-sc.yaml


### Hive site configuration file/AWS S3 credentials for Flink usage.

Take note that the flink images are build with hive-site.xml copied it, this file also contains the credentuals for the MinIO S3 environment.


### PostgreSQL configuration, 

The credentials come out of .env and the start parameters out of postgresql.conf
