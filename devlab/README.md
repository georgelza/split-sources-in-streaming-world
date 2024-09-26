## Running Stack

### Flink configuration variables.

See the docker-compose.yaml file for the various variables passed into the 3 Flink containers.

### Hive site configuration file/AWS S3 credentials

Take note that the flink images are build with hive-site.xml copied it, this file also contains the credentuals for the MinIO S3 environment.